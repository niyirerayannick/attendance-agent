import json
import tempfile
import unittest

from requests.auth import HTTPDigestAuth

try:
    from attendance_agent.cloud import CloudAuthenticationError, EpcClient
    from attendance_agent.config import AgentConfig
    from attendance_agent.hikvision import DeviceError, HikvisionClient
except ModuleNotFoundError:
    from cloud import CloudAuthenticationError, EpcClient
    from config import AgentConfig
    from hikvision import DeviceError, HikvisionClient


def config():
    return AgentConfig("192.168.88.187", "reader", "password-not-logged", "https://epca.example/api/internal/attendance/events/",
                       "EPCA-HQ-01", "cloud-token-not-logged", 60, tempfile.mktemp())


class Response:
    def __init__(self, status=200, payload=None, content=b"", headers=None):
        self.status_code, self.payload = status, payload if payload is not None else {}
        self.content, self.headers = content, headers or {}
    def raise_for_status(self):
        if self.status_code >= 400:
            from requests import HTTPError
            raise HTTPError("failed")
    def json(self):
        if isinstance(self.payload, Exception): raise self.payload
        return self.payload
    def close(self): self.closed = True


class Session:
    def __init__(self, response): self.response, self.auth, self.calls = response, None, []
    def request(self, *args, **kwargs): self.calls.append((args, kwargs)); return self.response
    def post(self, *args, **kwargs): self.calls.append((args, kwargs)); return self.response


# Realistic DS-K1T8003MF response; serial/MAC are placeholders, not real device identifiers.
DS_K1T8003MF_DEVICE_INFO_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<DeviceInfo version="2.0" xmlns="http://www.isapi.org/ver20/XMLSchema">
    <deviceName>T&amp;A Access Controller</deviceName>
    <deviceID>255</deviceID>
    <model>DS-K1T8003MF</model>
    <serialNumber>DS-K1T8003MF20210802V013737ENX00000000</serialNumber>
    <macAddress>00:00:5e:00:53:01</macAddress>
    <firmwareVersion>V1.3.37</firmwareVersion>
    <firmwareReleasedDate>build 210802</firmwareReleasedDate>
    <deviceType>ACS</deviceType>
</DeviceInfo>
"""


class DeviceInfoTests(unittest.TestCase):
    def device_info(self, response):
        return HikvisionClient(config(), Session(response)).device_info()

    def assert_no_secrets(self, exc):
        text = f"{exc!s} {exc!r} {exc.__cause__!s}"
        for secret in ("password-not-logged", "cloud-token-not-logged", "Authorization", "Digest"):
            self.assertNotIn(secret, text)

    def test_xml_device_info_is_parsed(self):
        info = self.device_info(Response(payload=ValueError("not json"), content=DS_K1T8003MF_DEVICE_INFO_XML,
                                         headers={"Content-Type": "application/xml"}))
        self.assertEqual(info["model"], "DS-K1T8003MF")
        self.assertEqual(info["deviceID"], "255")
        self.assertEqual(info["serialNumber"], "DS-K1T8003MF20210802V013737ENX00000000")
        self.assertEqual(info["macAddress"], "00:00:5e:00:53:01")
        self.assertEqual(info["firmwareVersion"], "V1.3.37")
        self.assertEqual(info["firmwareReleasedDate"], "build 210802")
        self.assertEqual(info["deviceType"], "ACS")

    def test_xml_namespace_is_stripped_from_keys(self):
        info = self.device_info(Response(payload=ValueError("not json"), content=DS_K1T8003MF_DEVICE_INFO_XML))
        self.assertIn("model", info)
        self.assertFalse(any(key.startswith("{") for key in info))

    def test_xml_entities_are_unescaped(self):
        info = self.device_info(Response(content=DS_K1T8003MF_DEVICE_INFO_XML))
        self.assertEqual(info["deviceName"], "T&A Access Controller")

    def test_xml_detected_without_content_type(self):
        info = self.device_info(Response(content=b"\n  " + DS_K1T8003MF_DEVICE_INFO_XML))
        self.assertEqual(info["model"], "DS-K1T8003MF")

    def test_json_device_info_still_supported(self):
        payload = {"DeviceInfo": {"deviceName": "Terminal", "model": "DS-K1T8003MF"}}
        info = self.device_info(Response(payload=payload, content=b'{"DeviceInfo": {}}',
                                         headers={"Content-Type": "application/json"}))
        self.assertEqual(info, payload)

    def test_malformed_xml_is_safe_error(self):
        with self.assertRaises(DeviceError) as ctx:
            self.device_info(Response(content=b"<DeviceInfo><model>DS-K1T8003MF</DeviceInfo>",
                                      headers={"Content-Type": "application/xml"}))
        self.assertIn("invalid deviceInfo XML", str(ctx.exception))
        self.assert_no_secrets(ctx.exception)

    def test_unexpected_xml_root_is_safe_error(self):
        with self.assertRaises(DeviceError) as ctx:
            self.device_info(Response(content=b"<ResponseStatus><statusCode>4</statusCode></ResponseStatus>"))
        self.assert_no_secrets(ctx.exception)

    def test_xml_with_dtd_is_rejected(self):
        body = b'<?xml version="1.0"?><!DOCTYPE d [<!ENTITY x "y">]><DeviceInfo><model>&x;</model></DeviceInfo>'
        with self.assertRaises(DeviceError):
            self.device_info(Response(content=body))

    def test_malformed_json_is_safe_error(self):
        with self.assertRaises(DeviceError) as ctx:
            self.device_info(Response(payload=ValueError("Expecting value"), content=b'{"DeviceInfo": ',
                                      headers={"Content-Type": "application/json"}))
        self.assertIn("invalid deviceInfo JSON", str(ctx.exception))
        self.assert_no_secrets(ctx.exception)


class SequenceSession(Session):
    """Returns queued responses in order, repeating the last one once exhausted."""
    def __init__(self, responses): super().__init__(None); self.responses = list(responses)
    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]


def user_page(employee_numbers, total, status=None, include_counts=True):
    result = {"searchID": "1", "UserInfo": [{"employeeNo": str(number), "name": f"User {number}", "userType": "normal"}
                                            for number in employee_numbers]}
    result["responseStatusStrg"] = status or ("MORE" if employee_numbers and employee_numbers[-1] < total else "OK")
    if include_counts:
        result.update({"numOfMatches": len(employee_numbers), "totalMatches": total})
    return Response(payload={"UserInfoSearch": result})


def search_conditions(session):
    return [json.loads(call[1]["data"])["UserInfoSearchCond"] for call in session.calls]


class UserDiscoveryTests(unittest.TestCase):
    def test_one_page_discovery_parses_confirmed_response(self):
        session = SequenceSession([Response(payload={"UserInfoSearch": {
            "searchID": "1", "responseStatusStrg": "OK", "numOfMatches": 1, "totalMatches": 1,
            "UserInfo": [{"employeeNo": "93", "name": "Yannick Niyirera", "userType": "normal",
                          "Valid": {"enable": True, "beginTime": "2021-01-01T00:00:00",
                                    "endTime": "2037-12-31T23:59:59"}}]}})])
        client = HikvisionClient(config(), session)
        result = client.discover_users()
        self.assertIsInstance(session.auth, HTTPDigestAuth)
        self.assertEqual(len(session.calls), 1)
        args, kwargs = session.calls[0]
        self.assertEqual(args, ("POST", "http://192.168.88.187/ISAPI/AccessControl/UserInfo/Search?format=json"))
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["totalMatches"], 1)
        self.assertTrue(result["complete"])
        user = result["users"][0]
        self.assertEqual((user["employeeNo"], user["name"], user["userType"]), ("93", "Yannick Niyirera", "normal"))
        self.assertTrue(user["enabled"])
        self.assertEqual(user["validEndTime"], "2037-12-31T23:59:59")

    def test_multi_page_discovery_uses_positions_of_ten_and_one_search_id(self):
        session = SequenceSession([user_page(range(1, 11), 23), user_page(range(11, 21), 23),
                                   user_page(range(21, 24), 23)])
        result = HikvisionClient(config(), session).discover_users()
        conditions = search_conditions(session)
        self.assertEqual([c["searchResultPosition"] for c in conditions], [0, 10, 20])
        self.assertEqual(len({c["searchID"] for c in conditions}), 1)
        self.assertEqual([user["employeeNo"] for user in result["users"]], [str(n) for n in range(1, 24)])
        self.assertEqual(result["count"], 23)
        self.assertTrue(result["complete"])

    def test_first_request_body_matches_proven_curl_request_exactly(self):
        session = SequenceSession([user_page([93], 1)])
        HikvisionClient(config(), session).discover_users()
        self.assertEqual(session.calls[0][1]["data"],
                         b'{"UserInfoSearchCond": {"searchID": "1", "searchResultPosition": 0, "maxResults": 10}}')
        self.assertNotIn("json", session.calls[0][1])
        condition = search_conditions(session)[0]
        self.assertIsInstance(condition["searchID"], str)
        self.assertIs(type(condition["searchResultPosition"]), int)
        self.assertIs(type(condition["maxResults"]), int)

    def test_each_page_request_is_logged_without_credentials(self):
        session = SequenceSession([user_page(range(1, 11), 13), user_page(range(11, 14), 13)])
        with self.assertLogs("epca_attendance_agent.hikvision", level="INFO") as logs:
            HikvisionClient(config(), session).discover_users()
        output = "\n".join(logs.output)
        self.assertIn("Hikvision user discovery: searchID=1 searchResultPosition=0 maxResults=10", output)
        self.assertIn("Hikvision user discovery: searchID=1 searchResultPosition=10 maxResults=10", output)
        self.assertIn('"searchResultPosition": 10', output)
        for secret in ("password-not-logged", "cloud-token-not-logged", "Authorization", "Digest", "reader"):
            self.assertNotIn(secret, output)

    def test_total_matches_23_requests_0_10_20_and_never_30(self):
        session = SequenceSession([user_page(range(1, 11), 23), user_page(range(11, 21), 23),
                                   user_page(range(21, 24), 23), user_page([], 23)])
        result = HikvisionClient(config(), session).discover_users()
        positions = [c["searchResultPosition"] for c in search_conditions(session)]
        self.assertEqual(positions, [0, 10, 20])
        self.assertNotIn(30, positions)
        self.assertEqual(result["count"], 23)
        self.assertTrue(result["complete"])

    def test_total_matches_below_ten_requests_only_position_zero(self):
        session = SequenceSession([user_page(range(1, 6), 5, status="MORE"), user_page([], 5)])
        result = HikvisionClient(config(), session).discover_users()
        self.assertEqual([c["searchResultPosition"] for c in search_conditions(session)], [0])
        self.assertEqual(result["count"], 5)

    def test_position_advances_by_num_of_matches_not_page_size(self):
        # Device returns short pages of 4 even though maxResults is 10.
        session = SequenceSession([user_page(range(1, 5), 9), user_page(range(5, 9), 9), user_page([9], 9),
                                   user_page([], 9)])
        result = HikvisionClient(config(), session).discover_users()
        self.assertEqual([c["searchResultPosition"] for c in search_conditions(session)], [0, 4, 8])
        self.assertEqual(result["count"], 9)

    def test_max_results_is_always_ten(self):
        session = SequenceSession([user_page(range(1, 11), 20), user_page(range(11, 21), 20)])
        client = HikvisionClient(config(), session)
        client.discover_users()
        client.search_users()
        client.search_users(position=0, page_size=100)
        client.search_users_page(max_results=50)
        for condition in search_conditions(session):
            self.assertEqual(condition["maxResults"], 10)
            self.assertEqual(set(condition), {"searchID", "searchResultPosition", "maxResults"})

    def test_empty_result_stops_after_one_request(self):
        session = SequenceSession([Response(payload={"UserInfoSearch": {
            "searchID": "1", "responseStatusStrg": "NO MATCH", "numOfMatches": 0, "totalMatches": 0}})])
        result = HikvisionClient(config(), session).discover_users()
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(result["users"], [])
        self.assertEqual(result["totalMatches"], 0)
        self.assertTrue(result["complete"])

    def test_pagination_stops_when_position_reaches_total_matches(self):
        # Device keeps saying MORE, but totalMatches says there is nothing left.
        session = SequenceSession([user_page(range(1, 11), 10, status="MORE")])
        HikvisionClient(config(), session).discover_users()
        self.assertEqual(len(session.calls), 1)

    def test_pagination_stops_when_device_repeats_a_page(self):
        session = SequenceSession([user_page(range(1, 11), 0, status="MORE", include_counts=False)])
        result = HikvisionClient(config(), session).discover_users()
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(result["count"], 10)
        self.assertFalse(result["complete"])

    def test_pagination_without_counts_stops_on_ok_status(self):
        session = SequenceSession([user_page(range(1, 11), 0, status="MORE", include_counts=False),
                                   user_page(range(11, 14), 0, status="OK", include_counts=False)])
        result = HikvisionClient(config(), session).discover_users()
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(result["count"], 13)

    def test_legacy_search_users_returns_users_and_more(self):
        users, more = HikvisionClient(config(), SequenceSession([user_page(range(1, 11), 23)])).search_users()
        self.assertEqual(users[0]["employeeNo"], "1")
        self.assertTrue(more)

    def test_malformed_user_search_response_is_safe_error(self):
        for response in (Response(payload=ValueError("bad json")),
                         Response(payload={"UserInfoSearch": {"UserInfo": "not-a-list"}}),
                         Response(payload={"UserInfoSearch": {"UserInfo": ["x"]}}),
                         Response(payload={"UserInfoSearch": {"numOfMatches": "many", "UserInfo": []}})):
            with self.assertRaises(DeviceError) as ctx:
                HikvisionClient(config(), SequenceSession([response])).discover_users()
            self.assertIn("invalid UserInfoSearch", str(ctx.exception))

    def assert_logged_error_is_sanitized(self, response, expected):
        with self.assertLogs("epca_attendance_agent.hikvision", level="WARNING") as logs:
            with self.assertRaises(DeviceError) as ctx:
                HikvisionClient(config(), SequenceSession([response])).discover_users()
        output = "\n".join(logs.output) + "\n" + str(ctx.exception)
        for fragment in expected:
            self.assertIn(fragment, output)
        for secret in ("password-not-logged", "cloud-token-not-logged", "Authorization", "Digest", "reader"):
            self.assertNotIn(secret, output)

    def test_http_400_json_error_is_logged_without_credentials(self):
        body = (b'{"statusCode": 6, "statusString": "Invalid Content", "subStatusCode": "badParameters",'
                b' "errorCode": 1610637344, "errorMsg": "maxResults"}')
        self.assert_logged_error_is_sanitized(Response(status=400, content=body), [
            "POST /ISAPI/AccessControl/UserInfo/Search?format=json", "HTTP 400", "statusCode=6",
            "statusString=Invalid Content", "subStatusCode=badParameters", "errorCode=1610637344",
            "errorMsg=maxResults"])

    def test_http_400_xml_error_is_logged_without_credentials(self):
        body = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
                b'<ResponseStatus version="2.0" xmlns="http://www.isapi.org/ver20/XMLSchema">'
                b'<requestURL>/ISAPI/AccessControl/UserInfo/Search</requestURL><statusCode>6</statusCode>'
                b'<statusString>Invalid Content</statusString><subStatusCode>badJsonFormat</subStatusCode>'
                b'</ResponseStatus>')
        self.assert_logged_error_is_sanitized(Response(status=400, content=body), [
            "HTTP 400", "statusString=Invalid Content", "subStatusCode=badJsonFormat"])

    def test_http_error_without_body_still_reports_status(self):
        self.assert_logged_error_is_sanitized(Response(status=401), ["POST /ISAPI/AccessControl/UserInfo/Search",
                                                                    "HTTP 401"])


# Shape of the verified HTTP 200 DS-K1T8003MF AcsEvent response (trimmed to two InfoList entries).
ACS_EVENT_RESPONSE = {"AcsEvent": {
    "searchID": "1", "responseStatusStrg": "MORE", "numOfMatches": 10, "totalMatches": 46328,
    "InfoList": [
        {"major": 5, "minor": 38, "time": "2022-05-11T13:00:49+08:00", "employeeNoString": "2", "serialNo": 32,
         "userType": "normal", "attendanceStatus": "undefined", "statusValue": 0},
        {"major": 5, "minor": 38, "time": "2022-05-11T13:05:12+08:00", "employeeNoString": "93", "serialNo": 35,
         "userType": "normal", "attendanceStatus": "undefined", "statusValue": 0},
    ]}}


class AcsEventTests(unittest.TestCase):
    def test_exact_acs_event_cond_request_structure(self):
        session = SequenceSession([Response(payload=ACS_EVENT_RESPONSE)])
        HikvisionClient(config(), session).search_events_page(0)
        args, kwargs = session.calls[0]
        self.assertEqual(args, ("POST", "http://192.168.88.187/ISAPI/AccessControl/AcsEvent?format=json"))
        self.assertEqual(kwargs["data"], b'{"AcsEventCond": {"searchID": "1", "searchResultPosition": 0, '
                                         b'"maxResults": 10, "major": 5, "minor": 38}}')
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")
        self.assertNotIn("json", kwargs)
        condition = json.loads(kwargs["data"])["AcsEventCond"]
        self.assertIsInstance(condition["searchID"], str)
        for name in ("searchResultPosition", "maxResults", "major", "minor"):
            self.assertIs(type(condition[name]), int)

    def test_http_200_response_is_parsed(self):
        page = HikvisionClient(config(), SequenceSession([Response(payload=ACS_EVENT_RESPONSE)])).search_events_page(0)
        self.assertEqual(page["responseStatusStrg"], "MORE")
        self.assertTrue(page["more"])
        self.assertEqual((page["numOfMatches"], page["totalMatches"]), (10, 46328))
        first = page["events"][0]
        self.assertEqual((first["serialNo"], first["employeeNoString"]), (32, "2"))
        self.assertEqual(first["time"], "2022-05-11T13:00:49+08:00")  # untouched, offset preserved
        self.assertEqual(first, ACS_EVENT_RESPONSE["AcsEvent"]["InfoList"][0])  # raw payload preserved
        self.assertEqual([e["serialNo"] for e in page["events"]], [32, 35])

    def test_legacy_search_events_uses_total_matches_for_more(self):
        events, more = HikvisionClient(config(), SequenceSession([Response(payload=ACS_EVENT_RESPONSE)])).search_events(0)
        self.assertEqual(len(events), 2)
        self.assertTrue(more)

    def test_empty_acs_event_result(self):
        empty = {"AcsEvent": {"searchID": "1", "responseStatusStrg": "NO MATCH", "numOfMatches": 0, "totalMatches": 0}}
        page = HikvisionClient(config(), SequenceSession([Response(payload=empty)])).search_events_page(0)
        self.assertEqual((page["events"], page["numOfMatches"], page["totalMatches"]), ([], 0, 0))
        self.assertFalse(page["more"])

    def test_malformed_acs_event_response_is_safe_error(self):
        for response in (Response(payload=ValueError("bad json")),
                         Response(payload={"AcsEvent": {"InfoList": "nope"}}),
                         Response(payload={"AcsEvent": {"InfoList": [1, 2]}}),
                         Response(payload={"AcsEvent": {"totalMatches": "lots", "InfoList": []}})):
            with self.assertRaises(DeviceError) as ctx:
                HikvisionClient(config(), SequenceSession([response])).search_events_page(0)
            self.assertIn("invalid AcsEvent", str(ctx.exception))

    def test_event_requests_are_logged_without_credentials(self):
        # Per-poll request details are DEBUG so a 24/7 service does not log every few seconds.
        with self.assertLogs("epca_attendance_agent.hikvision", level="DEBUG") as logs:
            HikvisionClient(config(), SequenceSession([Response(payload=ACS_EVENT_RESPONSE)])).search_events_page(20)
        output = "\n".join(logs.output)
        self.assertIn("Hikvision event discovery: searchID=1 searchResultPosition=20 maxResults=10 major=5 minor=38",
                      output)
        self.assertIn('"AcsEventCond": {"searchID": "1", "searchResultPosition": 20', output)
        for secret in ("password-not-logged", "cloud-token-not-logged", "Authorization", "Digest", "reader"):
            self.assertNotIn(secret, output)

    def test_http_400_bad_parameters_is_sanitized(self):
        body = (b'{"statusCode": 6, "statusString": "Invalid Content", "subStatusCode": "badParameters",'
                b' "errorCode": 1610612737, "errorMsg": "searchID"}')
        with self.assertLogs("epca_attendance_agent.hikvision", level="WARNING") as logs:
            with self.assertRaises(DeviceError) as ctx:
                HikvisionClient(config(), SequenceSession([Response(status=400, content=body)])).search_events_page(0)
        output = "\n".join(logs.output) + "\n" + str(ctx.exception)
        for fragment in ("POST /ISAPI/AccessControl/AcsEvent?format=json", "HTTP 400", "statusCode=6",
                         "statusString=Invalid Content", "subStatusCode=badParameters", "errorCode=1610612737"):
            self.assertIn(fragment, output)
        for secret in ("password-not-logged", "cloud-token-not-logged", "Authorization", "Digest", "reader"):
            self.assertNotIn(secret, output)


class ScriptedSession:
    """A requests.Session stand-in; each instance answers from its own script of responses/exceptions."""
    def __init__(self, script):
        self.script, self.auth, self.calls, self.closed = list(script), None, [], False
    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        outcome = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome
    def close(self): self.closed = True


class SessionFactory:
    def __init__(self, *scripts): self.scripts, self.sessions = list(scripts), []
    def __call__(self):
        session = ScriptedSession(self.scripts.pop(0) if self.scripts else [Response(status=500)])
        self.sessions.append(session)
        return session


EVENTS_OK = {"AcsEvent": {"searchID": "1", "responseStatusStrg": "OK", "numOfMatches": 0, "totalMatches": 0}}
SECRETS = ("password-not-logged", "cloud-token-not-logged", "Authorization", "Digest", "reader", "nonce", "response=")


class DigestSessionTests(unittest.TestCase):
    def test_digest_session_is_reused_across_requests(self):
        factory = SessionFactory([Response(payload=EVENTS_OK)])
        client = HikvisionClient(config(), session_factory=factory)
        for _ in range(3):
            client.search_events_page(0)
        self.assertEqual(len(factory.sessions), 1)
        self.assertEqual(len(factory.sessions[0].calls), 3)
        auth = factory.sessions[0].auth
        self.assertIsInstance(auth, HTTPDigestAuth)
        self.assertEqual((auth.username, auth.password), ("reader", "password-not-logged"))

    def test_final_401_recreates_session_and_retry_succeeds(self):
        factory = SessionFactory([Response(status=401)], [Response(payload=EVENTS_OK)])
        client = HikvisionClient(config(), session_factory=factory)
        page = client.search_events_page(0)
        self.assertEqual(page["totalMatches"], 0)
        self.assertEqual(len(factory.sessions), 2)
        old, new = factory.sessions
        self.assertTrue(old.closed)
        self.assertIs(client.session, new)
        self.assertIsInstance(new.auth, HTTPDigestAuth)
        # identical request body on the retry: event logic is untouched
        self.assertEqual(old.calls[0][1]["data"], new.calls[0][1]["data"])

    def test_repeated_401_retries_exactly_once_then_fails(self):
        factory = SessionFactory([Response(status=401)], [Response(status=401)], [Response(payload=EVENTS_OK)])
        client = HikvisionClient(config(), session_factory=factory)
        with self.assertRaises(DeviceError) as ctx:
            client.search_events_page(0)
        self.assertIn("HTTP 401", str(ctx.exception))
        self.assertEqual(len(factory.sessions), 2)  # original + one fresh session, never a third
        self.assertEqual(sum(len(s.calls) for s in factory.sessions), 2)

    def test_401_recovery_logs_contain_no_credentials(self):
        factory = SessionFactory([Response(status=401)], [Response(status=401)])
        with self.assertLogs("epca_attendance_agent.hikvision", level="INFO") as logs:
            with self.assertRaises(DeviceError) as ctx:
                HikvisionClient(config(), session_factory=factory).search_events_page(0)
        output = "\n".join(logs.output) + "\n" + str(ctx.exception)
        self.assertIn("recreating the session and retrying once", output)
        for secret in SECRETS:
            self.assertNotIn(secret, output)

    def test_timeout_is_a_device_error_without_session_reset(self):
        from requests import Timeout
        factory = SessionFactory([Timeout("read timed out"), Response(payload=EVENTS_OK)])
        client = HikvisionClient(config(), session_factory=factory)
        with self.assertRaises(DeviceError) as ctx:
            client.search_events_page(0)
        self.assertIn("Timeout", str(ctx.exception))
        self.assertEqual(client.search_events_page(0)["totalMatches"], 0)  # next call recovers
        self.assertEqual(len(factory.sessions), 1)

    def test_non_401_errors_are_not_retried(self):
        factory = SessionFactory([Response(status=400, content=b'{"statusCode": 6}')])
        with self.assertRaises(DeviceError):
            HikvisionClient(config(), session_factory=factory).search_events_page(0)
        self.assertEqual(len(factory.sessions), 1)
        self.assertEqual(len(factory.sessions[0].calls), 1)


class ClientTests(unittest.TestCase):
    def test_digest_and_confirmed_event_pagination_response(self):
        session = Session(Response(payload={"AcsEvent": {"responseStatusStrg": "MORE", "InfoList": [{"serialNo": 1}]}}))
        client = HikvisionClient(config(), session)
        events, more = client.get_events()
        self.assertIsInstance(session.auth, HTTPDigestAuth)
        self.assertEqual(events[0]["serialNo"], 1)
        self.assertTrue(more)
        self.assertEqual(session.calls[0][0][1], "http://192.168.88.187/ISAPI/AccessControl/AcsEvent?format=json")

    def test_malformed_device_response_is_safe_error(self):
        client = HikvisionClient(config(), Session(Response(payload=ValueError("bad json"))))
        with self.assertRaises(DeviceError): client.get_events()

    def test_cloud_auth_failure(self):
        client = EpcClient(config(), Session(Response(status=401)))
        with self.assertRaises(CloudAuthenticationError): client.send_events([])
