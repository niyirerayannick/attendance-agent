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
    def __init__(self, status=200, payload=None): self.status_code, self.payload = status, payload if payload is not None else {}
    def raise_for_status(self):
        if self.status_code >= 400:
            from requests import HTTPError
            raise HTTPError("failed")
    def json(self):
        if isinstance(self.payload, Exception): raise self.payload
        return self.payload


class Session:
    def __init__(self, response): self.response, self.auth, self.calls = response, None, []
    def request(self, *args, **kwargs): self.calls.append((args, kwargs)); return self.response
    def post(self, *args, **kwargs): self.calls.append((args, kwargs)); return self.response


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
