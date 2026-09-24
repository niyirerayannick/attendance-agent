"""24/7 production hardening: Hikvision lockout protection, outage recovery, durable queue, shutdown.

Everything is mocked; no test talks to a real Hikvision terminal or EPCA ONE.
"""

import io
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import requests
from requests.auth import HTTPDigestAuth

try:
    import attendance_agent.agent as agent_module
    from attendance_agent.agent import (BOOTSTRAP_STATE_KEY, DEVICE_COOLDOWN_STATE_KEY, AttendanceAgent,
                                        install_signal_handlers)
    from attendance_agent.cloud import EpcClient
    from attendance_agent.config import AgentConfig
    from attendance_agent.hikvision import (DeviceAuthenticationError, DeviceError, DeviceLockedError,
                                            HikvisionClient, parse_auth_failure)
    from attendance_agent.storage import AgentStore, StorageError
except ModuleNotFoundError:
    import agent as agent_module
    from agent import BOOTSTRAP_STATE_KEY, DEVICE_COOLDOWN_STATE_KEY, AttendanceAgent, install_signal_handlers
    from cloud import EpcClient
    from config import AgentConfig
    from hikvision import DeviceAuthenticationError, DeviceError, DeviceLockedError, HikvisionClient, parse_auth_failure
    from storage import AgentStore, StorageError


SOURCE_DIR = Path(__file__).resolve().parents[1]
PASSWORD, TOKEN, USERNAME = "hik-password-never-logged", "epca-token-never-logged", "hik-user-never-logged"
# Strings that must never reach a log line or exception message.
SECRETS = (PASSWORD, TOKEN, USERNAME, "Authorization", "Digest", "nonce", "response=", "Bearer")
ACS_COND = {"searchID": "1", "searchResultPosition": 0, "maxResults": 10, "major": 5, "minor": 38}

# The body the physical DS-K1T8003MF (V1.3.37) returned once its admin account was locked.
LOCKED_XML = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
              b'<userCheck version="2.0" xmlns="http://www.hikvision.com/ver20/XMLSchema">\n'
              b'<statusValue>401</statusValue>\n<statusString>Unauthorized</statusString>\n'
              b'<lockStatus>lock</lockStatus>\n<unlockTime>1795</unlockTime>\n'
              b'<retryLoginTime>0</retryLoginTime>\n</userCheck>\n')
UNLOCKED_401_XML = (b'<?xml version="1.0" encoding="UTF-8"?>\n<userCheck><statusValue>401</statusValue>'
                    b'<statusString>Unauthorized</statusString><lockStatus>unlock</lockStatus>'
                    b'<unlockTime>0</unlockTime><retryLoginTime>4</retryLoginTime></userCheck>')
# The Digest challenge a real 401 carries; its nonce must never be logged.
CHALLENGE = {"WWW-Authenticate": 'Digest realm="DS-K1T8003MF", nonce="abc123nonce", qop="auth"',
             "Content-Type": "application/xml"}


def make_config(db_path, **overrides):
    base = AgentConfig("192.168.88.187", USERNAME, PASSWORD, "https://one.epcafrica.example/api/internal/attendance/events/",
                       "EPCA-HQ-01", TOKEN, 15, db_path)
    return replace(base, **overrides)


def raw_event(serial, when=None):
    when = when or datetime.now(timezone.utc) - timedelta(minutes=5)
    return {"major": 5, "minor": 38, "time": when.astimezone(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
            "employeeNoString": "93", "serialNo": serial, "userType": "normal", "attendanceStatus": "undefined",
            "currentVerifyMode": "fingerPrint"}


class FakeClock:
    def __init__(self, now=1000.0): self.now = now
    def __call__(self): return self.now
    def advance(self, seconds): self.now += seconds


class Response:
    def __init__(self, status=200, payload=None, content=b"", headers=None):
        self.status_code, self.payload, self.headers = status, payload, headers or {}
        self.content = content if content or payload is None else json.dumps(payload).encode()
        self.closed = False
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")
    def json(self):
        if self.payload is None:
            raise ValueError("no json")
        return self.payload
    def close(self): self.closed = True


def locked(unlock=1795):
    body = LOCKED_XML.replace(b"1795", str(unlock).encode()) if unlock is not None else \
        LOCKED_XML.replace(b"<unlockTime>1795</unlockTime>\n", b"")
    return Response(401, content=body, headers=CHALLENGE)


def unauthorized(body=b""):
    return Response(401, content=body, headers=CHALLENGE)


class DeviceSession:
    """requests.Session stand-in serving an oldest-first AcsEvent log; ``script`` entries override responses."""
    def __init__(self, log, script):
        self.log, self.script, self.auth, self.calls, self.closed = log, list(script), None, [], False
    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.script:
            outcome = self.script.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if outcome is not None:
                return outcome
        body = json.loads(kwargs["data"])["AcsEventCond"]
        position = body["searchResultPosition"]
        page = self.log[position:position + body["maxResults"]]
        return Response(200, {"AcsEvent": {"searchID": "1", "numOfMatches": len(page), "totalMatches": len(self.log),
                                           "InfoList": page, "responseStatusStrg": "OK"}})
    def close(self): self.closed = True


class DeviceSessions:
    """session_factory: each new session (= each device request) takes the next script; the log is shared."""
    def __init__(self, log, *scripts):
        self.log, self.scripts, self.sessions = log, list(scripts), []
    def __call__(self):
        session = DeviceSession(self.log, self.scripts.pop(0) if self.scripts else [])
        self.sessions.append(session)
        return session
    @property
    def requests(self): return sum(len(s.calls) for s in self.sessions)
    def bodies(self): return [json.loads(c[2]["data"])["AcsEventCond"] for s in self.sessions for c in s.calls
                              if "data" in c[2]]


class CloudSession:
    """EPCA ONE stand-in; script entries are exceptions or HTTP status codes, then it accepts everything."""
    def __init__(self, script=()):
        self.script, self.batches, self.closed = list(script), [], False
    def post(self, url, json=None, **kwargs):
        outcome = self.script.pop(0) if self.script else 201
        if isinstance(outcome, Exception):
            raise outcome
        if outcome < 300:
            self.batches.append([event["serial_no"] for event in json["events"]])
            return Response(outcome, {"created": len(json["events"]), "duplicates": 0, "errors": 0})
        return Response(outcome, {"detail": "unavailable"})
    def close(self): self.closed = True
    @property
    def delivered(self): return [serial for batch in self.batches for serial in batch]


class ScriptedStop(threading.Event):
    """agent.stop_requested replacement: advances the fake clock instead of sleeping, stops after N waits."""
    def __init__(self, clock, cycles):
        super().__init__()
        self.clock, self.cycles, self.waits = clock, cycles, []
    def wait(self, timeout=None):
        self.waits.append(timeout)
        self.clock.advance(timeout or 0)
        if len(self.waits) >= self.cycles:
            self.set()
        return self.is_set()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "data" / "attendance_agent.db")
        self.clock = FakeClock()

    def tearDown(self):
        self.tmp.cleanup()

    def device(self, factory, **overrides):
        return HikvisionClient(make_config(self.db_path, **overrides), session_factory=factory, clock=self.clock)

    def agent(self, device, cloud_session=None, store=None, bootstrapped=True, **overrides):
        store = store or AgentStore(self.db_path)
        if bootstrapped:
            store.set_state(BOOTSTRAP_STATE_KEY, "1")
        cloud = EpcClient(make_config(self.db_path), cloud_session or CloudSession())
        agent = AttendanceAgent(make_config(self.db_path, **overrides), store, device, cloud)
        agent.clock = self.clock
        return agent

    def run_cycles(self, agent, cycles):
        agent.stop_requested = ScriptedStop(self.clock, cycles)
        agent.run()  # must return normally; it closes the store
        return agent.stop_requested.waits

    def reopen(self):
        return AgentStore(self.db_path)

    def assert_no_secrets(self, text):
        for secret in SECRETS:
            self.assertNotIn(secret, text)


class LockoutParsingTests(unittest.TestCase):
    def test_locked_401_xml_parses_unlock_time(self):
        failure = parse_auth_failure(LOCKED_XML)
        self.assertTrue(failure.locked)
        self.assertEqual((failure.unlock_seconds, failure.retries_left, failure.status), (1795, 0, "Unauthorized"))

    def test_locked_401_json_is_recognised(self):
        body = json.dumps({"statusCode": 4, "statusString": "Unauthorized", "lockStatus": "lock",
                           "unlockTime": 600, "retryLoginTime": 0}).encode()
        self.assertEqual(parse_auth_failure(body).unlock_seconds, 600)
        self.assertTrue(parse_auth_failure(body).locked)

    def test_implausible_or_missing_unlock_time_is_unknown(self):
        for value in (b"0", b"-5", b"abc", b"999999999"):
            failure = parse_auth_failure(LOCKED_XML.replace(b"1795", value))
            self.assertTrue(failure.locked)
            self.assertIsNone(failure.unlock_seconds)

    def test_ordinary_401_bodies_are_not_locks(self):
        for body in (b"", None, UNLOCKED_401_XML, b"<html><body>401 Unauthorized", b"not xml",
                     b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "lock">]><userCheck><lockStatus>&a;</lockStatus>'
                     b'</userCheck>'):
            self.assertFalse(parse_auth_failure(body).locked, body)
        self.assertEqual(parse_auth_failure(UNLOCKED_401_XML).retries_left, 4)


class LockoutClientTests(Base):
    def test_lock_pauses_all_requests_without_a_retry(self):
        factory = DeviceSessions([], [locked()])
        client = self.device(factory)
        with self.assertLogs("epca_attendance_agent.hikvision", level="WARNING") as logs:
            with self.assertRaises(DeviceLockedError) as ctx:
                client.search_events_page(0)
        self.assertEqual(ctx.exception.retry_after, 1795 + 30)  # device unlockTime plus the safety margin
        self.assertIn("Hikvision account locked; pausing device requests for approximately 1825 seconds.",
                      "\n".join(logs.output))
        self.assertEqual((len(factory.sessions), factory.requests), (1, 1))  # no fresh-session retry while locked

    def test_no_requests_of_any_kind_during_lock_cooldown(self):
        factory = DeviceSessions([], [locked(600)])
        client = self.device(factory)
        self.assertRaises(DeviceLockedError, client.search_events_page, 0)
        for step in (0, 1, 60, 300, 600, 628):  # up to 1s before 600+30 expires
            self.clock.now = 1000.0 + step
            for call in (lambda: client.search_events_page(0), client.device_info, client.test_connection,
                         client.discover_users, client.event_capabilities):
                with self.assertRaises(DeviceLockedError):
                    call()
        self.assertEqual((len(factory.sessions), factory.requests), (1, 1))

    def test_fresh_session_and_digest_auth_after_cooldown(self):
        factory = DeviceSessions([raw_event(1)], [locked(600)], [])
        client = self.device(factory)
        self.assertRaises(DeviceLockedError, client.search_events_page, 0)
        first = client.session
        self.clock.advance(630)
        with self.assertLogs("epca_attendance_agent.hikvision", level="INFO") as logs:
            page = client.search_events_page(0)
        self.assertEqual(page["totalMatches"], 1)
        self.assertEqual(len(factory.sessions), 2)
        self.assertTrue(first.closed)
        self.assertIsNot(client.session, first)
        self.assertIsInstance(client.session.auth, HTTPDigestAuth)
        self.assertIsNot(client.session.auth, first.auth)
        self.assertEqual(len(client.session.calls), 1)  # exactly one normal request
        self.assertTrue(any("authentication succeeded after the cooldown" in line for line in logs.output))

    def test_repeated_lock_after_cooldown_waits_again(self):
        factory = DeviceSessions([], [locked(600)], [locked(900)], [])
        client = self.device(factory)
        self.assertRaises(DeviceLockedError, client.search_events_page, 0)
        self.clock.advance(630)
        with self.assertRaises(DeviceLockedError) as ctx:
            client.search_events_page(0)
        self.assertEqual(ctx.exception.retry_after, 930)  # the NEW unlockTime, not the old one
        self.assertEqual(factory.requests, 2)  # one per cooldown, never a burst
        self.clock.advance(929)
        self.assertRaises(DeviceLockedError, client.search_events_page, 0)
        self.assertEqual((factory.requests, len(factory.sessions)), (2, 2))
        self.clock.advance(1)
        client.search_events_page(0)
        self.assertEqual((factory.requests, len(factory.sessions)), (3, 3))

    def test_lock_without_unlock_time_uses_conservative_default(self):
        client = self.device(DeviceSessions([], [locked(None)]))
        with self.assertRaises(DeviceLockedError) as ctx:
            client.search_events_page(0)
        self.assertEqual(ctx.exception.retry_after, 1800 + 30)

    def test_lock_after_a_genuine_401(self):
        factory = DeviceSessions([], [unauthorized()], [locked(300)])
        client = self.device(factory)
        self.assertRaises(DeviceAuthenticationError, client.search_events_page, 0)
        self.clock.advance(300)
        with self.assertRaises(DeviceLockedError) as ctx:
            client.search_events_page(0)
        self.assertEqual(ctx.exception.retry_after, 330)
        self.assertEqual(factory.requests, 2)

    def test_genuine_401_has_no_retry_and_escalating_cooldown(self):
        factory = DeviceSessions([], [unauthorized(UNLOCKED_401_XML)], [unauthorized()], [unauthorized()], [])
        client = self.device(factory)
        with self.assertRaises(DeviceAuthenticationError) as ctx:
            client.search_events_page(0)
        self.assertIn("HTTP 401", str(ctx.exception))
        self.assertEqual(ctx.exception.retry_after, 300)
        self.assertEqual(factory.requests, 1)  # a final 401 on a fresh session is never retried
        self.clock.advance(299)
        self.assertRaises(DeviceAuthenticationError, client.search_events_page, 0)
        self.assertEqual(factory.requests, 1)
        self.clock.advance(1)
        with self.assertRaises(DeviceAuthenticationError) as ctx:
            client.search_events_page(0)
        self.assertEqual((ctx.exception.retry_after, factory.requests), (600, 2))
        self.clock.advance(600)
        with self.assertRaises(DeviceAuthenticationError) as ctx:
            client.search_events_page(0)
        self.assertEqual((ctx.exception.retry_after, factory.requests), (1200, 3))
        self.clock.advance(1200)
        client.search_events_page(0)  # success resets the escalation
        self.assertEqual(client._auth_failures, 0)

    def test_no_recovery_retry_when_device_says_last_attempt(self):
        factory = DeviceSessions([], [unauthorized(UNLOCKED_401_XML.replace(b">4<", b">1<"))])
        with self.assertRaises(DeviceAuthenticationError):
            self.device(factory).search_events_page(0)
        self.assertEqual(factory.requests, 1)

    def test_timeouts_are_not_cooldowns(self):
        factory = DeviceSessions([], [requests.ConnectTimeout("timed out"), None])
        client = self.device(factory)
        with self.assertRaises(DeviceError) as ctx:
            client.search_events_page(0)
        self.assertNotIsInstance(ctx.exception, DeviceLockedError)
        client.search_events_page(0)  # the device is retried as soon as the caller wants
        self.assertEqual(len(factory.sessions), 2)

    def test_credentials_never_logged(self):
        factory = DeviceSessions([], [unauthorized(UNLOCKED_401_XML)], [unauthorized()], [locked(60)], [])
        client = self.device(factory)
        errors = []
        with self.assertLogs("epca_attendance_agent", level="DEBUG") as logs:
            for advance in (0, 300, 600, 90):
                self.clock.advance(advance)
                try:
                    client.search_events_page(0)
                except DeviceError as exc:
                    errors.append(f"{exc} {exc!r}")
            client.search_events_page(0)
        output = "\n".join(logs.output + errors)
        self.assertIn("Hikvision account locked", output)
        self.assertIn("Hikvision authentication failed", output)
        self.assert_no_secrets(output)


class LockoutAgentTests(Base):
    def test_run_waits_out_the_lock_then_resumes_with_fresh_session(self):
        factory = DeviceSessions([raw_event(5)], [locked(600)], [])
        agent = self.agent(self.device(factory))
        waits = self.run_cycles(agent, 2)
        self.assertEqual(waits, [630, 15])
        self.assertEqual(len(factory.sessions), 2)
        self.assertEqual(factory.bodies()[-1], ACS_COND)  # same proven AcsEventCond after recovery
        store = self.reopen()
        self.assertEqual(store.queue_counts()["delivered"], 1)
        store.close()

    def test_locked_device_does_not_block_cloud_and_is_never_contacted(self):
        factory = DeviceSessions([], [locked(600)])
        store = AgentStore(self.db_path)
        store.commit_discovery([agent_module.event_from_isapi(raw_event(3))], cursor=3)
        cloud = CloudSession([requests.ConnectionError("offline"), 503, 502])
        agent = self.agent(self.device(factory), cloud, store=store)
        waits = self.run_cycles(agent, 4)
        self.assertEqual(factory.requests, 1)  # the cloud retries woke the loop; the device was left alone
        self.assertEqual(waits, [5, 10, 20, 595])
        self.assertEqual(cloud.delivered, [3])  # delivered while the terminal was still locked

    def test_lock_survives_restart_and_cli_checks(self):
        factory = DeviceSessions([], [locked(600)])
        agent = self.agent(self.device(factory))
        self.assertRaises(DeviceLockedError, agent.poll_device)
        agent.store.close()
        store = self.reopen()
        self.assertTrue(store.get_state(DEVICE_COOLDOWN_STATE_KEY).endswith(" lock"))
        second = DeviceSessions([], [])
        client = self.device(second)
        with self.assertLogs("epca_attendance_agent", level="WARNING") as logs:
            restarted = self.agent(client, store=store)
        self.assertTrue(any("remain paused" in line for line in logs.output))
        self.assertAlmostEqual(client.cooldown_remaining(), 630, delta=5)
        self.assertRaises(DeviceLockedError, restarted.poll_device)
        self.assertRaises(DeviceLockedError, restarted.test_device)
        self.assertIn("unavailable", restarted.health()["hikvision"])
        self.assertEqual(second.requests, 0)
        store.close()

    def test_run_logs_contain_no_credentials(self):
        factory = DeviceSessions([], [unauthorized()], [unauthorized()], [locked(60)], [])
        cloud = CloudSession([requests.ConnectionError("offline")])
        store = AgentStore(self.db_path)
        store.commit_discovery([agent_module.event_from_isapi(raw_event(1))], cursor=1)
        with self.assertLogs("epca_attendance_agent", level="DEBUG") as logs:
            self.run_cycles(self.agent(self.device(factory), cloud, store=store), 6)
        self.assert_no_secrets("\n".join(logs.output))


class OutageTests(Base):
    def test_internet_dns_timeout_and_5xx_do_not_terminate_run(self):
        cloud = CloudSession([
            requests.ConnectionError("Failed to resolve 'one.epcafrica.example' (NameResolutionError)"),
            requests.ConnectTimeout("connect timeout"), requests.ReadTimeout("read timeout"),
            requests.ConnectionError("Connection reset by peer"), 503, 500, 429])
        factory = DeviceSessions([raw_event(1), raw_event(2)], [])
        with self.assertLogs("epca_attendance_agent", level="INFO") as logs:
            waits = self.run_cycles(self.agent(self.device(factory), cloud), 60)
        self.assertEqual(cloud.delivered, [1, 2])  # delivered exactly once, after recovery
        output = "\n".join(logs.output)
        self.assertIn("Cloud connection recovered after 7 failed attempt(s)", output)
        self.assertIn("Delivered 2 queued event(s) to EPCA ONE", output)
        self.assertLessEqual(max(waits), 300)
        store = self.reopen()
        self.assertEqual(store.queue_counts(), {"pending": 0, "delivered": 2, "rejected": 0})
        store.close()

    def test_cloud_outage_is_not_hammered_and_device_keeps_polling(self):
        cloud = CloudSession([requests.ConnectionError("offline")] * 1000)
        factory = DeviceSessions([raw_event(1)], [])
        agent = self.agent(self.device(factory), cloud)
        attempts = []
        original = cloud.post
        cloud.post = lambda *a, **k: (attempts.append(self.clock.now), original(*a, **k))[1]
        self.run_cycles(agent, 200)
        elapsed = self.clock.now - 1000.0
        # Cloud attempts follow 5, 10, 20 ... 300s backoff; the device keeps its 15s poll interval.
        gaps = [b - a for a, b in zip(attempts, attempts[1:])]
        self.assertTrue(all(gap >= 5 for gap in gaps))
        self.assertLessEqual(len(attempts), elapsed / 300 + 8)
        self.assertGreater(factory.requests, len(attempts) * 3)

    def test_hikvision_outage_and_reboot_do_not_terminate_run(self):
        store = AgentStore(self.db_path)
        store.commit_discovery([], cursor=4)
        log = [raw_event(n) for n in range(1, 7)]
        factory = DeviceSessions(log, [requests.ConnectionError("No route to host")],
                                 [requests.ConnectTimeout("timed out")],
                                 [requests.ConnectionError("Connection refused")],  # rebooting
                                 [requests.ReadTimeout("read timed out")],
                                 [Response(503, content=b"")])
        with self.assertLogs("epca_attendance_agent", level="INFO") as logs:
            waits = self.run_cycles(self.agent(self.device(factory), store=store), 7)
        self.assertEqual(waits, [5, 10, 20, 40, 80, 15, 15])
        self.assertIn("Hikvision connection recovered after 5 failed attempt(s)", "\n".join(logs.output))
        store = self.reopen()
        self.assertEqual(store.discovery_cursor, 6)  # cursor was never reset during the outage
        self.assertEqual([e["serial_no"] for e in store.pending_events(10)], [])
        self.assertEqual(store.queue_counts()["delivered"], 2)  # only serials 5 and 6 are new
        store.close()

    def test_device_offline_keeps_delivering_queued_events(self):
        store = AgentStore(self.db_path)
        store.commit_discovery([agent_module.event_from_isapi(raw_event(8))], cursor=8)
        factory = DeviceSessions([], *[[requests.ConnectionError("offline")]] * 10)
        cloud = CloudSession()
        self.run_cycles(self.agent(self.device(factory), cloud, store=store), 2)
        self.assertEqual(cloud.delivered, [8])


class DurableQueueTests(Base):
    def test_queued_event_survives_restart_and_is_delivered_exactly_once(self):
        log = [raw_event(n) for n in (41, 42)]
        # 1st process: discovers both events while EPCA ONE / the Internet is down.
        offline = CloudSession([requests.ConnectionError("DNS failure")] * 100)
        self.run_cycles(self.agent(self.device(DeviceSessions(log, [])), offline, bootstrapped=False), 3)
        self.assertEqual(offline.delivered, [])
        store = self.reopen()  # process / Ubuntu restart
        self.assertEqual(store.queue_counts(), {"pending": 2, "delivered": 0, "rejected": 0})
        self.assertEqual(store.discovery_cursor, 42)
        self.assertEqual(store.get_state(BOOTSTRAP_STATE_KEY), "1")
        # 2nd process: the terminal still reports the same events; the cloud is back.
        online = CloudSession()
        factory = DeviceSessions(log, [])
        with self.assertLogs("epca_attendance_agent", level="INFO") as logs:
            self.run_cycles(self.agent(self.device(factory), online, store=store), 3)
        self.assertIn("2 queued event(s) from a previous run are pending delivery", "\n".join(logs.output))
        self.assertEqual(online.delivered, [41, 42])  # exactly once, nothing rediscovered
        store = self.reopen()
        self.assertEqual(store.queue_counts(), {"pending": 0, "delivered": 2, "rejected": 0})
        self.assertEqual(store.discovery_cursor, 42)
        store.close()
        # 3rd process: nothing is delivered twice.
        again = CloudSession()
        self.run_cycles(self.agent(self.device(DeviceSessions(log, [])), again), 2)
        self.assertEqual(again.delivered, [])

    def test_rejected_events_are_preserved(self):
        store = AgentStore(self.db_path)
        store.commit_discovery([agent_module.event_from_isapi(raw_event(n)) for n in (1, 2)], cursor=2)
        cloud = CloudSession()
        cloud.post = lambda url, json=None, **k: Response(201, {"errors": 1, "error_details": [
            {"index": 0, "error": "unmapped timestamp"}]})
        self.run_cycles(self.agent(self.device(DeviceSessions([], [])), cloud, store=store), 1)
        store = self.reopen()
        self.assertEqual(store.queue_counts(), {"pending": 0, "delivered": 1, "rejected": 1})
        store.close()

    def test_bootstrap_after_lock_recovery_is_unchanged(self):
        now = datetime.now(timezone.utc)
        log = [raw_event(n, datetime(2022, 5, 11, tzinfo=timezone.utc)) for n in range(1, 31)]
        log += [raw_event(31, now - timedelta(hours=30)), raw_event(32, now - timedelta(hours=2))]
        factory = DeviceSessions(log, [locked(60)], [])
        cloud = CloudSession()
        self.run_cycles(self.agent(self.device(factory), cloud, bootstrapped=False), 2)
        self.assertEqual(cloud.delivered, [32])  # only the last 24 hours
        self.assertEqual([b["searchResultPosition"] for b in factory.bodies()], [0, 0, 22])
        for body in factory.bodies():
            self.assertEqual({k: v for k, v in body.items() if k != "searchResultPosition"},
                             {k: v for k, v in ACS_COND.items() if k != "searchResultPosition"})
        store = self.reopen()
        self.assertEqual(store.discovery_cursor, 32)
        store.close()

    def test_unwritable_data_dir_is_a_clear_startup_error(self):
        blocker = Path(self.tmp.name) / "not-a-directory"
        blocker.write_text("x")
        with self.assertRaises(StorageError) as ctx:
            AgentStore(str(blocker / "sub" / "attendance_agent.db"))
        self.assertIn("could not be created", str(ctx.exception))
        self.assertIn("AGENT_DATA_DIR", str(ctx.exception))

    def test_main_reports_storage_error_and_exits_2(self):
        blocker = Path(self.tmp.name) / "file"
        blocker.write_text("x")
        env = {"HIKVISION_HOST": "192.0.2.1", "HIKVISION_USERNAME": "u", "HIKVISION_PASSWORD": PASSWORD,
               "EPCA_API_URL": "https://epca.example/api/", "EPCA_DEVICE_CODE": "c", "EPCA_DEVICE_TOKEN": TOKEN,
               "AGENT_DATA_DIR": str(blocker / "data")}
        root_level = logging.getLogger().level  # main() configures the root logger; keep other tests isolated
        self.addCleanup(logging.getLogger().setLevel, root_level)
        with patch.dict("os.environ", env, clear=True), patch.object(sys, "argv", ["agent.py", "run"]), \
                patch.object(agent_module, "APP_DIR", Path(self.tmp.name)), \
                patch.object(agent_module.logging, "basicConfig"), \
                self.assertLogs("epca_attendance_agent", level="ERROR") as logs:
            self.assertEqual(agent_module.main(), 2)
        self.assertIn("could not be created", "\n".join(logs.output))
        self.assert_no_secrets("\n".join(logs.output))

    def test_restart_does_not_reset_database(self):
        store = AgentStore(self.db_path)
        store.commit_discovery([agent_module.event_from_isapi(raw_event(9))], cursor=9, state={"x": "1"})
        store.close()
        for _ in range(3):
            store = self.reopen()
            self.assertEqual((store.discovery_cursor, store.queue_counts()["pending"], store.get_state("x")),
                             (9, 1, "1"))
            store.close()


class ShutdownTests(Base):
    def test_signal_handlers_stop_run_and_close_everything(self):
        handlers = {}
        factory = DeviceSessions([raw_event(1)], [])
        cloud = CloudSession()
        agent = self.agent(self.device(factory), cloud)
        with patch.object(signal, "signal", lambda signum, handler: handlers.__setitem__(signum, handler)):
            install_signal_handlers(agent)
        self.assertEqual(set(handlers), {signal.SIGTERM, signal.SIGINT})
        for signum in (signal.SIGTERM, signal.SIGINT):
            self.assertFalse(agent.stop_requested.is_set())
            handlers[signum](signum, None)
            self.assertTrue(agent.stop_requested.is_set())
            agent.stop_requested.clear()
        # A signal arriving mid-cycle ends the loop after the current request and transaction.
        original = agent.poll_device
        def poll_then_signal():
            result = original()
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return result
        agent.poll_device = poll_then_signal
        with self.assertLogs("epca_attendance_agent", level="INFO") as logs:
            agent.run()
        self.assertIn("Attendance agent stopped safely.", "\n".join(logs.output))
        self.assertTrue(agent.store._closed)
        self.assertTrue(factory.sessions[-1].closed)
        self.assertTrue(cloud.closed)
        self.assertEqual(cloud.delivered, [])  # stop honoured before the cloud phase
        store = self.reopen()
        self.assertEqual(store.queue_counts()["pending"], 1)  # committed discovery kept for the next start
        store.close()

    def test_stop_between_upload_batches_keeps_rest_pending(self):
        store = AgentStore(self.db_path)
        store.commit_discovery([agent_module.event_from_isapi(raw_event(n)) for n in range(1, 6)], cursor=5)
        cloud = CloudSession()
        agent = self.agent(self.device(DeviceSessions([], [])), cloud, store=store, batch_size=2)
        original = cloud.post
        def post_then_stop(*args, **kwargs):
            agent.request_stop()
            return original(*args, **kwargs)
        cloud.post = post_then_stop
        self.assertEqual(agent.upload_pending()["delivered"], 2)
        self.assertEqual(store.queue_counts(), {"pending": 3, "delivered": 2, "rejected": 0})
        store.close()


class DigestTerminal(requests.adapters.BaseAdapter):
    """Transport-level model of the DS-K1T8003MF (V1.3.37) behind a REAL requests.Session + HTTPDigestAuth.

    An unauthenticated request gets a Digest challenge with a new nonce. An authenticated request succeeds only
    with a nonce this terminal issued, used for the first time (nc=00000001). A reused nonce gets the final 401
    seen in production, which carries no new challenge. ``locked`` answers every authenticated request with the
    userCheck lock body.
    """
    def __init__(self, log, locked_body=None, wrong_password=False):
        super().__init__()
        self.log, self.locked_body, self.wrong_password = log, locked_body, wrong_password
        self.exchanges, self.issued, self.used, self.sessions = [], set(), set(), []

    def session(self):
        terminal = self
        class TrackedSession(requests.Session):
            closed = False
            def close(self):
                self.closed = True
                super().close()
        session = TrackedSession()
        session.mount("http://", terminal)
        self.sessions.append(session)
        return session

    def _reply(self, request, status, body=b"", headers=None):
        response = requests.Response()
        response.status_code, response._content, response.request = status, body, request
        response.headers = requests.structures.CaseInsensitiveDict(headers or {})
        response.url, response.connection, response.raw, response.encoding = request.url, self, io.BytesIO(body), "utf-8"
        return response

    def send(self, request, **kwargs):
        authorization = request.headers.get("Authorization", "")
        if not authorization.startswith("Digest "):
            nonce = f"n{len(self.issued)}"
            self.issued.add(nonce)
            self.exchanges.append(("challenge", None))
            return self._reply(request, 401, b"", {"WWW-Authenticate":
                                                   f'Digest realm="DS-K1T8003MF", nonce="{nonce}", qop="auth"'})
        fields = requests.utils.parse_dict_header(authorization[len("Digest "):])
        nonce, nc = fields.get("nonce"), fields.get("nc")
        self.exchanges.append(("digest", (nonce, nc)))
        if self.locked_body is not None:
            return self._reply(request, 401, self.locked_body, {"Content-Type": "application/xml"})
        if self.wrong_password or nonce not in self.issued or nonce in self.used or nc != "00000001":
            return self._reply(request, 401, UNLOCKED_401_XML, {"Content-Type": "application/xml"})
        self.used.add(nonce)
        body = json.loads(request.body)["AcsEventCond"]
        page = self.log[body["searchResultPosition"]:body["searchResultPosition"] + body["maxResults"]]
        payload = {"AcsEvent": {"searchID": "1", "numOfMatches": len(page), "totalMatches": len(self.log),
                                "InfoList": page, "responseStatusStrg": "OK"}}
        return self._reply(request, 200, json.dumps(payload).encode(), {"Content-Type": "application/json"})

    def close(self):
        pass


class DigestNegotiationTests(Base):
    def test_reused_digest_session_is_what_fails_on_this_firmware(self):
        terminal = DigestTerminal([raw_event(1)])
        session = terminal.session()
        session.auth = HTTPDigestAuth(USERNAME, PASSWORD)
        url = "http://192.168.88.187/ISAPI/AccessControl/AcsEvent?format=json"
        body = json.dumps({"AcsEventCond": ACS_COND}).encode()
        self.assertEqual(session.post(url, data=body).status_code, 200)
        self.assertEqual(session.post(url, data=body).status_code, 401)  # reused nonce: the production symptom

    def test_each_poll_negotiates_digest_freshly_and_succeeds(self):
        terminal = DigestTerminal([raw_event(1), raw_event(2)])
        client = self.device(terminal.session)
        with self.assertLogs("epca_attendance_agent", level="DEBUG") as logs:
            for _ in range(3):
                self.assertEqual(client.search_events_page(0)["totalMatches"], 2)
                self.clock.advance(30)
        # Standard negotiation per request: challenge -> one Digest request (nc=1, new nonce) -> 200.
        self.assertEqual(terminal.exchanges, [("challenge", None), ("digest", ("n0", "00000001")),
                                              ("challenge", None), ("digest", ("n1", "00000001")),
                                              ("challenge", None), ("digest", ("n2", "00000001"))])
        self.assertEqual([s.closed for s in terminal.sessions], [True, True, False])
        output = "\n".join(logs.output)
        self.assertNotIn("401", output)  # the internal challenge is not an application-level failure
        self.assertNotIn("authentication failed", output)
        self.assertEqual(client._auth_failures, 0)

    def test_genuine_final_401_after_negotiation_invokes_cooldown_once(self):
        terminal = DigestTerminal([], wrong_password=True)
        client = self.device(terminal.session)
        with self.assertRaises(DeviceAuthenticationError):
            client.search_events_page(0)
        self.assertEqual([kind for kind, _ in terminal.exchanges], ["challenge", "digest"])  # one login, no retry
        self.clock.advance(299)
        self.assertRaises(DeviceAuthenticationError, client.search_events_page, 0)
        self.assertEqual(len(terminal.exchanges), 2)

    def test_lock_after_negotiation_prevents_all_further_requests(self):
        terminal = DigestTerminal([], locked_body=LOCKED_XML.replace(b"1795", b"600"))
        agent = self.agent(self.device(terminal.session))
        with self.assertRaises(DeviceLockedError) as ctx:
            agent.poll_device()
        self.assertEqual(ctx.exception.retry_after, 630)
        self.assertEqual([kind for kind, _ in terminal.exchanges], ["challenge", "digest"])
        for step in (1, 300, 629):
            self.clock.now = 1000.0 + step
            self.assertRaises(DeviceLockedError, agent.poll_device)
            self.assertRaises(DeviceLockedError, agent.test_device)
        self.assertEqual(len(terminal.exchanges), 2)
        self.assertTrue(agent.store.get_state(DEVICE_COOLDOWN_STATE_KEY).endswith(" lock"))  # persisted in SQLite
        agent.store.close()

    def test_run_loop_polls_every_30s_without_401s_duplicates_or_cursor_change(self):
        log = [raw_event(n) for n in (7, 8)]
        terminal = DigestTerminal(log)
        cloud = CloudSession()
        store = AgentStore(self.db_path)
        store.commit_discovery([], cursor=6)
        with self.assertLogs("epca_attendance_agent", level="INFO") as logs:
            waits = self.run_cycles(self.agent(self.device(terminal.session), cloud, store=store,
                                               poll_interval_seconds=30), 3)
        self.assertEqual(waits, [30, 30, 30])
        self.assertEqual(cloud.delivered, [7, 8])  # each event exactly once
        self.assertEqual(len(terminal.sessions), 3)
        self.assertTrue(all(s.closed for s in terminal.sessions))
        self.assertEqual([kind for kind, _ in terminal.exchanges], ["challenge", "digest"] * 3)
        self.assertNotIn("HTTP 401", "\n".join(logs.output))
        store = self.reopen()
        self.assertEqual(store.discovery_cursor, 8)
        self.assertEqual(store.queue_counts(), {"pending": 0, "delivered": 2, "rejected": 0})
        store.close()


def _agent_env(tmp):
    """Unreachable terminal (refused loopback port) and unreachable cloud; the run must survive both."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("HIKVISION_", "EPCA_", "AGENT_"))
           and k not in {"PYTHONPATH", "POLL_INTERVAL_SECONDS", "LOG_LEVEL"}}
    env.update({"HIKVISION_HOST": "127.0.0.1:9", "HIKVISION_USERNAME": USERNAME, "HIKVISION_PASSWORD": PASSWORD,
                "EPCA_API_URL": "https://127.0.0.1:9/api/internal/attendance/events/", "EPCA_DEVICE_CODE": "EPCA-HQ-01",
                "EPCA_DEVICE_TOKEN": TOKEN, "AGENT_DATA_DIR": str(Path(tmp) / "state"), "POLL_INTERVAL_SECONDS": "15",
                "DEVICE_TIMEOUT_SECONDS": "2", "RETRY_INITIAL_SECONDS": "1", "RETRY_MAX_SECONDS": "2",
                "PYTHONUNBUFFERED": "1"})
    return env


class ProcessTests(unittest.TestCase):
    """Runs the real `python agent.py run` as systemd would, from a copy of the standalone layout."""

    def test_run_survives_outages_and_exits_cleanly_on_sigterm(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp) / "attendance-agent"
            project.mkdir()
            for name in ("agent.py", "config.py", "hikvision.py", "cloud.py", "storage.py"):
                (project / name).write_bytes((SOURCE_DIR / name).read_bytes())
            log_path = Path(tmp) / "journal.log"
            with open(log_path, "w", encoding="utf-8") as journal:
                process = subprocess.Popen([sys.executable, str(project / "agent.py"), "run"], cwd=tmp,
                                           env=_agent_env(tmp), stdout=journal, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    output = log_path.read_text(encoding="utf-8")
                    if output.count("Hikvision unavailable") >= 2:
                        break
                    time.sleep(0.2)
                self.assertIsNone(process.poll(), output)  # outages never terminate the process
                self.assertIn("Attendance agent started: device_code=EPCA-HQ-01", output)
                self.assertIn("poll_interval=15s", output)
                self.assertIn("cloud_host=127.0.0.1", output)
                self.assertGreaterEqual(output.count("Hikvision unavailable"), 2)
                if os.name == "nt":
                    return  # Windows has no SIGTERM delivery to a console child; covered by ShutdownTests
                process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=30), 0)
                output = log_path.read_text(encoding="utf-8")
                self.assertIn("Shutdown requested", output)
                self.assertIn("Attendance agent stopped safely.", output)
                for secret in (PASSWORD, TOKEN, USERNAME, "Authorization", "Digest ", "nonce"):
                    self.assertNotIn(secret, output)
                self.assertTrue((Path(tmp) / "state" / "attendance_agent.db").exists())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)


class ConfigDefaultsTests(unittest.TestCase):
    def test_lockout_settings_and_poll_interval_defaults(self):
        env = {"HIKVISION_HOST": "h", "HIKVISION_USERNAME": "u", "HIKVISION_PASSWORD": "p",
               "EPCA_API_URL": "https://epca.example/api/", "EPCA_DEVICE_CODE": "c", "EPCA_DEVICE_TOKEN": "t"}
        with patch.dict("os.environ", env, clear=True):
            config = AgentConfig.from_environment()
        self.assertEqual((config.hikvision_lock_default_seconds, config.hikvision_lock_margin_seconds,
                          config.hikvision_auth_cooldown_seconds), (1800, 30, 300))
        self.assertEqual(config.poll_interval_seconds, 60)  # unchanged default; never silently altered
        with patch.dict("os.environ", env | {"POLL_INTERVAL_SECONDS": "5", "HIKVISION_LOCK_MARGIN_SECONDS": "0"},
                        clear=True):
            config = AgentConfig.from_environment()
        self.assertEqual((config.poll_interval_seconds, config.hikvision_lock_margin_seconds), (5, 0))


if __name__ == "__main__":
    unittest.main()
