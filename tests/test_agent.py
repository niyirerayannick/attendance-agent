import json
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import requests
from requests.auth import HTTPDigestAuth

try:
    from attendance_agent.agent import BOOTSTRAP_STATE_KEY, AttendanceAgent, event_from_isapi
    from attendance_agent.cloud import CloudAuthenticationError, CloudError
    from attendance_agent.config import AgentConfig, ConfigurationError
    from attendance_agent.hikvision import DeviceError, HikvisionClient
    from attendance_agent.storage import AgentStore
except ModuleNotFoundError:
    from agent import BOOTSTRAP_STATE_KEY, AttendanceAgent, event_from_isapi
    from cloud import CloudAuthenticationError, CloudError
    from config import AgentConfig, ConfigurationError
    from hikvision import DeviceError, HikvisionClient
    from storage import AgentStore


def config(path):
    return AgentConfig("192.168.1.10", "reader", "secret", "https://epca.example/api/internal/attendance/events/",
                       "HQ-01", "token", 60, path)


def raw(serial=1):
    return {"serialNo": serial, "employeeNoString": "E-001", "time": "2026-09-21T08:00:00+02:00",
            "major": 5, "minor": 75, "attendanceStatus": "checkIn", "currentVerifyMode": "fingerPrint"}


class FakeDevice:
    """Models the terminal's AcsEvent search: an oldest-first log sliced by searchResultPosition."""
    def __init__(self, pages=None, events=None, page_size=10):
        self.log = list(events if events is not None else [e for page_events, _ in (pages or []) for e in page_events])
        self.page_size, self.positions = page_size, []
    @property
    def calls(self): return len(self.positions)
    def device_info(self): return {"deviceName": "Terminal"}
    def search_events_page(self, position):
        self.positions.append(position)
        if position >= len(self.log) and self.log:
            raise DeviceError("badParameters: position past the end")  # never request past the end
        page = self.log[position:position + self.page_size]
        return {"events": page, "numOfMatches": len(page), "totalMatches": len(self.log),
                "responseStatusStrg": "MORE" if position + len(page) < len(self.log) else "OK",
                "more": position + len(page) < len(self.log)}


class FakeCloud:
    def __init__(self, response=None, error=None): self.response, self.error, self.sent = response or {}, error, []
    def test_connection(self): return None
    def send_events(self, events):
        self.sent.append(events)
        if self.error: raise self.error
        return self.response


class AttendanceAgentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = AgentStore(str(Path(self.tmp.name) / "queue.sqlite3"))

    def tearDown(self):
        self.store.close(); self.tmp.cleanup()

    def make_agent(self, pages, cloud=None):
        # These tests cover an agent that is already past its initial sync; bootstrap has its own tests.
        self.store.set_state(BOOTSTRAP_STATE_KEY, "1")
        return AttendanceAgent(config(str(Path(self.tmp.name) / "queue.sqlite3")), self.store, FakeDevice(pages), cloud or FakeCloud())

    def test_event_normalisation(self):
        event = event_from_isapi(raw())
        self.assertEqual(event["serial_no"], 1)
        self.assertEqual(event["event_time"], "2026-09-21T08:00:00+02:00")
        self.assertIsNone(event_from_isapi({"serialNo": "bad"}))

    def test_cursor_advances_only_with_durable_queue_insert(self):
        agent = self.make_agent([([raw(3), raw(4)], False)])
        self.assertEqual(agent.poll_device()["queued"], 2)
        self.assertEqual(self.store.discovery_cursor, 4)
        self.assertEqual(self.store.queue_counts()["pending"], 2)

    def test_repoll_duplicate_is_not_requeued(self):
        agent = self.make_agent([([raw(2)], False)])
        agent.poll_device(); agent.poll_device()
        self.assertEqual(self.store.queue_counts()["pending"], 1)

    def test_restart_retains_cursor_and_pending_queue(self):
        agent = self.make_agent([([raw(7)], False)])
        agent.poll_device()
        self.store.close()
        self.store = AgentStore(str(Path(self.tmp.name) / "queue.sqlite3"))
        self.assertEqual(self.store.discovery_cursor, 7)
        self.assertEqual(self.store.queue_counts()["pending"], 1)

    def test_data_directory_selects_container_safe_default_database_path(self):
        variables = {
            "HIKVISION_HOST": "192.168.88.187", "HIKVISION_USERNAME": "reader", "HIKVISION_PASSWORD": "secret",
            "EPCA_API_URL": "https://epca.example/api/internal/attendance/events/", "EPCA_DEVICE_CODE": "EPCA-HQ-01",
            "EPCA_DEVICE_TOKEN": "token", "AGENT_DATA_DIR": str(Path(self.tmp.name) / "data"),
        }
        with patch.dict("os.environ", variables, clear=True):
            configured = AgentConfig.from_environment()
        self.assertEqual(configured.database_path, str(Path(self.tmp.name) / "data" / "attendance_agent.db"))
        durable_store = AgentStore(configured.database_path)
        durable_store.queue_events([event_from_isapi(raw(9))])
        durable_store.close()
        reopened = AgentStore(configured.database_path)
        self.assertEqual(reopened.discovery_cursor, 9)
        reopened.close()

    def test_requested_shutdown_closes_database_after_current_work(self):
        db_path = str(Path(self.tmp.name) / "shutdown.sqlite3")
        shutdown_store = AgentStore(db_path)
        shutdown_store.queue_events([event_from_isapi(raw(10))])
        agent = AttendanceAgent(config(db_path), shutdown_store, FakeDevice([]), FakeCloud())
        agent.request_stop()
        agent.run()
        reopened = AgentStore(db_path)
        self.assertEqual(reopened.discovery_cursor, 10)
        self.assertEqual(reopened.queue_counts()["pending"], 1)
        reopened.close()

    def test_paginated_polling(self):
        agent = self.make_agent([([raw(n) for n in range(1, 11)], True), ([raw(n) for n in range(11, 16)], False)])
        self.assertEqual(agent.poll_device()["pages"], 2)  # probe at 0, then newest page at 5
        self.assertEqual(agent.device.positions, [0, 5])
        self.assertEqual(self.store.queue_counts()["pending"], 15)

    def test_success_and_unmapped_are_successful_delivery(self):
        cloud = FakeCloud({"created": 0, "duplicates": 0, "unmapped": 1, "errors": 0})
        agent = self.make_agent([([raw(1)], False)], cloud)
        agent.poll_device()
        self.assertEqual(agent.upload_pending()["delivered"], 1)
        self.assertEqual(self.store.queue_counts()["pending"], 0)

    def test_partial_cloud_error_keeps_only_rejected_event(self):
        cloud = FakeCloud({"errors": 1, "error_details": [{"index": 1, "error": "invalid timestamp"}]})
        agent = self.make_agent([([raw(1), raw(2)], False)], cloud)
        agent.poll_device(); result = agent.upload_pending()
        self.assertEqual((result["delivered"], result["rejected"]), (1, 1))
        self.assertEqual(self.store.queue_counts()["rejected"], 1)

    def test_offline_cloud_retains_queue_for_retry(self):
        agent = self.make_agent([([raw(1)], False)], FakeCloud(error=CloudError("offline")))
        agent.poll_device()
        self.assertEqual(agent.upload_pending()["retried"], 1)
        self.assertEqual(self.store.queue_counts()["pending"], 1)

    def test_auth_failure_is_not_silently_lost(self):
        agent = self.make_agent([([raw(1)], False)], FakeCloud(error=CloudAuthenticationError("bad token")))
        agent.poll_device()
        with self.assertRaises(CloudAuthenticationError): agent.upload_pending()
        self.assertEqual(self.store.queue_counts()["pending"], 1)

    def test_device_failure_does_not_change_queue(self):
        class OfflineDevice:
            def search_events_page(self, position): raise DeviceError("offline")
        agent = AttendanceAgent(config(str(Path(self.tmp.name) / "queue.sqlite3")), self.store, OfflineDevice(), FakeCloud())
        with self.assertRaises(DeviceError): agent.poll_device()
        self.assertEqual(self.store.queue_counts()["pending"], 0)


DEVICE_TZ = timezone(timedelta(hours=8))  # terminal currently reports +08:00; must be preserved as-is


def device_event(serial, when, employee="2"):
    """Mirrors a real DS-K1T8003MF InfoList entry (major 5 / minor 38 fingerprint pass)."""
    return {"major": 5, "minor": 38, "time": when.astimezone(DEVICE_TZ).isoformat(timespec="seconds"),
            "employeeNoString": employee, "serialNo": serial, "userType": "normal",
            "attendanceStatus": "undefined", "currentVerifyMode": "fingerPrint"}


def history(old_count, recent_hours_ago, start_serial=1):
    """old_count events from 2022 followed by one event per entry in recent_hours_ago (oldest first)."""
    now = datetime.now(timezone.utc)
    old_start = datetime(2022, 5, 11, 5, 0, 49, tzinfo=timezone.utc)
    events = [device_event(start_serial + i, old_start + timedelta(minutes=i)) for i in range(old_count)]
    serial = start_serial + old_count
    for hours_ago in sorted(recent_hours_ago, reverse=True):
        events.append(device_event(serial, now - timedelta(hours=hours_ago)))
        serial += 1
    return events


class InitialSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "queue.sqlite3")
        self.store = AgentStore(self.db_path)

    def tearDown(self):
        self.store.close(); self.tmp.cleanup()

    def make_agent(self, device, **overrides):
        return AttendanceAgent(replace(config(self.db_path), **overrides), self.store, device, FakeCloud())

    def test_initial_sync_does_not_import_46k_historical_events(self):
        device = FakeDevice(events=history(46_323, [20, 10, 5, 2, 1]))
        self.assertEqual(len(device.log), 46_328)
        result = self.make_agent(device).poll_device()
        self.assertEqual(result["queued"], 5)
        self.assertEqual(self.store.queue_counts()["pending"], 5)
        self.assertEqual(result["skipped_history"], 46_323)
        self.assertEqual(device.positions, [0, 46_318])  # probe, then only the newest page
        self.assertEqual(self.store.discovery_cursor, 46_328)
        self.assertEqual(self.store.get_state(BOOTSTRAP_STATE_KEY), "1")

    def test_recent_bootstrap_window_respects_lookback_hours(self):
        device = FakeDevice(events=history(30, [30, 25, 23, 12, 0.5]))
        self.make_agent(device, initial_sync_lookback_hours=24).poll_device()
        queued = self.store.pending_events(100)
        self.assertEqual([e["serial_no"] for e in queued], [33, 34, 35])  # 23h, 12h and 30min ago
        self.assertEqual(self.store.discovery_cursor, 35)

    def test_recent_window_spanning_several_pages_walks_backwards(self):
        device = FakeDevice(events=history(40, [23 - i * 0.5 for i in range(25)]))
        self.make_agent(device).poll_device()
        self.assertEqual(self.store.queue_counts()["pending"], 25)
        self.assertEqual(device.positions, [0, 55, 45, 35])

    def test_zero_lookback_skips_all_history_but_sets_cursor(self):
        device = FakeDevice(events=history(50, [1]))
        result = self.make_agent(device, initial_sync_lookback_hours=0).poll_device()
        self.assertEqual(result["queued"], 0)
        self.assertEqual(self.store.discovery_cursor, 51)

    def test_initial_sync_cap_limits_events_when_device_clock_is_wrong(self):
        future = datetime.now(timezone.utc) + timedelta(days=365)
        device = FakeDevice(events=[device_event(n, future + timedelta(minutes=n)) for n in range(1, 101)])
        result = self.make_agent(device, initial_sync_max_events=25).poll_device()
        self.assertEqual(result["queued"], 25)
        self.assertEqual([e["serial_no"] for e in self.store.pending_events(100)], list(range(76, 101)))
        self.assertEqual(self.store.discovery_cursor, 100)

    def test_empty_device_completes_bootstrap(self):
        device = FakeDevice(events=[])
        result = self.make_agent(device).poll_device()
        self.assertEqual((result["queued"], result["pages"]), (0, 1))
        self.assertEqual(self.store.get_state(BOOTSTRAP_STATE_KEY), "1")
        self.make_agent(device).poll_device()
        self.assertEqual(device.positions, [0, 0])

    def test_durable_cursor_survives_restart_and_only_new_events_follow(self):
        device = FakeDevice(events=history(500, [2, 1]))
        self.make_agent(device).poll_device()
        self.store.close()
        self.store = AgentStore(self.db_path)
        self.assertEqual(self.store.discovery_cursor, 502)
        device.log += history(0, [0.1, 0.05], start_serial=503)
        device.positions.clear()
        result = self.make_agent(device).poll_device()
        self.assertNotIn("bootstrap", result)
        self.assertEqual(result["queued"], 2)
        self.assertEqual(device.positions, [0, 494])
        self.assertEqual(self.store.discovery_cursor, 504)
        self.assertEqual(self.store.queue_counts()["pending"], 4)

    def test_repolling_same_events_is_duplicate_safe(self):
        device = FakeDevice(events=history(20, [3, 2, 1]))
        agent = self.make_agent(device)
        agent.poll_device()
        second = agent.poll_device()
        self.assertEqual(second["queued"], 0)
        self.assertEqual(self.store.queue_counts()["pending"], 3)
        inserted, duplicates = self.store.commit_discovery(self.store.pending_events(10), cursor=None)
        self.assertEqual((inserted, duplicates), (0, 3))

    def test_device_timestamp_offset_is_preserved(self):
        device = FakeDevice(events=history(0, [1]))
        self.make_agent(device).poll_device()
        stored = self.store.pending_events(1)[0]
        self.assertTrue(stored["event_time"].endswith("+08:00"))
        self.assertEqual(stored["event_time"], device.log[0]["time"])
        self.assertEqual(stored["raw_payload"], device.log[0])

    def test_large_backlog_keeps_cursor_until_gap_is_closed(self):
        self.store.set_state(BOOTSTRAP_STATE_KEY, "1")
        self.store.commit_discovery([], cursor=5)
        device = FakeDevice(events=history(55, []))  # serials 1..55; 50 new after cursor 5
        agent = self.make_agent(device, max_pages_per_poll=3)
        first = agent.poll_device()
        self.assertTrue(first["backlog_remaining"])
        self.assertEqual(self.store.discovery_cursor, 5)  # not advanced past the unread gap
        while agent.poll_device()["backlog_remaining"]:
            pass
        self.assertEqual([e["serial_no"] for e in self.store.pending_events(100)], list(range(6, 56)))
        self.assertEqual(self.store.discovery_cursor, 55)
        self.assertIsNone(self.store.get_state("event_backfill_position"))

    def test_full_history_import_is_opt_in(self):
        device = FakeDevice(events=history(35, []))
        agent = self.make_agent(device, initial_sync_full_history=True, max_pages_per_poll=2)
        self.assertEqual(agent.poll_device()["bootstrap"], "full_history")
        while agent.poll_device()["backlog_remaining"]:
            pass
        self.assertEqual(self.store.queue_counts()["pending"], 35)
        self.assertEqual(self.store.discovery_cursor, 35)

    def test_full_history_is_disabled_by_default(self):
        self.assertFalse(config(self.db_path).initial_sync_full_history)
        with patch.dict("os.environ", {
            "HIKVISION_HOST": "h", "HIKVISION_USERNAME": "u", "HIKVISION_PASSWORD": "p",
            "EPCA_API_URL": "https://epca.example/api/", "EPCA_DEVICE_CODE": "c", "EPCA_DEVICE_TOKEN": "t",
        }, clear=True):
            configured = AgentConfig.from_environment()
        self.assertEqual((configured.initial_sync_lookback_hours, configured.initial_sync_full_history,
                          configured.event_page_size, configured.event_major, configured.event_minor),
                         (24, False, 10, 5, 38))

    def test_event_fields_are_extracted_from_device_entry(self):
        event = event_from_isapi({"major": 5, "minor": 38, "time": "2022-05-11T13:00:49+08:00",
                                  "employeeNoString": "2", "serialNo": 32, "userType": "normal",
                                  "attendanceStatus": "undefined"})
        self.assertEqual((event["serial_no"], event["employee_no"], event["event_time"]),
                         (32, "2", "2022-05-11T13:00:49+08:00"))
        self.assertEqual((event["major"], event["minor"], event["attendance_status"]), (5, 38, "undefined"))
        self.assertEqual(event["raw_payload"]["userType"], "normal")


class ScriptedStop(threading.Event):
    """Replaces agent.stop_requested: records every wait instead of sleeping, stops after N cycles.

    It also owns a fake monotonic clock that each wait advances, so scheduling is exact and instant.
    """
    def __init__(self, cycles):
        super().__init__()
        self.cycles, self.waits, self.now = cycles, [], 1000.0
    def clock(self): return self.now
    def wait(self, timeout=None):
        self.waits.append(timeout)
        self.now += timeout or 0
        if len(self.waits) >= self.cycles:
            self.set()
        return self.is_set()


class FlakyDevice(FakeDevice):
    """FakeDevice whose search_events_page raises the scripted exceptions (None = succeed) per poll cycle."""
    def __init__(self, failures, **kwargs):
        super().__init__(**kwargs)
        self.failures, self.cycle_calls = list(failures), 0
    def search_events_page(self, position):
        if position == 0:  # each poll cycle starts with the position-0 probe
            outcome = self.failures.pop(0) if self.failures else None
            if outcome is not None:
                raise outcome
        return super().search_events_page(position)


class FakeHttpResponse:
    def __init__(self, status, payload=None):
        self.status_code, self.payload, self.content, self.headers = status, payload, b"", {}
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")
    def json(self): return self.payload
    def close(self): pass


class FakeHttpSession:
    """Stands in for requests.Session: serves the event log over the proven AcsEvent contract."""
    instances = []
    def __init__(self, log, script):
        self.log, self.script, self.auth, self.bodies = log, script, None, []
        FakeHttpSession.instances.append(self)
    def request(self, method, url, **kwargs):
        body = json.loads(kwargs["data"])["AcsEventCond"]
        self.bodies.append(body)
        if self.script:
            status = self.script.pop(0)
            if status != 200:
                return FakeHttpResponse(status)
        position = body["searchResultPosition"]
        page = self.log[position:position + body["maxResults"]]
        return FakeHttpResponse(200, {"AcsEvent": {"searchID": "1", "numOfMatches": len(page),
                                                   "totalMatches": len(self.log), "InfoList": page,
                                                   "responseStatusStrg": "OK"}})
    def close(self): pass


class RunLoopTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "queue.sqlite3")
        self.store = AgentStore(self.db_path)
        self.store.set_state(BOOTSTRAP_STATE_KEY, "1")

    def tearDown(self):
        self.store.close(); self.tmp.cleanup()

    def run_agent(self, device, cycles, cloud=None, **overrides):
        settings = {"poll_interval_seconds": 5} | overrides
        agent = AttendanceAgent(replace(config(self.db_path), **settings), self.store, device, cloud or FakeCloud())
        agent.stop_requested = ScriptedStop(cycles)
        agent.clock = agent.stop_requested.clock
        agent.run()  # must return normally, never raise
        self.store = AgentStore(self.db_path)  # run() closes the store on exit
        return agent

    def test_repeated_successful_polling_uses_configured_interval(self):
        device = FakeDevice(events=[raw(n) for n in range(1, 4)])
        agent = self.run_agent(device, cycles=4)
        self.assertEqual(agent.stop_requested.waits, [5, 5, 5, 5])
        self.assertEqual(device.positions.count(0), 4)  # one probe per cycle

    def test_poll_interval_is_not_hard_coded(self):
        agent = self.run_agent(FakeDevice(events=[]), cycles=2, poll_interval_seconds=17)
        self.assertEqual(agent.stop_requested.waits, [17, 17])

    def test_timeout_and_network_errors_do_not_terminate_run(self):
        device = FlakyDevice([DeviceError("Device request failed: Timeout"),
                              DeviceError("Device request failed: ConnectionError")], events=[raw(1)])
        agent = self.run_agent(device, cycles=4)
        self.assertEqual(agent.stop_requested.waits, [5, 10, 5, 5])  # backoff, then normal interval
        self.assertEqual(self.store.queue_counts()["delivered"], 1)

    def test_repeated_failures_use_bounded_exponential_backoff(self):
        device = FlakyDevice([DeviceError("HTTP 401")] * 20, events=[])
        agent = self.run_agent(device, cycles=9, retry_initial_seconds=5, retry_max_seconds=300)
        self.assertEqual(agent.stop_requested.waits, [5, 10, 20, 40, 80, 160, 300, 300, 300])

    def test_recovery_returns_to_normal_interval_and_logs_it(self):
        device = FlakyDevice([DeviceError("x"), DeviceError("x"), DeviceError("x")], events=[])
        with self.assertLogs("epca_attendance_agent", level="INFO") as logs:
            agent = self.run_agent(device, cycles=6, poll_interval_seconds=7)
        self.assertEqual(agent.stop_requested.waits, [5, 10, 20, 7, 7, 7])
        self.assertTrue(any("recovered after 3 failed attempt" in line for line in logs.output))

    def test_unexpected_error_does_not_terminate_run(self):
        device = FlakyDevice([RuntimeError("boom"), None], events=[])
        agent = self.run_agent(device, cycles=3)
        self.assertEqual(agent.stop_requested.waits, [5, 5, 5])

    def test_cloud_outage_keeps_events_and_polling_continues(self):
        cloud = FakeCloud(error=CloudError("Cloud request failed: ConnectionError"))
        agent = self.run_agent(FakeDevice(events=[raw(1), raw(2)]), cycles=3, cloud=cloud)
        self.assertEqual(agent.stop_requested.waits, [5, 5, 5])  # upload failure is retried in-queue
        self.assertEqual(self.store.queue_counts()["pending"], 2)

    def test_no_duplicate_events_after_failure_and_retry(self):
        device = FlakyDevice([None, DeviceError("HTTP 401"), None], events=[raw(1), raw(2)])
        cloud = FakeCloud({"created": 2})
        self.run_agent(device, cycles=4, cloud=cloud)
        sent = [event["serial_no"] for batch in cloud.sent for event in batch]
        self.assertEqual(sent, [1, 2])  # each attendance event uploaded exactly once
        self.assertEqual(self.store.queue_counts(), {"pending": 0, "delivered": 2, "rejected": 0})

    def test_single_401_recovered_by_fresh_session_uses_normal_interval(self):
        FakeHttpSession.instances = []
        log = [raw(1), raw(2)]
        scripts = iter([[200, 401], [200]])  # 2nd request of the 1st session is a stale-nonce 401
        client = HikvisionClient(replace(config(self.db_path), poll_interval_seconds=5),
                                 session_factory=lambda: FakeHttpSession(log, next(scripts)))
        cloud = FakeCloud({"created": 2})
        agent = self.run_agent(client, cycles=3, cloud=cloud)
        self.assertEqual(agent.stop_requested.waits, [5, 5, 5])  # no failure backoff at all
        self.assertEqual(len(FakeHttpSession.instances), 2)
        for session in FakeHttpSession.instances:
            self.assertIsInstance(session.auth, HTTPDigestAuth)
        self.assertEqual([e["serial_no"] for batch in cloud.sent for e in batch], [1, 2])
        # the retried request is byte-for-byte the proven AcsEventCond
        self.assertEqual(FakeHttpSession.instances[1].bodies[0],
                         {"searchID": "1", "searchResultPosition": 0, "maxResults": 10, "major": 5, "minor": 38})

    def test_run_logs_contain_no_credentials(self):
        FakeHttpSession.instances = []
        scripts = iter([[401], [401], [401], [401]])
        client = HikvisionClient(config(self.db_path), session_factory=lambda: FakeHttpSession([], next(scripts)))
        with self.assertLogs("epca_attendance_agent", level="DEBUG") as logs:
            self.run_agent(client, cycles=2)
        output = "\n".join(logs.output)
        self.assertIn("HTTP 401", output)
        for secret in ("secret", "token", "Authorization", "Digest", "nonce", "response="):
            self.assertNotIn(secret, output)

    def test_poll_interval_validation(self):
        base = {"HIKVISION_HOST": "h", "HIKVISION_USERNAME": "u", "HIKVISION_PASSWORD": "p",
                "EPCA_API_URL": "https://epca.example/api/", "EPCA_DEVICE_CODE": "c", "EPCA_DEVICE_TOKEN": "t"}
        with patch.dict("os.environ", base | {"POLL_INTERVAL_SECONDS": "5"}, clear=True):
            self.assertEqual(AgentConfig.from_environment().poll_interval_seconds, 5)
        for bad in ("0", "-5", "abc", "86401"):
            with patch.dict("os.environ", base | {"POLL_INTERVAL_SECONDS": bad}, clear=True):
                with self.assertRaises(ConfigurationError):
                    AgentConfig.from_environment()
        with patch.dict("os.environ", base | {"RETRY_INITIAL_SECONDS": "60", "RETRY_MAX_SECONDS": "10"}, clear=True):
            with self.assertRaises(ConfigurationError):
                AgentConfig.from_environment()
