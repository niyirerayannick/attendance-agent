"""Historical backfill reliability: page retries, timeouts, page size, checkpoints and dry-run safety.

Every test drives a scripted in-memory device; no Hikvision terminal or EPCA server is contacted.
"""

import contextlib
import io
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from unittest.mock import patch

import requests
from requests.auth import HTTPDigestAuth

try:
    from attendance_agent.agent import AttendanceAgent, BackfillPageError, print_backfill_report
    from attendance_agent.cloud import CloudError
    from attendance_agent.config import AgentConfig, ConfigurationError
    from attendance_agent.hikvision import (DeviceAuthenticationError, DeviceError, DeviceTransientError,
                                            HikvisionClient)
    from attendance_agent.storage import AgentStore
except ModuleNotFoundError:
    from agent import AttendanceAgent, BackfillPageError, print_backfill_report
    from cloud import CloudError
    from config import AgentConfig, ConfigurationError
    from hikvision import DeviceAuthenticationError, DeviceError, DeviceTransientError, HikvisionClient
    from storage import AgentStore


def config(path, **overrides):
    base = AgentConfig("192.168.1.10", "reader", "secret", "https://epca.example/api/internal/attendance/events/",
                       "HQ-01", "token", 60, path)
    return replace(base, **overrides)


def event(serial, when="2026-09-20T08:00:00+02:00", employee="E-001"):
    return {"serialNo": serial, "employeeNoString": employee, "time": when, "major": 5, "minor": 38,
            "attendanceStatus": "undefined", "currentVerifyMode": "fingerPrint"}


OK = "ok"


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status_code, self.payload, self.content, self.headers = status, payload, b"", {}
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))
    def json(self): return self.payload
    def close(self): pass


class ScriptedDevice:
    """Session factory for HikvisionClient serving an oldest-first AcsEvent log.

    ``script`` maps a searchResultPosition to the outcomes of successive requests at that position:
    an exception instance is raised, an int is returned as that HTTP status, OK serves the page.
    """
    def __init__(self, log, script=None):
        self.log, self.script = log, {k: list(v) for k, v in (script or {}).items()}
        self.sessions, self.requests = [], []  # requests: (position, maxResults, timeout, AcsEventCond)

    def session(self):
        device = self

        class Session:
            def __init__(self):
                self.auth, self.closed = None, False
            def request(self, method, url, **kwargs):
                body = json.loads(kwargs["data"])["AcsEventCond"]
                position = body["searchResultPosition"]
                device.requests.append((position, body["maxResults"], kwargs.get("timeout"), body))
                outcomes = device.script.get(position)
                outcome = outcomes.pop(0) if outcomes else OK
                if isinstance(outcome, BaseException):
                    raise outcome
                if outcome != OK:
                    return FakeResponse(outcome)
                page = device.log[position:position + body["maxResults"]]
                return FakeResponse(200, {"AcsEvent": {"searchID": "1", "numOfMatches": len(page),
                                                       "totalMatches": len(device.log), "InfoList": page,
                                                       "responseStatusStrg": "MORE"}})
            def close(self): self.closed = True

        created = Session()
        self.sessions.append(created)
        return created

    @property
    def positions(self):
        return [position for position, *_ in self.requests]


class RecordingStop(threading.Event):
    """Replaces agent.stop_requested: records backoff waits instead of sleeping."""
    def __init__(self, on_wait=None, stop_on_wait=False):
        super().__init__()
        self.waits, self.on_wait, self.stop_on_wait = [], on_wait, stop_on_wait
    def wait(self, timeout=None):
        self.waits.append(timeout)
        if self.on_wait:
            self.on_wait()
        if self.stop_on_wait:
            self.set()
        return self.is_set()


class FakeCloud:
    def __init__(self, response=None, errors=None):
        self.response, self.errors, self.sent = response, list(errors or []), []
    def test_connection(self): return None
    def send_events(self, events):
        self.sent.append([e["serial_no"] for e in events])
        if self.errors:
            error = self.errors.pop(0)
            if error is not None:
                raise error
        return self.response or {"created": len(events), "duplicates": 0}


class BackfillRetryTests(unittest.TestCase):
    RANGE = (date(2026, 9, 20), date(2026, 9, 20))

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "queue.sqlite3")
        self.store = AgentStore(self.db_path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def agent(self, device, cloud=None, stop=None, **overrides):
        client = HikvisionClient(config(self.db_path, **overrides), session_factory=device.session)
        agent = AttendanceAgent(config(self.db_path, **overrides), self.store, client, cloud or FakeCloud())
        agent.stop_requested = stop or RecordingStop()
        return agent

    def job(self):
        row = self.store.connection.execute("SELECT * FROM backfill_jobs WHERE job_id='2026-09-20:2026-09-20'").fetchone()
        return dict(row) if row else None

    def test_read_timeout_then_successful_retry(self):
        device = ScriptedDevice([event(n) for n in range(1, 16)], {0: [requests.ReadTimeout("read timed out")]})
        agent = self.agent(device)
        result = agent.backfill(*self.RANGE, dry_run=True)
        self.assertTrue(result["complete"])
        self.assertEqual((result["scanned"], result["matched"]), (15, 15))
        self.assertEqual(device.positions, [0, 0, 10])
        self.assertEqual(agent.stop_requested.waits, [2])
        self.assertEqual((result["retries"], result["transient_errors"], result["pages"]), (1, 1, 2))

    def test_connect_timeout_then_successful_retry_resumes_same_page_in_dry_run(self):
        device = ScriptedDevice([event(n) for n in range(1, 26)],
                                {10: [requests.ConnectTimeout("connect timed out"), requests.ReadTimeout("slow")]})
        agent = self.agent(device)
        result = agent.backfill(*self.RANGE, dry_run=True)
        self.assertTrue(result["complete"])
        # The failed page is retried at position 10; the dry run never restarts from position 0.
        self.assertEqual(device.positions, [0, 10, 10, 10, 20])
        self.assertEqual(agent.stop_requested.waits, [2, 5])
        self.assertEqual((result["scanned"], result["matched"]), (25, 25))  # nothing counted twice

    def test_connection_reset_is_retried(self):
        device = ScriptedDevice([event(1)], {0: [requests.ConnectionError("connection reset by peer")]})
        result = self.agent(device).backfill(*self.RANGE, dry_run=True)
        self.assertTrue(result["complete"])
        self.assertEqual(device.positions, [0, 0])

    def test_every_retry_uses_a_fresh_digest_session(self):
        device = ScriptedDevice([event(1)], {0: [requests.ReadTimeout(), requests.ReadTimeout()]})
        self.agent(device).backfill(*self.RANGE, dry_run=True)
        self.assertEqual(len(device.sessions), 3)  # one new Session per attempt
        self.assertEqual(len({id(s.auth) for s in device.sessions}), 3)
        self.assertTrue(all(isinstance(s.auth, HTTPDigestAuth) for s in device.sessions))
        self.assertTrue(all(s.closed for s in device.sessions[:-1]))  # replaced sessions are closed

    def test_retries_exhausted_fail_clearly_with_bounded_backoff(self):
        device = ScriptedDevice([event(n) for n in range(1, 26)], {10: [requests.ReadTimeout()] * 10})
        agent = self.agent(device)
        with self.assertRaises(BackfillPageError) as caught:
            agent.backfill(*self.RANGE, dry_run=True)
        message = str(caught.exception)
        self.assertIn("searchResultPosition=10", message)
        self.assertIn("5 attempt(s)", message)
        self.assertIn("ReadTimeout", message)
        self.assertIn("nothing was checkpointed", message)
        self.assertEqual(agent.stop_requested.waits, [2, 5, 10, 20])
        self.assertEqual(device.positions, [0] + [10] * 5)
        partial = caught.exception.result
        self.assertEqual((partial["next_position"], partial["scanned"], partial["retries"],
                          partial["transient_errors"], partial["complete"]), (10, 10, 4, 5, False))
        self.assertIsInstance(caught.exception, DeviceError)  # the CLI exits 2 like other device errors

    def test_max_retries_is_configurable(self):
        device = ScriptedDevice([event(1)], {0: [requests.ReadTimeout()] * 10})
        agent = self.agent(device, backfill_max_retries=6)
        with self.assertRaises(BackfillPageError):
            agent.backfill(*self.RANGE, dry_run=True)
        self.assertEqual(agent.stop_requested.waits, [2, 5, 10, 20, 20, 20])  # last delay repeats
        agent = self.agent(ScriptedDevice([event(1)], {0: [requests.ReadTimeout()]}), backfill_max_retries=0)
        with self.assertRaises(BackfillPageError):
            agent.backfill(*self.RANGE, dry_run=True)
        self.assertEqual(agent.stop_requested.waits, [])

    def test_checkpoint_does_not_advance_while_a_page_is_failing(self):
        device = ScriptedDevice([event(n) for n in range(1, 26)], {10: [requests.ReadTimeout()] * 10})
        seen = []
        stop = RecordingStop(on_wait=lambda: seen.append(self.job()["next_position"]))
        cloud = FakeCloud()
        with self.assertRaises(BackfillPageError) as caught:
            self.agent(device, cloud, stop).backfill(*self.RANGE)
        self.assertEqual(seen, [10, 10, 10, 10])  # first page acknowledged, failing page never checkpointed
        self.assertEqual(self.job()["next_position"], 10)
        self.assertIsNone(self.job()["completed_at"])
        self.assertEqual(cloud.sent, [list(range(1, 11))])
        self.assertIn("checkpoint remains at position 10", str(caught.exception))

    def test_real_backfill_resumes_from_checkpoint_after_exhausted_retries(self):
        log = [event(n) for n in range(1, 26)]
        first_cloud = FakeCloud()
        with self.assertRaises(BackfillPageError):
            self.agent(ScriptedDevice(log, {10: [requests.ReadTimeout()] * 5}), first_cloud).backfill(*self.RANGE)
        device, cloud = ScriptedDevice(log), FakeCloud()
        result = self.agent(device, cloud).backfill(*self.RANGE)
        self.assertTrue(result["complete"])
        self.assertEqual(device.positions, [10, 20])  # resumed, did not rescan position 0
        self.assertEqual(cloud.sent, [list(range(11, 21)), list(range(21, 26))])
        self.assertEqual((result["scanned"], result["matched"], result["delivered"]), (25, 25, 25))
        self.assertEqual(self.job()["next_position"], 25)
        self.assertIsNotNone(self.job()["completed_at"])

    def test_successful_next_page_advances_checkpoint(self):
        positions = []
        device = ScriptedDevice([event(n) for n in range(1, 26)], {10: [requests.ReadTimeout()]})
        stop = RecordingStop(on_wait=lambda: positions.append(self.job()["next_position"]))
        result = self.agent(device, stop=stop).backfill(*self.RANGE)
        self.assertEqual(positions, [10])  # while page 2 was being retried
        self.assertEqual((result["next_position"], self.job()["next_position"]), (25, 25))

    def test_retried_page_is_sent_once_and_duplicates_stay_idempotent(self):
        log = [event(n) for n in range(1, 21)]
        # Page 2 times out once; then its cloud request fails, so the rerun replays it.
        device = ScriptedDevice(log, {10: [requests.ReadTimeout()]})
        cloud = FakeCloud(errors=[None, CloudError("Cloud request failed: ReadTimeout")])
        with self.assertRaises(CloudError):
            self.agent(device, cloud).backfill(*self.RANGE)
        self.assertEqual(cloud.sent, [list(range(1, 11)), list(range(11, 21))])  # the retry did not double-send
        self.assertEqual(self.job()["next_position"], 10)
        # EPCA had actually stored page 2: the replay is reported as already existing, never delivered twice.
        replay = FakeCloud(response={"created": 0, "duplicates": 10})
        result = self.agent(ScriptedDevice(log), replay).backfill(*self.RANGE)
        self.assertEqual(replay.sent, [list(range(11, 21))])
        self.assertEqual((result["delivered"], result["already_existing"], result["complete"]), (10, 10, True))

    def test_dry_run_writes_no_checkpoint_or_state(self):
        self.store.commit_discovery([], cursor=900)
        before = dict(self.store.connection.execute("SELECT key, value FROM agent_state").fetchall())
        cloud = FakeCloud()
        device = ScriptedDevice([event(n) for n in range(1, 26)], {10: [requests.ReadTimeout()]})
        self.agent(device, cloud).backfill(*self.RANGE, dry_run=True)
        with self.assertRaises(BackfillPageError):
            self.agent(ScriptedDevice([event(1), event(2)] * 10, {10: [requests.ReadTimeout()] * 5}),
                       cloud).backfill(*self.RANGE, dry_run=True)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM backfill_jobs").fetchone()[0], 0)
        self.assertEqual(dict(self.store.connection.execute("SELECT key, value FROM agent_state").fetchall()), before)
        self.assertEqual(self.store.queue_counts(), {"pending": 0, "delivered": 0, "rejected": 0})
        self.assertEqual(self.store.discovery_cursor, 900)
        self.assertEqual(cloud.sent, [])

    def test_dry_run_ignores_and_preserves_a_real_checkpoint(self):
        log = [event(n) for n in range(1, 26)]
        with self.assertRaises(BackfillPageError):
            self.agent(ScriptedDevice(log, {10: [requests.ReadTimeout()] * 5})).backfill(*self.RANGE)
        device = ScriptedDevice(log)
        result = self.agent(device).backfill(*self.RANGE, dry_run=True)
        self.assertEqual((device.positions[0], result["matched"]), (0, 25))
        self.assertEqual(self.job()["next_position"], 10)

    def test_authentication_cooldown_is_never_retried(self):
        device = ScriptedDevice([event(1)], {0: [401]})
        agent = self.agent(device)
        with self.assertRaises(DeviceAuthenticationError):
            agent.backfill(*self.RANGE, dry_run=True)
        self.assertEqual((device.positions, agent.stop_requested.waits), ([0], []))

    def test_http_errors_are_not_retried(self):
        device = ScriptedDevice([event(1)], {0: [500]})
        agent = self.agent(device)
        with self.assertRaises(DeviceError) as caught:
            agent.backfill(*self.RANGE, dry_run=True)
        self.assertNotIsInstance(caught.exception, BackfillPageError)
        self.assertEqual((device.positions, agent.stop_requested.waits), ([0], []))

    def test_stop_during_backoff_ends_cleanly_without_advancing(self):
        device = ScriptedDevice([event(n) for n in range(1, 26)], {10: [requests.ReadTimeout()] * 5})
        result = self.agent(device, stop=RecordingStop(stop_on_wait=True)).backfill(*self.RANGE)
        self.assertFalse(result["complete"])
        self.assertEqual((result["next_position"], self.job()["next_position"]), (10, 10))
        self.assertEqual(device.positions, [0, 10])

    def test_retry_logs_contain_position_and_no_credentials(self):
        device = ScriptedDevice([event(1)], {0: [requests.ReadTimeout()]})
        with self.assertLogs("epca_attendance_agent", level="DEBUG") as logs:
            self.agent(device).backfill(*self.RANGE, dry_run=True)
        output = "\n".join(logs.output)
        self.assertIn("Backfill page 1 (searchResultPosition=0) attempt 1/5 failed", output)
        self.assertIn("retrying the same position in 2 seconds", output)
        self.assertIn("Backfill progress: scanned=1 matched=1", output)
        self.assertIn("last_event_time=2026-09-20T08:00:00+02:00", output)
        for secret in ("secret", "token", "Authorization", "Digest", "nonce"):
            self.assertNotIn(secret, output)


class BackfillQueryTests(unittest.TestCase):
    RANGE = BackfillRetryTests.RANGE
    setUp, tearDown, agent = BackfillRetryTests.setUp, BackfillRetryTests.tearDown, BackfillRetryTests.agent

    def test_backfill_uses_the_backfill_timeouts_and_page_size(self):
        device = ScriptedDevice([event(n) for n in range(1, 13)])
        self.agent(device).backfill(*self.RANGE, dry_run=True)
        self.assertEqual({(size, timeout) for _, size, timeout, _ in device.requests}, {(10, (10, 60))})
        device = ScriptedDevice([event(n) for n in range(1, 13)])
        result = self.agent(device, backfill_page_size=5, backfill_connect_timeout_seconds=7,
                            backfill_read_timeout_seconds=120).backfill(*self.RANGE, dry_run=True)
        self.assertEqual(device.positions, [0, 5, 10])
        self.assertEqual({(size, timeout) for _, size, timeout, _ in device.requests}, {(5, (7, 120))})
        self.assertEqual(result["matched"], 12)

    def test_live_polling_keeps_its_own_timeout_and_page_size(self):
        device = ScriptedDevice([event(n) for n in range(1, 4)])
        self.agent(device, backfill_page_size=5, backfill_read_timeout_seconds=120,
                   device_timeout_seconds=15).poll_device()
        self.assertEqual({(size, timeout) for _, size, timeout, _ in device.requests}, {(10, 15)})

    def test_device_query_uses_only_proven_fields_and_no_date_filter(self):
        device = ScriptedDevice([event(1)])
        self.agent(device).backfill(*self.RANGE, dry_run=True)
        self.assertEqual(device.requests[0][3],
                         {"searchID": "1", "searchResultPosition": 0, "maxResults": 10, "major": 5, "minor": 38})

    def test_no_early_stop_after_range_and_out_of_order_times_are_counted(self):
        # A backwards clock correction: serial 5 carries an in-range date after later-dated events.
        log = [event(1, "2026-09-19T08:00:00+02:00"), event(2, "2026-09-20T08:00:00+02:00"),
               event(3, "2026-09-21T08:00:00+02:00"), event(4, "2026-09-22T08:00:00+02:00"),
               event(5, "2026-09-20T17:00:00+02:00"), event(6, "2026-09-23T08:00:00+02:00")]
        device = ScriptedDevice(log)
        result = self.agent(device, backfill_page_size=2).backfill(*self.RANGE, dry_run=True)
        self.assertEqual(device.positions, [0, 2, 4])  # kept scanning after passing --to
        self.assertEqual(result["matched"], 2)
        self.assertEqual((result["first_matched_event_time"], result["last_matched_event_time"]),
                         ("2026-09-20T08:00:00+02:00", "2026-09-20T17:00:00+02:00"))
        self.assertEqual((result["time_regressions"], result["serial_regressions"]), (1, 0))

    def test_match_semantics_use_the_unconverted_device_local_date(self):
        log = [event(1, "2026-09-19T23:59:59+02:00"), event(2, "2026-09-20T00:00:00+02:00"),
               event(3, "2026-09-20T23:59:59+02:00"), event(4, "2026-09-21T00:00:00+02:00"),
               {"serialNo": 5, "time": "2026-09-20T09:00:00+02:00"}]  # no employee: not attendance
        result = self.agent(ScriptedDevice(log)).backfill(*self.RANGE, dry_run=True)
        self.assertEqual((result["scanned"], result["matched"]), (5, 2))
        self.assertEqual((result["oldest_event_time"], result["newest_event_time"]),
                         ("2026-09-20T00:00:00+02:00", "2026-09-20T23:59:59+02:00"))


class BackfillClientTests(unittest.TestCase):
    def client(self, error):
        class Session:
            auth = None
            def request(self, *args, **kwargs): raise error
            def close(self): pass
        return HikvisionClient(config(":memory:"), session_factory=Session)

    def test_timeouts_and_resets_are_transient_device_errors(self):
        for error in (requests.ReadTimeout(), requests.ConnectTimeout(), requests.ConnectionError()):
            with self.assertRaises(DeviceTransientError) as caught:
                self.client(error).search_events_page(0)
            self.assertIn(error.__class__.__name__, str(caught.exception))

    def test_tls_and_other_request_errors_are_not_transient(self):
        for error in (requests.exceptions.SSLError(), requests.exceptions.InvalidURL()):
            with self.assertRaises(DeviceError) as caught:
                self.client(error).search_events_page(0)
            self.assertNotIsInstance(caught.exception, DeviceTransientError)


class BackfillConfigTests(unittest.TestCase):
    BASE = {"HIKVISION_HOST": "h", "HIKVISION_USERNAME": "u", "HIKVISION_PASSWORD": "p",
            "EPCA_API_URL": "https://epca.example/api/", "EPCA_DEVICE_CODE": "c", "EPCA_DEVICE_TOKEN": "t"}

    def load(self, **env):
        with patch.dict("os.environ", self.BASE | env, clear=True):
            return AgentConfig.from_environment()

    def test_defaults(self):
        cfg = self.load()
        self.assertEqual((cfg.backfill_connect_timeout_seconds, cfg.backfill_read_timeout_seconds,
                          cfg.backfill_page_size, cfg.backfill_max_retries), (10, 60, 10, 4))
        self.assertEqual((cfg.device_timeout_seconds, cfg.event_page_size), (15, 10))  # live settings unchanged

    def test_overrides(self):
        cfg = self.load(HIKVISION_BACKFILL_CONNECT_TIMEOUT="5", HIKVISION_BACKFILL_READ_TIMEOUT="120",
                        HIKVISION_BACKFILL_PAGE_SIZE="5", HIKVISION_BACKFILL_MAX_RETRIES="0")
        self.assertEqual((cfg.backfill_connect_timeout_seconds, cfg.backfill_read_timeout_seconds,
                          cfg.backfill_page_size, cfg.backfill_max_retries), (5, 120, 5, 0))

    def test_invalid_values_are_rejected(self):
        for name, value in (("HIKVISION_BACKFILL_PAGE_SIZE", "11"), ("HIKVISION_BACKFILL_PAGE_SIZE", "0"),
                            ("HIKVISION_BACKFILL_READ_TIMEOUT", "0"), ("HIKVISION_BACKFILL_CONNECT_TIMEOUT", "x"),
                            ("HIKVISION_BACKFILL_MAX_RETRIES", "-1")):
            with self.subTest(name=name, value=value), self.assertRaises(ConfigurationError):
                self.load(**{name: value})


class BackfillReportTests(unittest.TestCase):
    def render(self, result, dry_run, failure=None):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            print_backfill_report(result, dry_run, failure)
        return output.getvalue()

    def test_dry_run_report(self):
        text = self.render({"range_from": "2026-09-20", "range_to": "2026-09-20", "all_history": False,
                            "scanned": 2000, "matched": 3, "first_matched_event_time": "2026-09-20T07:01:00+02:00",
                            "last_matched_event_time": "2026-09-20T17:30:00+02:00", "pages": 200, "retries": 2,
                            "transient_errors": 2, "start_position": 0, "next_position": 2000,
                            "total_matches": 46000, "complete": False}, True, failure="page 201 failed")
        for line in ("Historical attendance backfill (dry run)", "Requested range: 2026-09-20 -> 2026-09-20",
                     "Scanned: 2000", "Matched attendance: 3", "First matched event: 2026-09-20T07:01:00+02:00",
                     "Last matched event: 2026-09-20T17:30:00+02:00", "Pages scanned (this run): 200",
                     "Timeouts/connection errors (this run): 2", "Page retries (this run): 2",
                     "Search position: 0 -> 2000 of 46000",
                     "Dry run: nothing was sent to EPCA ONE and no checkpoint was written.",
                     "Backfill FAILED: page 201 failed"):
            self.assertIn(line, text)
        self.assertNotIn("Delivered", text)

    def test_cli_prints_partial_report_and_exits_2_when_retries_are_exhausted(self):
        agent_module = __import__(AttendanceAgent.__module__, fromlist=["main"])
        device = ScriptedDevice([event(n) for n in range(1, 26)], {10: [requests.ReadTimeout()] * 5})
        with tempfile.TemporaryDirectory() as data_dir, \
                patch.dict("os.environ", BackfillConfigTests.BASE | {"AGENT_DATA_DIR": data_dir}, clear=True), \
                patch.object(agent_module, "load_project_env"), \
                patch.object(agent_module, "install_signal_handlers"), \
                patch.object(agent_module, "HikvisionClient",
                             lambda cfg: HikvisionClient(cfg, session_factory=device.session)), \
                patch.object(threading.Event, "wait", lambda self, timeout=None: False), \
                patch("sys.argv", ["agent.py", "backfill", "--from", "2026-09-20", "--to", "2026-09-20",
                                   "--dry-run"]):
            output = io.StringIO()
            with contextlib.redirect_stdout(output), self.assertLogs("epca_attendance_agent", "ERROR"):
                code = agent_module.main()
            store = AgentStore(str(Path(data_dir) / "attendance_agent.db"))
            jobs = store.connection.execute("SELECT COUNT(*) FROM backfill_jobs").fetchone()[0]
            store.close()
        text = output.getvalue()
        self.assertEqual((code, jobs), (2, 0))
        self.assertIn("Scanned: 10", text)
        self.assertIn("Timeouts/connection errors (this run): 5", text)
        self.assertIn("Backfill FAILED: Backfill page 2 (searchResultPosition=10)", text)
        self.assertIn("Dry run: nothing was sent to EPCA ONE and no checkpoint was written.", text)

    def test_real_report_keeps_delivery_counters(self):
        text = self.render({"range_from": "2026-09-20", "range_to": "2026-09-20", "delivered": 4,
                            "already_existing": 1, "complete": True}, False)
        self.assertIn("Delivered: 4", text)
        self.assertIn("Already existing: 1", text)
        self.assertIn("Backfill complete", text)
        self.assertNotIn("Dry run", text)


if __name__ == "__main__":
    unittest.main()
