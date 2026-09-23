import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from attendance_agent.agent import AttendanceAgent, event_from_isapi
    from attendance_agent.cloud import CloudAuthenticationError, CloudError
    from attendance_agent.config import AgentConfig
    from attendance_agent.hikvision import DeviceError
    from attendance_agent.storage import AgentStore
except ModuleNotFoundError:
    from agent import AttendanceAgent, event_from_isapi
    from cloud import CloudAuthenticationError, CloudError
    from config import AgentConfig
    from hikvision import DeviceError
    from storage import AgentStore


def config(path):
    return AgentConfig("192.168.1.10", "reader", "secret", "https://epca.example/api/internal/attendance/events/",
                       "HQ-01", "token", 60, path)


def raw(serial=1):
    return {"serialNo": serial, "employeeNoString": "E-001", "time": "2026-09-21T08:00:00+02:00",
            "major": 5, "minor": 75, "attendanceStatus": "checkIn", "currentVerifyMode": "fingerPrint"}


class FakeDevice:
    def __init__(self, pages): self.pages, self.calls = pages, 0
    def device_info(self): return {"deviceName": "Terminal"}
    def search_events(self, position):
        page = self.pages[self.calls] if self.calls < len(self.pages) else ([], False)
        self.calls += 1
        return page


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
        agent = self.make_agent([([raw(1)], True), ([raw(2)], False)])
        self.assertEqual(agent.poll_device()["pages"], 2)
        self.assertEqual(self.store.queue_counts()["pending"], 2)

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
            def search_events(self, position): raise DeviceError("offline")
        agent = AttendanceAgent(config(str(Path(self.tmp.name) / "queue.sqlite3")), self.store, OfflineDevice(), FakeCloud())
        with self.assertRaises(DeviceError): agent.poll_device()
        self.assertEqual(self.store.queue_counts()["pending"], 0)
