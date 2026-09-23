#!/usr/bin/env python3
"""Command line entrypoint for the EPCA ONE Proxmox attendance agent."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from typing import Any

if __package__ in {None, ""}:  # supports the documented `python agent.py ...` command
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:  # Package imports work inside EPCA ONE until this directory is extracted.
    from attendance_agent.cloud import CloudAuthenticationError, CloudError, EpcClient
    from attendance_agent.config import AgentConfig, ConfigurationError
    from attendance_agent.hikvision import DeviceError, HikvisionClient
    from attendance_agent.storage import AgentStore
except ModuleNotFoundError:  # Standalone repository: modules live beside agent.py.
    from cloud import CloudAuthenticationError, CloudError, EpcClient
    from config import AgentConfig, ConfigurationError
    from hikvision import DeviceError, HikvisionClient
    from storage import AgentStore


LOG = logging.getLogger("epca_attendance_agent")


def event_from_isapi(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Keep only source metadata EPCA ONE accepts; do not derive attendance."""
    try:
        serial = int(raw["serialNo"])
        employee_no = str(raw["employeeNoString"]).strip()
        event_time = str(raw["time"]).strip()
    except (KeyError, TypeError, ValueError):
        return None
    if not employee_no or not event_time or serial < 0:
        return None
    return {
        "serial_no": serial, "employee_no": employee_no, "event_time": event_time,
        "major": raw.get("major"), "minor": raw.get("minor"),
        "attendance_status": str(raw.get("attendanceStatus", "")),
        "verification_method": str(raw.get("currentVerifyMode", "")), "raw_payload": raw,
    }


class AttendanceAgent:
    def __init__(self, config: AgentConfig, store: AgentStore | None = None,
                 device: HikvisionClient | None = None, cloud: EpcClient | None = None):
        self.config = config
        self.store = store or AgentStore(config.database_path)
        self.device = device or HikvisionClient(config)
        self.cloud = cloud or EpcClient(config)
        self.stop_requested = threading.Event()

    def request_stop(self) -> None:
        """Stop after the current request/SQLite transaction completes."""
        if not self.stop_requested.is_set():
            LOG.info("Shutdown requested; finishing the current operation safely.")
            self.stop_requested.set()

    def test_device(self) -> dict[str, Any]:
        info = self.device.device_info()
        LOG.info("Device connectivity succeeded.")
        return info

    def test_cloud(self) -> None:
        self.cloud.test_connection()
        LOG.info("Cloud connectivity and device credential validation succeeded.")

    def poll_device(self) -> dict[str, int]:
        cursor = self.store.discovery_cursor
        inserted = duplicates = invalid = pages = 0
        position = 0
        while pages < self.config.max_pages_per_poll:
            raw_events, more = self.device.search_events(position)
            pages += 1
            events = []
            for raw in raw_events:
                event = event_from_isapi(raw)
                if event is None:
                    invalid += 1
                elif event["serial_no"] > cursor:
                    events.append(event)
            if events:
                added, known = self.store.queue_events(events)
                inserted += added
                duplicates += known
            if not more or not raw_events:
                break
            position += len(raw_events)
        else:
            LOG.warning("Stopped discovery at configured page limit; the next poll will continue safely.")
        self.store.set_state("last_successful_device_poll", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        return {"pages": pages, "queued": inserted, "duplicates": duplicates, "invalid": invalid}

    def upload_pending(self) -> dict[str, int]:
        delivered = rejected = retried = 0
        while True:
            batch = self.store.pending_events(self.config.batch_size)
            if not batch:
                break
            serials = [event["serial_no"] for event in batch]
            try:
                response = self.cloud.send_events(batch)
            except CloudAuthenticationError:
                self.store.mark_retry(serials, "Cloud authentication failed")
                self.store.set_state("last_cloud_error", "Cloud authentication failed")
                raise
            except CloudError as exc:
                self.store.mark_retry(serials, str(exc))
                self.store.set_state("last_cloud_error", str(exc))
                retried += len(batch)
                break
            bad_indexes = {item.get("index") for item in response.get("error_details", []) if isinstance(item, dict)}
            for index, event in enumerate(batch):
                if index in bad_indexes:
                    detail = next((item.get("error", "Cloud rejected event") for item in response["error_details"]
                                   if item.get("index") == index), "Cloud rejected event")
                    self.store.mark_rejected(event["serial_no"], str(detail))
                    rejected += 1
                else:
                    self.store.mark_delivered([event["serial_no"]])
                    delivered += 1
            self.store.set_state("last_cloud_error", "")
        return {"delivered": delivered, "rejected": rejected, "retried": retried}

    def sync_once(self) -> dict[str, Any]:
        result = {"discovery": self.poll_device(), "delivery": self.upload_pending()}
        result["queue"] = self.store.queue_counts()
        LOG.info("Sync complete: queued=%s delivered=%s rejected=%s", result["discovery"]["queued"],
                 result["delivery"]["delivered"], result["delivery"]["rejected"])
        return result

    def health(self) -> dict[str, Any]:
        try:
            self.device.test_connection() if hasattr(self.device, "test_connection") else self.device.device_info()
            device_status = "reachable"
        except DeviceError as exc:
            device_status = f"unavailable: {exc}"
        try:
            self.cloud.test_connection()
            cloud_status = "reachable"
        except CloudError as exc:
            cloud_status = f"unavailable: {exc}"
        return {"hikvision": device_status, "epca_one": cloud_status, "last_discovered_serial": self.store.discovery_cursor,
                "queue": self.store.queue_counts(), "last_successful_device_poll": self.store.get_state("last_successful_device_poll"),
                "last_successful_upload": self.store.get_state("last_successful_upload"),
                "last_cloud_error": self.store.get_state("last_cloud_error", "")}

    def run(self) -> None:
        failures = 0
        try:
            while not self.stop_requested.is_set():
                try:
                    self.sync_once()
                    failures = 0
                    delay = self.config.poll_interval_seconds
                except CloudAuthenticationError:
                    failures += 1
                    delay = max(3600, self.config.poll_interval_seconds)
                    LOG.error("Cloud authentication failed; retrying in %s seconds.", delay)
                except (CloudError, DeviceError) as exc:
                    failures += 1
                    delay = min(max(self.config.poll_interval_seconds, 30) * (2 ** min(failures, 6)), 3600)
                    LOG.warning("Sync failed (%s); retrying in %s seconds.", exc, delay)
                self.stop_requested.wait(delay)
        finally:
            self.store.close()
            LOG.info("Attendance agent stopped safely.")


def main() -> int:
    parser = argparse.ArgumentParser(description="EPCA ONE Hikvision attendance agent")
    parser.add_argument("command", choices=["test-device", "test-cloud", "sync-once", "health", "run", "discover-users"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        agent = AttendanceAgent(AgentConfig.from_environment())
        if args.command == "test-device":
            print(agent.test_device())
        elif args.command == "test-cloud":
            agent.test_cloud()
        elif args.command == "sync-once":
            print(agent.sync_once())
        elif args.command == "health":
            print(agent.health())
        elif args.command == "discover-users":
            users, more = agent.device.search_users()
            print({"users": users, "more": more})
        else:
            def handle_shutdown(signum, frame):
                agent.request_stop()
            signal.signal(signal.SIGTERM, handle_shutdown)
            signal.signal(signal.SIGINT, handle_shutdown)
            agent.run()
        return 0
    except (ConfigurationError, DeviceError, CloudError) as exc:
        LOG.error("%s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
