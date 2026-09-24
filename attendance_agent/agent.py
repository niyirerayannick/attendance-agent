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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:  # supports the documented `python agent.py ...` command
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:  # Package imports work inside EPCA ONE until this directory is extracted.
    from attendance_agent.cloud import CloudAuthenticationError, CloudError, EpcClient
    from attendance_agent.config import AgentConfig, ConfigurationError, load_project_env
    from attendance_agent.hikvision import DeviceError, HikvisionClient
    from attendance_agent.storage import AgentStore
except ModuleNotFoundError:  # Standalone repository: modules live beside agent.py.
    from cloud import CloudAuthenticationError, CloudError, EpcClient
    from config import AgentConfig, ConfigurationError, load_project_env
    from hikvision import DeviceError, HikvisionClient
    from storage import AgentStore


LOG = logging.getLogger("epca_attendance_agent")

# Directory of the launched agent.py, independent of the working directory (e.g. a Windows Service).
APP_DIR = Path(__file__).resolve().parent

# agent_state keys for event discovery (all durable in SQLite alongside last_discovered_serial).
BOOTSTRAP_STATE_KEY = "event_bootstrap_complete"
BACKFILL_POSITION_KEY = "event_backfill_position"
BACKFILL_HIGH_SERIAL_KEY = "event_backfill_high_serial"


@dataclass
class TailScan:
    """Result of walking the device event log from newest page towards older pages."""
    events: list[dict[str, Any]] = field(default_factory=list)
    highest_serial: int = -1
    pages: int = 0
    invalid: int = 0
    total_matches: int | None = None
    finished: bool = False  # reached the stop serial, the cutoff time, or the oldest event
    next_position: int = 0  # where an unfinished scan should resume


def _event_instant(event_time: str) -> datetime | None:
    """Parse the device timestamp for comparison only; the stored value is never rewritten."""
    try:
        parsed = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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

    def poll_device(self) -> dict[str, Any]:
        """Discover new events. An empty database first bootstraps from a recent window only."""
        bootstrapped = self.store.get_state(BOOTSTRAP_STATE_KEY) == "1" or self.store.discovery_cursor > 0
        result = self._poll_new_events() if bootstrapped else self._bootstrap_events()
        self.store.set_state("last_successful_device_poll", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        return result

    def _scan_tail(self, *, start: int | None, stop_serial: int, cutoff: datetime | None,
                   max_pages: int, max_events: int | None = None) -> TailScan:
        """Walk the event log newest -> oldest, one page at a time.

        The device returns matches oldest-first, so the newest page is at totalMatches - pageSize.
        Scanning stops at the first event with serialNo <= stop_serial, older than ``cutoff``, or at
        position 0. Every requested position is < totalMatches, so no page past the end is requested.
        """
        page_size = self.config.event_page_size
        scan = TailScan()
        found: dict[int, dict[str, Any]] = {}
        page = probe = None
        if start is None:
            page = probe = self.device.search_events_page(0)  # probe for totalMatches
            scan.pages = 1
            scan.total_matches = page["totalMatches"]
            if scan.total_matches is None:
                raise DeviceError("Device did not report AcsEvent totalMatches; refusing to scan event history.")
            position = max(0, scan.total_matches - page_size)
            if position:
                page = None  # the probe was the oldest page; fetch the newest one instead
        else:
            position = start
        while True:
            if page is None and position == 0 and probe is not None:
                page = probe  # already fetched; do not request position 0 twice
            if page is None:
                if scan.pages >= max_pages:
                    scan.next_position = position
                    break
                page = self.device.search_events_page(position)
                scan.pages += 1
            raw_events = page["events"]
            reached_stop = False
            for raw in raw_events:
                event = event_from_isapi(raw)
                if event is None:
                    scan.invalid += 1
                    continue
                scan.highest_serial = max(scan.highest_serial, event["serial_no"])
                if event["serial_no"] <= stop_serial:
                    reached_stop = True
                    continue
                if cutoff is not None:
                    instant = _event_instant(event["event_time"])
                    if instant is None or instant < cutoff:
                        reached_stop = True
                        continue
                found[event["serial_no"]] = event
            if reached_stop or position == 0 or page["numOfMatches"] <= 0 or not raw_events:
                scan.finished = True
                break
            position = max(0, position - page_size)
            page = None
            if max_events is not None and len(found) >= max_events:
                scan.next_position = position
                break
        scan.events = [found[serial] for serial in sorted(found)]
        return scan

    def _bootstrap_events(self) -> dict[str, Any]:
        if self.config.initial_sync_full_history:
            LOG.warning("INITIAL_SYNC_FULL_HISTORY is enabled: the full device event history will be imported "
                        "over successive polls (at most MAX_PAGES_PER_POLL pages each).")
            self.store.set_state(BOOTSTRAP_STATE_KEY, "1")
            return self._poll_new_events() | {"bootstrap": "full_history"}
        hours = self.config.initial_sync_lookback_hours
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        max_events = self.config.initial_sync_max_events
        scan = self._scan_tail(start=None, stop_serial=-1, cutoff=cutoff,
                               max_pages=max_events // self.config.event_page_size + 2, max_events=max_events)
        if not scan.finished:
            LOG.warning("Initial sync stopped at INITIAL_SYNC_MAX_EVENTS=%s; older events in the window are skipped.",
                        max_events)
        cursor = max(scan.highest_serial, 0)
        scan.events = scan.events[-max_events:]  # the last page read may overshoot the cap
        inserted, duplicates = self.store.commit_discovery(scan.events, cursor=cursor,
                                                           state={BOOTSTRAP_STATE_KEY: "1"})
        skipped = max(0, (scan.total_matches or 0) - len(scan.events))
        LOG.info("Initial event sync: device reports %s matching events; queued %s from the last %s hour(s); "
                 "skipped %s older events; cursor set to serialNo %s.",
                 scan.total_matches, inserted, hours, skipped, cursor)
        return {"pages": scan.pages, "queued": inserted, "duplicates": duplicates, "invalid": scan.invalid,
                "bootstrap": "recent_window", "skipped_history": skipped, "cursor": cursor}

    def _poll_new_events(self) -> dict[str, Any]:
        cursor = self.store.discovery_cursor
        resume = self.store.get_state(BACKFILL_POSITION_KEY)
        backfill_high = int(self.store.get_state(BACKFILL_HIGH_SERIAL_KEY, "-1") or -1)
        scan = self._scan_tail(start=int(resume) if resume is not None else None, stop_serial=cursor, cutoff=None,
                               max_pages=self.config.max_pages_per_poll)
        highest = max(scan.highest_serial, backfill_high)
        if scan.finished:
            inserted, duplicates = self.store.commit_discovery(
                scan.events, cursor=highest, state={BACKFILL_POSITION_KEY: None, BACKFILL_HIGH_SERIAL_KEY: None})
        else:
            # Backlog larger than one poll: queue what we have but keep the cursor until the gap is closed.
            LOG.warning("Event backlog exceeds MAX_PAGES_PER_POLL; continuing from position %s next poll.",
                        scan.next_position)
            inserted, duplicates = self.store.commit_discovery(
                scan.events, cursor=None,
                state={BACKFILL_POSITION_KEY: str(scan.next_position), BACKFILL_HIGH_SERIAL_KEY: str(highest)})
        return {"pages": scan.pages, "queued": inserted, "duplicates": duplicates, "invalid": scan.invalid,
                "backlog_remaining": not scan.finished}

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

    def failure_backoff(self, failures: int) -> int:
        """Bounded exponential backoff for consecutive failures: 5, 10, 20 ... RETRY_MAX_SECONDS."""
        return min(self.config.retry_initial_seconds * (2 ** min(failures - 1, 16)), self.config.retry_max_seconds)

    def run(self) -> None:
        failures = 0
        try:
            while not self.stop_requested.is_set():
                try:
                    self.sync_once()
                    if failures:
                        LOG.info("Sync recovered after %s failed attempt(s); polling every %s seconds again.",
                                 failures, self.config.poll_interval_seconds)
                    failures = 0
                    delay = self.config.poll_interval_seconds
                except CloudAuthenticationError:
                    failures += 1
                    delay = max(3600, self.config.poll_interval_seconds)
                    LOG.error("Cloud authentication failed; retrying in %s seconds.", delay)
                except (CloudError, DeviceError) as exc:
                    failures += 1
                    delay = self.failure_backoff(failures)
                    LOG.warning("Sync failed (%s); retry %s in %s seconds.", exc, failures, delay)
                except Exception as exc:  # a 24/7 service must outlive unexpected errors too
                    failures += 1
                    delay = self.failure_backoff(failures)
                    LOG.error("Unexpected sync error (%s); retry %s in %s seconds.",
                              exc.__class__.__name__, failures, delay)
                    LOG.debug("Unexpected sync error detail.", exc_info=True)
                self.stop_requested.wait(delay)
        finally:
            self.store.close()
            LOG.info("Attendance agent stopped safely.")


def main() -> int:
    parser = argparse.ArgumentParser(description="EPCA ONE Hikvision attendance agent")
    parser.add_argument("command", choices=["test-device", "test-cloud", "sync-once", "health", "run", "discover-users",
                                            "event-capabilities"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        load_project_env(APP_DIR / ".env")  # real environment variables take precedence
        agent = AttendanceAgent(AgentConfig.from_environment(app_dir=APP_DIR))
        if args.command == "test-device":
            print(agent.test_device())
        elif args.command == "test-cloud":
            agent.test_cloud()
        elif args.command == "sync-once":
            print(agent.sync_once())
        elif args.command == "health":
            print(agent.health())
        elif args.command == "event-capabilities":
            print(agent.device.event_capabilities())
        elif args.command == "discover-users":
            result = agent.device.discover_users()
            result["users"] = [{k: v for k, v in user.items() if k != "raw"} for user in result["users"]]
            print(result)
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
