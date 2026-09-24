#!/usr/bin/env python3
"""Command line entrypoint for the EPCA ONE Proxmox attendance agent."""

from __future__ import annotations

import argparse
import logging
import math
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

if __package__ in {None, ""}:  # supports the documented `python agent.py ...` command
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:  # Package imports work inside EPCA ONE until this directory is extracted.
    from attendance_agent.cloud import CloudAuthenticationError, CloudError, EpcClient
    from attendance_agent.config import (RECOMMENDED_MIN_POLL_INTERVAL_SECONDS, AgentConfig, ConfigurationError,
                                         load_project_env)
    from attendance_agent.hikvision import MAX_LOCK_SECONDS, DeviceCooldownError, DeviceError, HikvisionClient
    from attendance_agent.storage import AgentStore, StorageError
except ModuleNotFoundError:  # Standalone repository: modules live beside agent.py.
    from cloud import CloudAuthenticationError, CloudError, EpcClient
    from config import RECOMMENDED_MIN_POLL_INTERVAL_SECONDS, AgentConfig, ConfigurationError, load_project_env
    from hikvision import MAX_LOCK_SECONDS, DeviceCooldownError, DeviceError, HikvisionClient
    from storage import AgentStore, StorageError


LOG = logging.getLogger("epca_attendance_agent")

# Directory of the launched agent.py, independent of the working directory (e.g. a Windows Service).
APP_DIR = Path(__file__).resolve().parent

# agent_state keys for event discovery (all durable in SQLite alongside last_discovered_serial).
BOOTSTRAP_STATE_KEY = "event_bootstrap_complete"
BACKFILL_POSITION_KEY = "event_backfill_position"
BACKFILL_HIGH_SERIAL_KEY = "event_backfill_high_serial"
# "<unix time> <lock|auth>": a Hikvision cooldown that must outlive a process/systemd restart.
DEVICE_COOLDOWN_STATE_KEY = "hikvision_cooldown_until"
# While nothing happens, the run loop logs one INFO status line per interval instead of one per poll.
HEARTBEAT_SECONDS = 3600


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
        self.clock: Callable[[], float] = time.monotonic  # scheduling only; replaced by a fake clock in tests
        if hasattr(self.device, "on_cooldown"):
            self.device.on_cooldown = self._remember_device_cooldown
        self._restore_device_cooldown()

    def request_stop(self) -> None:
        """Stop after the current request/SQLite transaction completes."""
        if not self.stop_requested.is_set():
            LOG.info("Shutdown requested; finishing the current operation safely.")
            self.stop_requested.set()

    def _remember_device_cooldown(self, seconds: float, kind: str) -> None:
        """Persist a Hikvision cooldown so a restart (or a CLI check) does not authenticate while locked."""
        self.store.set_state(DEVICE_COOLDOWN_STATE_KEY, f"{time.time() + seconds:.0f} {kind}")

    def _restore_device_cooldown(self) -> None:
        value = self.store.get_state(DEVICE_COOLDOWN_STATE_KEY)
        if not value or not hasattr(self.device, "pause"):
            return
        try:
            until, kind = value.split()
            remaining = float(until) - time.time()
        except ValueError:
            return
        # Bounded, so a wall-clock jump can never pause the device longer than any real lockout.
        remaining = min(remaining, MAX_LOCK_SECONDS + self.config.hikvision_lock_margin_seconds)
        if remaining > 0:
            self.device.pause(remaining, kind)
            LOG.warning("Hikvision requests remain paused for approximately %s seconds (%s recorded before restart).",
                        round(remaining), "account lock" if kind == "lock" else "authentication failure")

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
        error = ""
        while not self.stop_requested.is_set():
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
                error = str(exc)
                self.store.mark_retry(serials, error)
                self.store.set_state("last_cloud_error", error)
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
        return {"delivered": delivered, "rejected": rejected, "retried": retried, "error": error}

    def sync_once(self) -> dict[str, Any]:
        result = {"discovery": self.poll_device(), "delivery": self.upload_pending()}
        result["queue"] = self.store.queue_counts()
        LOG.info("Sync complete: queued=%s delivered=%s rejected=%s", result["discovery"]["queued"],
                 result["delivery"]["delivered"], result["delivery"]["rejected"])
        return result

    def backfill(self, range_from: date | None, range_to: date | None, *, all_history: bool = False,
                 dry_run: bool = False) -> dict[str, Any]:
        """Import a date-bounded AcsEvent history without touching live queue/cursor state.

        Checkpoint advancement occurs only after a page was either inspected in dry-run mode or
        acknowledged by EPCA.  Thus an interrupted cloud request is safely replayed by serial.
        """
        from_text = range_from.isoformat() if range_from else None
        to_text = range_to.isoformat() if range_to else None
        job_id = "all" if all_history else f"{from_text}:{to_text}"
        job = self.store.backfill_job(job_id, from_text, to_text, all_history)
        if job["completed_at"] and not dry_run:
            return job | {"complete": True, "resumed": True}
        totals = {key: int(job[key]) for key in ("scanned", "matched", "delivered", "already_existing", "unmapped", "rejected", "failed")}
        position, oldest, newest = int(job["next_position"]), job["oldest_event_time"], job["newest_event_time"]
        # A dry run intentionally has no durable checkpoint: it cannot later result in delivery.
        if dry_run:
            totals = {key: 0 for key in totals}; position = 0; oldest = newest = None
        while not self.stop_requested.is_set():
            page = self.device.search_events_page(position)
            raw_events = page["events"]
            selected: list[dict[str, Any]] = []
            for raw in raw_events:
                totals["scanned"] += 1
                event = event_from_isapi(raw)
                if event is None:
                    continue
                # The unmodified timestamp string defines the device-local calendar date.
                try:
                    event_day = date.fromisoformat(event["event_time"][:10])
                except ValueError:
                    continue
                if not all_history and (event_day < range_from or event_day > range_to):
                    continue
                selected.append(event)
                totals["matched"] += 1
                oldest = min(filter(None, [oldest, event["event_time"]]), default=event["event_time"])
                newest = max(filter(None, [newest, event["event_time"]]), default=event["event_time"])
            if not dry_run and selected:
                response = self.cloud.send_events(selected)
                errors = {item.get("index") for item in response.get("error_details", []) if isinstance(item, dict)}
                totals["rejected"] += len(errors)
                acknowledged = len(selected) - len(errors)
                duplicates = min(max(int(response.get("duplicates", 0) or 0), 0), acknowledged)
                totals["already_existing"] += duplicates
                totals["delivered"] += acknowledged - duplicates
                # Only use this explicit server aggregate; never infer unmapped employees locally.
                totals["unmapped"] += min(max(int(response.get("unmapped", 0) or 0), 0), acknowledged)
            # Some firmware returns a short page before the final page; advance by what it
            # actually returned so no historical records are skipped.
            next_position = position + max(1, int(page.get("numOfMatches") or len(raw_events)))
            finished = not raw_events or next_position >= int(page.get("totalMatches") or next_position)
            if not dry_run:
                self.store.update_backfill_job(job_id, next_position=next_position, oldest_event_time=oldest,
                                               newest_event_time=newest, complete=finished, **totals)
            if totals["scanned"] and totals["scanned"] % 1000 < max(1, len(raw_events)):
                LOG.info("Backfill progress: scanned=%s matched=%s delivered=%s existing=%s", totals["scanned"],
                         totals["matched"], totals["delivered"], totals["already_existing"])
            if finished:
                return totals | {"oldest_event_time": oldest, "newest_event_time": newest, "complete": True,
                                 "dry_run": dry_run}
            position = next_position
        return totals | {"oldest_event_time": oldest, "newest_event_time": newest, "complete": False,
                         "dry_run": dry_run}

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

    def _log_startup(self) -> None:
        cfg = self.config
        LOG.info("Attendance agent started: device_code=%s hikvision_host=%s poll_interval=%ss database=%s "
                 "cloud_host=%s", cfg.epca_device_code, cfg.hikvision_host, cfg.poll_interval_seconds,
                 cfg.database_path, urlparse(cfg.epca_api_url).hostname)
        if cfg.poll_interval_seconds < RECOMMENDED_MIN_POLL_INTERVAL_SECONDS:
            LOG.warning("POLL_INTERVAL_SECONDS=%s is below the recommended %s-30 seconds for the DS-K1T8003MF; "
                        "keep it only if soak testing proved it stable.", cfg.poll_interval_seconds,
                        RECOMMENDED_MIN_POLL_INTERVAL_SECONDS)
        pending = self.store.queue_counts()["pending"]
        if pending:
            LOG.info("%s queued event(s) from a previous run are pending delivery.", pending)

    def _heartbeat(self) -> None:
        counts = self.store.queue_counts()
        LOG.info("Agent running: pending=%s delivered=%s rejected=%s last_discovered_serial=%s",
                 counts["pending"], counts["delivered"], counts["rejected"], self.store.discovery_cursor)

    def _device_phase(self, failures: int) -> tuple[int, float]:
        """Poll the terminal once; returns (consecutive failures, seconds until the next poll)."""
        try:
            discovery = self.poll_device()
        except DeviceCooldownError as exc:  # the client has already logged the lock/auth pause
            return failures + 1, max(1, math.ceil(exc.retry_after))
        except DeviceError as exc:
            failures += 1
            delay = self.failure_backoff(failures)
            LOG.warning("Hikvision unavailable (%s); retry %s in %s seconds.", exc, failures, delay)
            return failures, delay
        except Exception as exc:  # a 24/7 service must outlive unexpected errors too
            failures += 1
            delay = self.failure_backoff(failures)
            LOG.error("Unexpected device polling error (%s); retry %s in %s seconds.",
                      exc.__class__.__name__, failures, delay)
            LOG.debug("Unexpected device polling error detail.", exc_info=True)
            return failures, delay
        if failures:
            LOG.info("Hikvision connection recovered after %s failed attempt(s); polling every %s seconds again.",
                     failures, self.config.poll_interval_seconds)
        if discovery["queued"]:
            LOG.info("Discovered %s new attendance event(s).", discovery["queued"])
        return 0, self.config.poll_interval_seconds

    def _cloud_phase(self, failures: int) -> tuple[int, float]:
        """Deliver the queue; returns (consecutive failures, seconds before delivery may be attempted again)."""
        try:
            delivery = self.upload_pending()
        except CloudAuthenticationError:
            delay = max(3600, self.config.poll_interval_seconds)
            LOG.error("EPCA ONE rejected the device credentials; queued events are kept. Retrying in %s seconds.",
                      delay)
            return failures + 1, delay
        except Exception as exc:
            failures += 1
            delay = self.failure_backoff(failures)
            LOG.error("Unexpected cloud delivery error (%s); retry %s in %s seconds.",
                      exc.__class__.__name__, failures, delay)
            LOG.debug("Unexpected cloud delivery error detail.", exc_info=True)
            return failures, delay
        sent = delivery["delivered"] + delivery["rejected"]
        if failures and sent:
            LOG.info("Cloud connection recovered after %s failed attempt(s).", failures)
        if sent:
            LOG.info("Delivered %s queued event(s) to EPCA ONE (%s rejected).", delivery["delivered"],
                     delivery["rejected"])
        if delivery["retried"]:
            failures = 0 if sent else failures
            failures += 1
            delay = self.failure_backoff(failures)
            LOG.warning("EPCA ONE unavailable (%s); %s event(s) kept in the queue; retry %s in %s seconds.",
                        delivery["error"], delivery["retried"], failures, delay)
            return failures, delay
        return 0, 0

    def run(self) -> None:
        """Foreground loop for systemd: device polling and cloud delivery recover independently, forever.

        Device and cloud each keep their own schedule, so a locked or offline terminal never blocks delivery of
        queued events and an Internet outage never slows device polling. Only request_stop() ends the loop.
        """
        device_failures = cloud_failures = 0
        try:
            self._log_startup()
            now = self.clock()
            device_due = cloud_due = now
            next_heartbeat = now + HEARTBEAT_SECONDS
            while not self.stop_requested.is_set():
                now = self.clock()
                if now >= device_due:
                    device_failures, delay = self._device_phase(device_failures)
                    device_due = now + delay
                if self.stop_requested.is_set():
                    break
                if now >= cloud_due:  # a healthy cloud is served right after every device poll
                    cloud_failures, delay = self._cloud_phase(cloud_failures)
                    cloud_due = now + delay
                if now >= next_heartbeat:
                    next_heartbeat = now + HEARTBEAT_SECONDS
                    try:
                        self._heartbeat()
                    except Exception:
                        LOG.debug("Heartbeat failed.", exc_info=True)
                # Only a failed delivery needs its own wake-up (the device may be paused for a long lockout).
                wake = min(device_due, cloud_due) if cloud_failures else device_due
                self.stop_requested.wait(max(0.0, wake - now))
        finally:
            for resource in (self.device, self.cloud):
                close = getattr(resource, "close", None)
                if close is not None:
                    try:
                        close()
                    except Exception:
                        LOG.debug("Closing %s failed.", resource.__class__.__name__, exc_info=True)
            self.store.close()
            LOG.info("Attendance agent stopped safely.")


def install_signal_handlers(agent: AttendanceAgent) -> None:
    """SIGTERM (systemd stop) and SIGINT (Ctrl+C) end the run loop after the current request/transaction."""
    def handle_shutdown(signum, frame):
        agent.request_stop()
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)


def main() -> int:
    parser = argparse.ArgumentParser(description="EPCA ONE Hikvision attendance agent")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("test-device", "test-cloud", "sync-once", "health", "run", "discover-users", "event-capabilities"):
        commands.add_parser(command)
    backfill_parser = commands.add_parser("backfill", help="Safely import historical attendance without changing live cursor")
    backfill_parser.add_argument("--from", dest="range_from", metavar="YYYY-MM-DD", help="First device-local date, inclusive")
    backfill_parser.add_argument("--to", dest="range_to", metavar="YYYY-MM-DD", help="Last device-local date, inclusive")
    backfill_parser.add_argument("--all", action="store_true", help="Scan all terminal history")
    backfill_parser.add_argument("--dry-run", action="store_true", help="Scan and report only; never send or checkpoint")
    args = parser.parse_args()
    if args.command == "backfill":
        if args.all and (args.range_from or args.range_to):
            parser.error("backfill accepts --all or --from/--to, not both")
        if not args.all and not (args.range_from and args.range_to):
            parser.error("backfill requires --all or both --from YYYY-MM-DD and --to YYYY-MM-DD")
        try:
            range_from = date.fromisoformat(args.range_from) if args.range_from else None
            range_to = date.fromisoformat(args.range_to) if args.range_to else None
        except ValueError:
            parser.error("backfill dates must use YYYY-MM-DD")
        if range_from and range_from > range_to:
            parser.error("backfill --from must be on or before --to")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        load_project_env(APP_DIR / ".env")  # real environment variables take precedence
        level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise ConfigurationError("LOG_LEVEL must be DEBUG, INFO, WARNING or ERROR.")
        logging.getLogger().setLevel(level)
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
        elif args.command == "backfill":
            install_signal_handlers(agent)
            result = agent.backfill(range_from, range_to, all_history=args.all, dry_run=args.dry_run)
            print("Historical attendance backfill" + (" (dry run)" if args.dry_run else ""))
            print(f"Range: {'all history' if args.all else f'{range_from} -> {range_to}'}")
            for label, key in (("Scanned", "scanned"), ("Matched attendance", "matched"),
                               ("Delivered", "delivered"), ("Already existing", "already_existing"),
                               ("Unmapped", "unmapped"), ("Rejected", "rejected"), ("Failed", "failed"),
                               ("Oldest imported event", "oldest_event_time"), ("Newest imported event", "newest_event_time")):
                print(f"{label}: {result.get(key, 0) if result.get(key) is not None else '-'}")
            print("Backfill complete" if result.get("complete") else "Backfill interrupted; rerun the same range to resume")
        else:
            install_signal_handlers(agent)
            agent.run()
        return 0
    except (ConfigurationError, StorageError, DeviceError, CloudError) as exc:
        LOG.error("%s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
