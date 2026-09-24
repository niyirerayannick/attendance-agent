"""Durable SQLite queue. Credentials are deliberately never stored here."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StorageError(RuntimeError):
    """The durable queue cannot be opened; raised at startup with an actionable message."""


class AgentStore:
    def __init__(self, path: str):
        self.path = path
        parent = Path(path).parent
        try:
            if parent != Path("."):
                parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise StorageError(
                f"Agent data directory {parent} does not exist and could not be created ({exc.__class__.__name__}). "
                f"Create it for the service user, e.g. `sudo install -d -o epca -g epca {parent}`, "
                "or set AGENT_DATA_DIR to a writable persistent directory.") from exc
        try:
            # Opens the existing database; tables are only created when missing, nothing is ever reset.
            self.connection = sqlite3.connect(path)
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA journal_mode=WAL")
            # FULL: a committed queue insert or delivery mark survives a power loss, not only a process crash.
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self._create_schema()
        except sqlite3.Error as exc:
            raise StorageError(f"Cannot open agent database {path}: {exc}. Check that AGENT_DATA_DIR is writable "
                               "by the service user.") from exc
        self._closed = False

    def close(self) -> None:
        """Commit anything outstanding and close; WAL content is checkpointed by SQLite on the last close."""
        if not self._closed:
            try:
                self.connection.commit()
            finally:
                self.connection.close()
                self._closed = True

    def _create_schema(self) -> None:
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS agent_state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queued_events (
                serial_no INTEGER PRIMARY KEY,
                employee_no TEXT NOT NULL,
                event_time TEXT NOT NULL,
                major INTEGER, minor INTEGER,
                attendance_status TEXT NOT NULL DEFAULT '',
                verification_method TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL,
                delivery_status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT, last_error TEXT,
                created_at TEXT NOT NULL, delivered_at TEXT
            );
            CREATE INDEX IF NOT EXISTS queued_events_delivery_idx
                ON queued_events(delivery_status, serial_no);
        """)
        self.connection.commit()

    def get_state(self, key: str, default: str | None = None) -> str | None:
        row = self.connection.execute("SELECT value FROM agent_state WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_state(self, key: str, value: str) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO agent_state(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    @property
    def discovery_cursor(self) -> int:
        return int(self.get_state("last_discovered_serial", "0") or 0)

    def queue_events(self, events: Iterable[dict[str, Any]]) -> tuple[int, int]:
        """Insert events and move the discovery cursor in the same transaction."""
        inserted = duplicates = 0
        max_serial = self.discovery_cursor
        with self.connection:
            for event in events:
                serial = int(event["serial_no"])
                cursor = self.connection.execute(
                    """INSERT OR IGNORE INTO queued_events
                    (serial_no, employee_no, event_time, major, minor, attendance_status,
                     verification_method, raw_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (serial, event["employee_no"], event["event_time"], event.get("major"), event.get("minor"),
                     event.get("attendance_status", ""), event.get("verification_method", ""),
                     json.dumps(event["raw_payload"], separators=(",", ":")), _now()),
                )
                if cursor.rowcount:
                    inserted += 1
                else:
                    duplicates += 1
                max_serial = max(max_serial, serial)
            if max_serial > self.discovery_cursor:
                self.connection.execute(
                    "INSERT INTO agent_state(key, value) VALUES ('last_discovered_serial', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(max_serial),),
                )
        return inserted, duplicates

    def commit_discovery(self, events: Iterable[dict[str, Any]], cursor: int | None,
                         state: dict[str, str | None] | None = None) -> tuple[int, int]:
        """Queue events, optionally raise the cursor, and update/delete state keys in ONE transaction.

        Unlike queue_events, the cursor only moves to the explicit ``cursor`` value, so a partial
        backlog scan can queue events without skipping the older events it has not reached yet.
        """
        inserted = duplicates = 0
        with self.connection:
            for event in events:
                result = self.connection.execute(
                    """INSERT OR IGNORE INTO queued_events
                    (serial_no, employee_no, event_time, major, minor, attendance_status,
                     verification_method, raw_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (int(event["serial_no"]), event["employee_no"], event["event_time"], event.get("major"),
                     event.get("minor"), event.get("attendance_status", ""), event.get("verification_method", ""),
                     json.dumps(event["raw_payload"], separators=(",", ":")), _now()),
                )
                if result.rowcount:
                    inserted += 1
                else:
                    duplicates += 1
            if cursor is not None and cursor > self.discovery_cursor:
                self.connection.execute(
                    "INSERT INTO agent_state(key, value) VALUES ('last_discovered_serial', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(cursor),),
                )
            for key, value in (state or {}).items():
                if value is None:
                    self.connection.execute("DELETE FROM agent_state WHERE key = ?", (key,))
                else:
                    self.connection.execute(
                        "INSERT INTO agent_state(key, value) VALUES (?, ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value),
                    )
        return inserted, duplicates

    def pending_events(self, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM queued_events WHERE delivery_status = 'pending' ORDER BY serial_no LIMIT ?", (limit,)
        ).fetchall()
        return [dict(row) | {"raw_payload": json.loads(row["raw_json"])} for row in rows]

    def mark_delivered(self, serials: Iterable[int]) -> None:
        values = list(serials)
        if not values:
            return
        with self.connection:
            self.connection.executemany(
                "UPDATE queued_events SET delivery_status='delivered', delivered_at=?, last_error=NULL WHERE serial_no=?",
                [(_now(), serial) for serial in values],
            )
        self.set_state("last_successful_upload", _now())

    def mark_rejected(self, serial: int, error: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE queued_events SET delivery_status='rejected', attempt_count=attempt_count+1, last_attempt_at=?, last_error=? WHERE serial_no=?",
                (_now(), error[:1000], serial),
            )

    def mark_retry(self, serials: Iterable[int], error: str) -> None:
        with self.connection:
            self.connection.executemany(
                "UPDATE queued_events SET attempt_count=attempt_count+1, last_attempt_at=?, last_error=? WHERE serial_no=?",
                [(_now(), error[:1000], serial) for serial in serials],
            )

    def queue_counts(self) -> dict[str, int]:
        rows = self.connection.execute("SELECT delivery_status, COUNT(*) AS count FROM queued_events GROUP BY delivery_status").fetchall()
        counts = {"pending": 0, "delivered": 0, "rejected": 0}
        counts.update({row["delivery_status"]: row["count"] for row in rows})
        return counts
