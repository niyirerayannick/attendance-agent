# EPCA ONE Hikvision attendance agent

This is a small outbound-only service for an Ubuntu VM on the local Proxmox network. It reads attendance event metadata from a Hikvision terminal over HTTP Digest authentication and posts batches to EPCA ONE over HTTPS. It never receives inbound internet traffic, never moves biometric templates, and does not calculate payroll, attendance status, overtime, or leave.

## What is persisted locally

`$AGENT_DATA_DIR/attendance_agent.db` (`/var/lib/epca-attendance-agent` under systemd, `/data` in Docker) contains source event metadata, delivery status, retry history, and the last discovered `serialNo`. It does **not** contain terminal or EPCA credentials; those exist only in the runtime environment. Discovery and cursor movement occur in one SQLite transaction. An event is therefore queued before its discovery cursor can advance. Cloud delivery is separate: a queued event is retained until EPCA acknowledges it, and duplicates are safe because EPCA deduplicates by device code plus serial number.

The queue has `pending`, `delivered`, and `rejected` states. A malformed event rejected by EPCA is retained locally with its error for investigation. Device/network/5xx failures leave events pending for later retry. Cloud authentication errors wait at least an hour in continuous mode; Hikvision authentication errors and lockouts are described under *Hikvision lockout protection*.

## Hikvision endpoints

The agent uses these standard ISAPI endpoints with HTTP Digest authentication:

* `GET /ISAPI/System/deviceInfo` for `test-device`.
* `POST /ISAPI/AccessControl/AcsEvent?format=json` with `AcsEventCond` for event pages. The body is exactly
  `{"AcsEventCond": {"searchID": "1", "searchResultPosition": <n>, "maxResults": 10, "major": 5, "minor": 38}}`;
  the DS-K1T8003MF (V1.3.37) rejects a UUID `searchID` or an oversized `maxResults` with `badParameters`.
* `GET /ISAPI/AccessControl/AcsEvent/capabilities?format=json` for the read-only `event-capabilities` command.
* `POST /ISAPI/AccessControl/UserInfo/Search?format=json` with `UserInfoSearchCond` for the optional `discover-users` diagnostic command.

Hikvision firmware and terminal access-control configuration vary. Test the event search response on the terminal before enabling the systemd service. The event timestamp must include its timezone offset because the EPCA endpoint preserves and checks that offset. If a terminal returns local timestamps without an offset, correct the terminal timezone/NTP configuration first; the agent deliberately does not invent an offset.

The existing Phase 1 cloud endpoint supports event ingestion only. There is no device-user inventory endpoint, so `discover-users` is diagnostic only. To use device discovery for HR mapping, add a separately authenticated Phase 1 extension that stores a reviewable device-user staging list; do not automatically map it to employees.

## Standalone repository layout

The contents of this directory are the complete `epca-attendance-agent` repository. Copy its
contents, rather than the parent EPCA ONE repository, into the new private repository:

```text
agent.py  cloud.py  config.py  hikvision.py  storage.py
requirements.txt  Dockerfile  .dockerignore  .gitignore  .env.example
README.md  tests/  systemd/
```

It imports only Python standard-library modules and `requests`. It does not import Django or any
EPCA ONE `people`, `employees`, `config`, `accounts`, or other application module.

## Installation on Ubuntu 22.04 (systemd)

Production layout: code in `/home/epca/attendance-agent`, virtual environment in `.venv`, secrets in
the project-root `.env` (loaded by `agent.py`), durable state in `/var/lib/epca-attendance-agent`.

```bash
sudo adduser --disabled-password --gecos "" epca          # skip if the user already exists
sudo -u epca git clone <private-repo-url> /home/epca/attendance-agent
cd /home/epca/attendance-agent
sudo -u epca python3 -m venv .venv
sudo -u epca .venv/bin/pip install -r requirements.txt
sudo -u epca cp .env.example .env
sudo chmod 600 .env
sudo install -d -o epca -g epca -m 0750 /var/lib/epca-attendance-agent
```

Edit `/home/epca/attendance-agent/.env` with the terminal's LAN address, a least-privilege terminal
user, device code, and the one-time EPCA device token, and keep
`AGENT_DATA_DIR=/var/lib/epca-attendance-agent`. Do not put values in the repository, the systemd unit,
or shell history. EPCA URL is required to be HTTPS and certificate verification is always enabled.
`HIKVISION_SCHEME` defaults to `http`; use `https` only when the terminal is configured for it.
`HIKVISION_VERIFY_TLS` stays true by default. If the data directory is missing or not writable the
agent exits at startup with a message naming the directory and the command to create it; it never
deletes or recreates an existing database.

Run one-shot checks as the service user:

```bash
cd /home/epca/attendance-agent
sudo -u epca .venv/bin/python agent.py test-device
sudo -u epca .venv/bin/python agent.py test-cloud
sudo -u epca .venv/bin/python agent.py sync-once
sudo -u epca .venv/bin/python agent.py health
```

`test-cloud` posts an empty batch. EPCA returns the expected validation 400 only after it validates the device bearer token, so it sends no attendance event. `sync-once` first persists discovered events and then uploads pending events. `health` checks both connections and reports their status without exposing secrets. `discover-users` prints every device user (paged 10 at a time) and is for controlled HR mapping discovery. `event-capabilities` prints the terminal's supported `AcsEventCond` fields for diagnosis. While a Hikvision lockout recorded by the service is active, these commands report it instead of authenticating.

Enable continuous operation:

```bash
sudo cp /home/epca/attendance-agent/systemd/epca-attendance-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now epca-attendance-agent
sudo systemctl status epca-attendance-agent
journalctl -u epca-attendance-agent -f
```

`python agent.py run` is a long-running foreground process; it never forks or daemonizes. systemd
owns supervision (`Restart=always`, `RestartSec=10`). `systemctl stop` sends SIGTERM (Ctrl+C sends
SIGINT): the agent stops waiting immediately, lets the in-flight request and SQLite transaction
finish, closes the HTTP sessions and the database, logs `Attendance agent stopped safely.` and exits 0
well inside `TimeoutStopSec=30`. The unit contains no secrets; `StateDirectory=` creates
`/var/lib/epca-attendance-agent` for `epca` if it is missing, and `ProtectSystem=strict` /
`ProtectHome=read-only` leave only that directory writable.

## Polling interval for the DS-K1T8003MF

`POLL_INTERVAL_SECONDS` is always taken from configuration and is never changed by the agent (the code
default when unset is `60`). Physical testing showed this old terminal locks its account after repeated
authentication failures, so start production at **15–30 seconds** (`30` in `.env.example`). Shorten it
only after several days at that rate show no `HTTP 401`, `authentication failed` or `account locked`
warnings in the journal. Below 15 seconds the agent logs a startup warning but still uses the value.

## Failure and recovery

`run` never terminates because of an Internet outage, DNS failure, EPCA ONE timeout or 5xx/429,
Hikvision timeout, connection refused/reset, or a terminal reboot. Each problem is logged, state is
kept, and the affected side retries on its own schedule:

* **Device and cloud are independent.** A locked or offline terminal does not stop delivery of queued
  events, and an Internet outage does not slow device polling.
* **Hikvision offline:** retries follow a bounded exponential backoff of `RETRY_INITIAL_SECONDS`
  (default `5`) doubling up to `RETRY_MAX_SECONDS` (default `300`). The first successful poll logs
  `Hikvision connection recovered` and returns to `POLL_INTERVAL_SECONDS`. The discovery cursor is never
  reset or deleted.
* **EPCA ONE offline:** discovered events stay `pending` in SQLite (never marked delivered) and delivery
  is retried with the same bounded backoff, which is independent of polling and never faster than
  every 5 seconds. Recovery logs `Cloud connection recovered` and `Delivered N queued event(s)`.
  Rejected events keep their existing `rejected` handling. Cloud 401/403 waits an hour between attempts.
* **Restarts:** pending events, the cursor, any backlog position and any Hikvision cooldown survive a
  process or Ubuntu restart. After restart, pending events are delivered once. The same terminal events
  are not queued again because discovery resumes from the durable cursor.

### Hikvision lockout protection

The DS-K1T8003MF (V1.3.37) rejects a Digest session reused from an earlier poll, so **every Hikvision
request uses a new `requests.Session` and `HTTPDigestAuth`**. The previous session is closed first. Each
request performs the standard negotiation: an unauthenticated challenge (HTTP 401), one Digest request,
then the response. That internal challenge is not a failure. Only an HTTP 401 that is still the final
response after negotiation counts as an authentication failure. The agent then parses the device's `<userCheck>`/`ResponseStatus` body (XML or JSON) for `lockStatus`, `unlockTime`,
`retryLoginTime` and `statusString`:

1. **`lockStatus=lock`:** no retry. The agent logs
   `Hikvision account locked; pausing device requests for approximately N seconds.` and sends **no**
   device request of any kind for `unlockTime + HIKVISION_LOCK_MARGIN_SECONDS` (default margin `30`).
   If `unlockTime` is missing or implausible (outside 1 s–24 h) it uses `HIKVISION_LOCK_DEFAULT_SECONDS`
   (default `1800`). When the pause ends it creates a new `requests.Session` and `HTTPDigestAuth` and
   sends one normal request. If the device is still locked, it reads the new `unlockTime` and waits again.
2. **Ordinary final 401 (not locked):** no retry, because the session was already fresh. Device requests
   pause for `HIKVISION_AUTH_COOLDOWN_SECONDS` (default `300`), doubling on each repeat up to one hour.
   After a pause, a single request is made.
3. The pause is also written to SQLite, so a systemd restart or a manual `test-device` does not
   authenticate against a locked account.

Logs never contain the password, the `Authorization` header, the Digest response or nonce, or the EPCA
token. Only whitelisted status fields from the device's error body are logged.

### Logging

At startup the agent logs the device code, Hikvision host, poll interval, database path and EPCA
hostname. At `LOG_LEVEL=INFO` (default) it then logs only discoveries, deliveries, failures,
recoveries and one status line per hour; a quiet healthy poll logs nothing. `LOG_LEVEL=DEBUG` adds each
`AcsEventCond` request.

Do not delete the database to solve an upload issue, because that discards the durable discovery
state. Back up `/var/lib/epca-attendance-agent/attendance_agent.db` (with its `-wal`/`-shm` files) only
while the service is stopped or by using SQLite's backup tooling.

For an upgrade, stop the service, back up the database, `git pull` in `/home/epca/attendance-agent`, update dependencies in the existing `.venv`, then start the service. Keep `AGENT_DATA_DIR` unchanged. `journalctl -u epca-attendance-agent -f` is the first place to investigate a device or cloud error; run `health`, then `test-device` and `test-cloud` after correcting network or credential settings.

## Deploying with Coolify

Use the **local** Coolify instance running on Proxmox to build this outbound worker. The
container reaches `192.168.88.187` over the Proxmox office LAN and reaches the public EPCA ONE
domain over Starlink HTTPS. It does not need, and must not have, a public domain, a proxy,
published ports, MikroTik port forwarding, or any direct connection to the Hostinger Coolify
instance.

Create an **Application** in local Coolify from the standalone private repository. Its Dockerfile
is at the repository root and its build context is the repository root. The image is built with:

```bash
docker build -t epca-attendance-agent:local .
docker run --rm --name epca-attendance-agent \
  --env-file /secure/epca-attendance-agent.env \
  --mount type=volume,source=epca_attendance_agent_data,target=/data \
  epca-attendance-agent:local
```

In Coolify, add a persistent volume at `/data` (for example, named
`epca_attendance_agent_data`). The database defaults to `/data/attendance_agent.db`; its WAL and
shared-memory files live beside it. Keep this same volume attached on every deployment and
rollback. Container filesystem state is intentionally disposable, but deleting or replacing the
`/data` volume loses pending events and the discovery cursor.

Set these Coolify environment secrets, rather than adding an `.env` file to the repository:

```text
HIKVISION_HOST=192.168.88.187
HIKVISION_USERNAME=<secret>
HIKVISION_PASSWORD=<secret>
EPCA_API_URL=https://one.epcafrica.com/api/internal/attendance/events/
EPCA_DEVICE_CODE=EPCA-HQ-01
EPCA_DEVICE_TOKEN=<secret>
POLL_INTERVAL_SECONDS=30
AGENT_DATA_DIR=/data
```

### Local `.env` file (native Windows or development)

`python agent.py <command>` automatically loads a `.env` file located **next to `agent.py`** (resolved
from the script location, not the current working directory, so a Windows Service finds it too).
Real OS environment variables always take precedence over `.env` values (`override=False`); in
Docker/Coolify no `.env` is needed. Copy `.env.example` to `.env` and fill in real values; `.env` is
git- and docker-ignored and must never be committed. Only the path of the loaded file is logged.
On Windows set `AGENT_DATA_DIR` to a full path such as `C:\EPCAttendance-agent\data`; when unset
it defaults to a `data` folder next to `agent.py` (and to `/data` on Linux).

Optional settings are `HIKVISION_SCHEME` (default `http`), `HIKVISION_VERIFY_TLS` (default
`true`), `DEVICE_TIMEOUT_SECONDS`, `CLOUD_TIMEOUT_SECONDS`, `BATCH_SIZE`, `EVENT_PAGE_SIZE`, and
`MAX_PAGES_PER_POLL` (default `EVENT_PAGE_SIZE=10`), `RETRY_INITIAL_SECONDS` / `RETRY_MAX_SECONDS` (default `5` / `300`), `HIKVISION_EVENT_MAJOR` / `HIKVISION_EVENT_MINOR`
(default `5` / `38`, fingerprint verified), `HIKVISION_LOCK_DEFAULT_SECONDS` / `HIKVISION_LOCK_MARGIN_SECONDS` /
`HIKVISION_AUTH_COOLDOWN_SECONDS` (default `1800` / `30` / `300`), `LOG_LEVEL` (default `INFO`), and the initial-sync settings below. `AGENT_DATABASE_PATH=/data/attendance_agent.db` can override the default,
but should remain inside the persistent volume. EPCA HTTPS certificate verification is always on.

### First start: recent events only

The terminal keeps its whole event log (tens of thousands of events back to 2022). With an empty
database the agent does **not** import that history. The first poll reads `totalMatches`, walks back
from the newest page, and queues only events from the last `INITIAL_SYNC_LOOKBACK_HOURS` (default `24`;
`0` queues nothing), capped at `INITIAL_SYNC_MAX_EVENTS` (default `1000`) in case the terminal clock is
wrong. The highest `serialNo` seen becomes the durable cursor, and later polls queue only newer
events. The agent uses only `AcsEventCond` fields proven on this firmware, so the window is applied to
the event `time` locally rather than through `startTime`/`endTime`. Timestamps are stored exactly as
the device reports them (for example `+08:00`) and are not converted.

`INITIAL_SYNC_FULL_HISTORY=true` (default `false`) imports the full history instead, at most
`MAX_PAGES_PER_POLL` pages per poll. Set it only before the first start and only when EPCA ONE should
receive every historical event. EPCA ONE deduplicates on device plus serial number, so re-sent events
are safe.

The image starts `python agent.py run`; no ports are exposed. Docker runs the existing
`python agent.py health` every 60 seconds after a 90-second start period. The health command
shows temporary Hikvision or cloud outages but exits successfully when the agent process and
SQLite state are usable, so a short terminal or Starlink interruption does not cause a Docker
restart loop. Docker `SIGTERM` is handled by ending the wait immediately, completing the current
request/SQLite transaction, closing the database, and exiting cleanly.

For the first deployment, deploy with the persistent volume and secrets, then use Coolify's
terminal (or a one-off container with the same volume and environment) to run:

```bash
python agent.py test-device
python agent.py test-cloud
python agent.py sync-once
python agent.py health
```

Review the Coolify application logs for the normal sync counters. The worker has no HTTP server;
Coolify logs are the operational log stream. For upgrades, keep `/data` attached, deploy the new
image, and verify `health`. For rollback, redeploy the previous image with the exact same `/data`
volume and environment variables. To back up state, stop the application first and archive the
entire persistent volume (database plus WAL files), or use SQLite's online backup tooling; never
copy only the main database while it is actively being written.

## Verification and tests

All agent tests mock device and cloud HTTP behavior; they never need a terminal or an EPCA server:

```bash
python -m unittest discover -s tests -v
```
