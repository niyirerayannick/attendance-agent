"""Small HTTP Digest client for Hikvision ISAPI attendance endpoints."""

from __future__ import annotations

import json
import logging
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Callable

import requests
from requests.auth import HTTPDigestAuth

try:
    from attendance_agent.config import AgentConfig
except ModuleNotFoundError:
    from config import AgentConfig


LOG = logging.getLogger("epca_attendance_agent.hikvision")

USER_SEARCH_PATH = "/ISAPI/AccessControl/UserInfo/Search?format=json"
# DS-K1T8003MF firmware V1.3.37 rejects UserInfoSearchCond.maxResults above 10.
USER_SEARCH_MAX_RESULTS = 10
# This firmware returns 400 badParameters for a UUID searchID; the short ID "1" is proven to work.
USER_SEARCH_ID = "1"

ACS_EVENT_PATH = "/ISAPI/AccessControl/AcsEvent?format=json"
# Same firmware rule as user search: a UUID searchID is rejected with badParameters, "1" is proven.
ACS_EVENT_SEARCH_ID = "1"
# Hard stop for discovery so a misbehaving device can never loop forever (10 users per page).
USER_SEARCH_MAX_PAGES = 5000

# Only these fields from a Hikvision error body are ever logged; everything else is discarded.
_ERROR_FIELDS = ("statusCode", "statusString", "subStatusCode", "errorCode", "errorMsg",
                 "lockStatus", "unlockTime", "retryLoginTime")

# Fields of the <userCheck>/ResponseStatus body a Hikvision 401 carries, e.g. while the account is locked:
# <statusValue>401</statusValue><statusString>Unauthorized</statusString><lockStatus>lock</lockStatus>
# <unlockTime>1795</unlockTime><retryLoginTime>0</retryLoginTime>
_AUTH_FIELDS = ("statusValue", "statusString", "lockStatus", "unlockTime", "retryLoginTime")
# A device-reported unlockTime outside 1..MAX_LOCK_SECONDS is treated as unknown.
MAX_LOCK_SECONDS = 86_400
# Consecutive ordinary authentication failures double the cooldown up to this ceiling.
MAX_AUTH_COOLDOWN_SECONDS = 3_600


class DeviceError(RuntimeError):
    pass


class DeviceCooldownError(DeviceError):
    """Device requests are paused; ``retry_after`` is the number of seconds until one is permitted again."""
    kind = "cooldown"

    def __init__(self, message: str, retry_after: float):
        super().__init__(message)
        self.retry_after = retry_after


class DeviceLockedError(DeviceCooldownError):
    """The device reported lockStatus=lock for the configured account."""
    kind = "lock"


class DeviceAuthenticationError(DeviceCooldownError):
    """HTTP 401 persisted after the single fresh-session recovery attempt."""
    kind = "auth"


_COOLDOWN_ERRORS = {cls.kind: cls for cls in (DeviceLockedError, DeviceAuthenticationError)}


DEVICE_INFO_FIELDS = ("deviceName", "deviceID", "model", "serialNumber", "macAddress",
                      "firmwareVersion", "firmwareReleasedDate", "deviceType")


def _local_name(tag: str) -> str:
    """Strip an ElementTree namespace prefix such as '{http://www.isapi.org/ver20/XMLSchema}model'."""
    return tag.rsplit("}", 1)[-1]


# UTF-8 BOM plus ASCII whitespace that some firmware emits before the XML declaration.
_LEADING_JUNK = b"\xef\xbb\xbf \t\r\n"


def _looks_like_xml(body: bytes, content_type: str) -> bool:
    return "xml" in content_type.lower() or body.lstrip(_LEADING_JUNK).startswith(b"<")


def parse_device_info_xml(body: bytes | str) -> dict[str, Any]:
    """Flatten an ISAPI <DeviceInfo> XML document into {fieldName: text}, ignoring the namespace."""
    raw = (body.encode("utf-8") if isinstance(body, str) else body).lstrip(_LEADING_JUNK)
    if b"<!DOCTYPE" in raw or b"<!ENTITY" in raw:
        raise DeviceError("Device returned invalid deviceInfo XML (DTD not allowed).")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise DeviceError("Device returned invalid deviceInfo XML.") from exc
    if _local_name(root.tag) != "DeviceInfo":
        raise DeviceError("Device returned unexpected deviceInfo XML root element.")
    info: dict[str, Any] = {}
    for child in root:
        if len(child) == 0:
            info[_local_name(child.tag)] = (child.text or "").strip()
    for field in DEVICE_INFO_FIELDS:
        info.setdefault(field, None)
    return info


def _optional_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    return int(value)


def _normalise_user(user: dict[str, Any]) -> dict[str, Any]:
    """Expose the identifying fields of a UserInfo entry; the raw entry is kept for other attributes."""
    valid = user.get("Valid") if isinstance(user.get("Valid"), dict) else {}
    enabled = valid.get("enable", user.get("enable"))
    return {
        "employeeNo": str(user.get("employeeNo", "")).strip(),
        "name": user.get("name"),
        "userType": user.get("userType"),
        "enabled": enabled if isinstance(enabled, bool) or enabled is None else str(enabled).lower() == "true",
        "validBeginTime": valid.get("beginTime"),
        "validEndTime": valid.get("endTime"),
        "raw": user,
    }


def _clean_error_value(value: Any) -> str:
    return " ".join(str(value).split())[:200]


def _safe_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class AuthFailure:
    """The lockout-relevant part of a Hikvision HTTP 401 body; never contains credentials."""
    locked: bool = False
    unlock_seconds: int | None = None  # device-reported remaining lock time, only when plausible
    retries_left: int | None = None  # retryLoginTime: login attempts left before the device locks
    status: str = ""


def _collect_auth_fields(node: Any, fields: dict[str, str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in _AUTH_FIELDS and not isinstance(value, (dict, list)):
                fields.setdefault(key, str(value).strip())
            else:
                _collect_auth_fields(value, fields)
    elif isinstance(node, list):
        for item in node:
            _collect_auth_fields(item, fields)


def parse_auth_failure(body: bytes | str | None) -> AuthFailure:
    """Parse a Hikvision 401 body (XML <userCheck>/<ResponseStatus>, or JSON); unknown shapes are ordinary 401s."""
    raw = (body.encode("utf-8") if isinstance(body, str) else body or b"").lstrip(_LEADING_JUNK)
    fields: dict[str, str] = {}
    if raw.startswith(b"{"):
        try:
            _collect_auth_fields(json.loads(raw), fields)
        except (ValueError, TypeError):
            pass
    elif raw.startswith(b"<") and b"<!DOCTYPE" not in raw and b"<!ENTITY" not in raw:
        try:
            for element in ET.fromstring(raw).iter():
                name = _local_name(element.tag)
                if name in _AUTH_FIELDS and len(element) == 0:
                    fields.setdefault(name, (element.text or "").strip())
        except ET.ParseError:
            pass
    unlock = _safe_int(fields.get("unlockTime"))
    return AuthFailure(
        locked=fields.get("lockStatus", "").lower() == "lock",
        unlock_seconds=unlock if unlock is not None and 0 < unlock <= MAX_LOCK_SECONDS else None,
        retries_left=_safe_int(fields.get("retryLoginTime")),
        status=_clean_error_value(fields.get("statusString", "")),
    )


def _describe_http_error(method: str, path: str, response: Any) -> str:
    """Summarise a failed ISAPI call using only whitelisted ResponseStatus fields (never headers)."""
    parts = [f"{method} {path}", f"HTTP {getattr(response, 'status_code', '?')}"]
    fields: dict[str, Any] = {}
    body = getattr(response, "content", b"") or b""
    try:
        payload = json.loads(body)
        if isinstance(payload, dict):
            status = payload.get("ResponseStatus", payload)
            fields = status if isinstance(status, dict) else {}
    except (ValueError, TypeError):
        try:
            root = ET.fromstring(body.lstrip(_LEADING_JUNK)) if b"<!DOCTYPE" not in body else None
            if root is not None:
                fields = {_local_name(child.tag): (child.text or "").strip() for child in root}
        except ET.ParseError:
            fields = {}
    for name in _ERROR_FIELDS:
        if fields.get(name) not in (None, ""):
            parts.append(f"{name}={_clean_error_value(fields[name])}")
    return " ".join(parts)


class HikvisionClient:
    def __init__(self, config: AgentConfig, session: requests.Session | None = None,
                 session_factory: Callable[[], requests.Session] | None = None,
                 clock: Callable[[], float] = time.monotonic):
        self.config = config
        # An injected session (tests) is reused on recovery unless a factory is also supplied.
        self._session_factory = session_factory or ((lambda: session) if session is not None else requests.Session)
        # One lock serialises requests and session replacement, so a swap never races an in-flight request.
        self._lock = threading.RLock()
        self._clock = clock
        # While clock() < _cooldown_until no request of any kind is sent to the device.
        self._cooldown_until = 0.0
        self._cooldown_kind = ""
        self._fresh_session_due = False  # the first request after a cooldown uses a brand-new session
        self._auth_failures = 0
        # Optional hook(seconds, kind) so the agent can persist a cooldown across restarts.
        self.on_cooldown: Callable[[float, str], None] | None = None
        self.session = self._new_session()

    def _new_session(self) -> requests.Session:
        """A Session whose HTTPDigestAuth holds the nonce/nc state reused across requests."""
        session = self._session_factory()
        session.auth = HTTPDigestAuth(self.config.hikvision_username, self.config.hikvision_password)
        return session

    def reset_session(self) -> None:
        """Drop cached Digest nonce state and pooled connections, then start a fresh session."""
        with self._lock:
            old = self.session
            self.session = self._new_session()
            if old is not self.session:
                try:
                    old.close()
                except Exception:  # closing a dead pool must never break recovery
                    pass

    def close(self) -> None:
        with self._lock:
            try:
                self.session.close()
            except Exception:
                pass

    def cooldown_remaining(self) -> float:
        return max(0.0, self._cooldown_until - self._clock())

    def pause(self, seconds: float, kind: str = "auth") -> None:
        """Send no device request for ``seconds`` (e.g. restoring a lockout recorded before a restart)."""
        with self._lock:
            self._start_cooldown(seconds, kind)

    def _start_cooldown(self, seconds: float, kind: str) -> None:
        self._cooldown_until = max(self._cooldown_until, self._clock() + seconds)
        self._cooldown_kind = kind
        self._fresh_session_due = True

    def _check_cooldown(self) -> bool:
        """Raise while paused; return True when this is the first request after a cooldown ended."""
        remaining = self.cooldown_remaining()
        if remaining > 0:
            error = _COOLDOWN_ERRORS.get(self._cooldown_kind, DeviceAuthenticationError)
            reason = "account locked" if error is DeviceLockedError else "authentication cooldown"
            LOG.debug("Hikvision request skipped: %s for approximately %s more seconds.", reason, round(remaining))
            raise error(f"Hikvision requests paused ({reason}) for approximately {round(remaining)} more seconds.",
                        remaining)
        if not self._fresh_session_due:
            return False
        self._fresh_session_due = False
        LOG.info("Hikvision cooldown finished; trying one request with a fresh authentication session.")
        self.reset_session()
        return True

    def _enter_cooldown(self, error: type[DeviceCooldownError], seconds: float, message: str) -> DeviceCooldownError:
        self._start_cooldown(seconds, error.kind)
        LOG.warning("%s", message)
        if self.on_cooldown is not None:
            try:
                self.on_cooldown(seconds, error.kind)
            except Exception:  # persisting the cooldown is best effort; the in-memory pause still holds
                LOG.debug("Could not record the Hikvision cooldown.", exc_info=True)
        return error(message, seconds)

    def _lock_cooldown(self, failure: AuthFailure) -> DeviceCooldownError:
        base = failure.unlock_seconds if failure.unlock_seconds is not None else self.config.hikvision_lock_default_seconds
        seconds = base + self.config.hikvision_lock_margin_seconds
        return self._enter_cooldown(
            DeviceLockedError, seconds,
            f"Hikvision account locked; pausing device requests for approximately {round(seconds)} seconds.")

    def _auth_cooldown(self, method: str, path: str, response: requests.Response) -> DeviceCooldownError:
        self._auth_failures += 1
        base = self.config.hikvision_auth_cooldown_seconds
        seconds = min(base * 2 ** min(self._auth_failures - 1, 16), max(base, MAX_AUTH_COOLDOWN_SECONDS))
        detail = _describe_http_error(method, path, response)
        return self._enter_cooldown(
            DeviceAuthenticationError, seconds,
            f"Hikvision authentication failed ({detail}); check the configured Hikvision username/password. "
            f"Pausing device requests for approximately {round(seconds)} seconds.")

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        with self._lock:
            after_cooldown = self._check_cooldown()
            response = self._send(method, path, **kwargs)
            if response.status_code == 401:
                failure = parse_auth_failure(getattr(response, "content", b""))
                # requests already answered the Digest challenge once; a final 401 usually means the cached
                # nonce went stale between polls. Retry exactly once with fresh auth state, but never while the
                # account is locked, when the session is already fresh after a cooldown, or when the device
                # says one more failed login would lock it.
                if not failure.locked and not after_cooldown and (failure.retries_left is None
                                                                  or failure.retries_left > 1):
                    LOG.warning("Hikvision %s %s returned HTTP 401 after authentication negotiation; "
                                "recreating the session and retrying once.", method, path)
                    response.close()
                    self.reset_session()
                    response = self._send(method, path, **kwargs)
                    if response.status_code == 401:
                        failure = parse_auth_failure(getattr(response, "content", b""))
                    elif response.status_code < 400:
                        LOG.info("Hikvision %s %s succeeded after authentication session reset.", method, path)
                if response.status_code == 401:
                    error = self._lock_cooldown(failure) if failure.locked else self._auth_cooldown(method, path, response)
                    response.close()
                    raise error
            if response.status_code < 400:
                if after_cooldown:
                    LOG.info("Hikvision authentication succeeded after the cooldown.")
                self._auth_failures = 0
            return self._check(method, path, response)

    def _send(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        try:
            return self.session.request(
                method, f"{self.config.hikvision_base_url}{path}", timeout=self.config.device_timeout_seconds,
                verify=self.config.hikvision_verify_tls, **kwargs,
            )
        except requests.RequestException as exc:
            LOG.warning("Hikvision %s %s failed: %s", method, path, exc.__class__.__name__)
            raise DeviceError(f"Device request failed: {method} {path}: {exc.__class__.__name__}") from exc

    def _check(self, method: str, path: str, response: requests.Response) -> requests.Response:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = _describe_http_error(method, path, response)
            LOG.warning("Hikvision request failed: %s", detail)
            raise DeviceError(f"Device request failed: {detail}") from exc
        return response

    def device_info(self) -> dict[str, Any]:
        response = self._request("GET", "/ISAPI/System/deviceInfo",
                                 headers={"Accept": "application/json, application/xml;q=0.9"})
        body = response.content or b""
        if _looks_like_xml(body, response.headers.get("Content-Type", "")):
            return parse_device_info_xml(body)
        try:
            return response.json()
        except ValueError as exc:
            raise DeviceError("Device returned invalid deviceInfo JSON.") from exc

    get_device_info = device_info

    def test_connection(self) -> dict[str, Any]:
        return self.device_info()

    def search_events_page(self, position: int = 0, search_id: str | None = None,
                           max_results: int | None = None) -> dict[str, Any]:
        """Fetch one AcsEvent page; raw InfoList entries are returned untouched (timestamps included)."""
        condition = {"searchID": str(search_id or ACS_EVENT_SEARCH_ID), "searchResultPosition": int(position),
                     "maxResults": int(max_results or self.config.event_page_size),
                     "major": int(self.config.event_major), "minor": int(self.config.event_minor)}
        # Serialised here so the logged body is byte-for-byte what the device receives.
        body = json.dumps({"AcsEventCond": condition})
        LOG.debug("Hikvision event discovery: searchID=%s searchResultPosition=%s maxResults=%s major=%s minor=%s",
                 condition["searchID"], condition["searchResultPosition"], condition["maxResults"],
                 condition["major"], condition["minor"])
        LOG.debug("Hikvision AcsEventCond request body: %s", body)
        response = self._request("POST", ACS_EVENT_PATH, data=body.encode("utf-8"),
                                 headers={"Accept": "application/json", "Content-Type": "application/json"})
        try:
            payload = response.json()
            result = payload.get("AcsEvent", payload)
            events = result.get("InfoList", result.get("infoList", []))
            if events is None:
                events = []
            if isinstance(events, dict):  # single event returned as an object
                events = [events]
            if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
                raise ValueError
            status = str(result.get("responseStatusStrg", "")).upper()
            return {
                "events": events,
                "numOfMatches": _optional_int(result.get("numOfMatches"), default=len(events)),
                "totalMatches": _optional_int(result.get("totalMatches")),
                "responseStatusStrg": status,
                "more": bool(result.get("more", False)) or status == "MORE",
            }
        except (ValueError, TypeError, AttributeError) as exc:
            raise DeviceError("Device returned an invalid AcsEvent response.") from exc

    def search_events(self, position: int) -> tuple[list[dict[str, Any]], bool]:
        """Backward-compatible single page lookup returning (raw events, more)."""
        page = self.search_events_page(position)
        more = page["more"]
        if page["totalMatches"] is not None:
            more = position + page["numOfMatches"] < page["totalMatches"]
        return page["events"], more

    def event_capabilities(self) -> dict[str, Any]:
        """Read-only diagnostic: which AcsEventCond fields (e.g. startTime/endTime) this firmware supports."""
        response = self._request("GET", "/ISAPI/AccessControl/AcsEvent/capabilities?format=json",
                                 headers={"Accept": "application/json"})
        try:
            return response.json()
        except ValueError as exc:
            raise DeviceError("Device returned invalid AcsEvent capabilities JSON.") from exc

    def get_events(self, position: int = 0) -> tuple[list[dict[str, Any]], bool]:
        return self.search_events(position)

    def search_users_page(self, position: int = 0, search_id: str | None = None,
                          max_results: int = USER_SEARCH_MAX_RESULTS) -> dict[str, Any]:
        """Fetch one UserInfoSearch page and return users plus the device's pagination counters."""
        condition = {"searchID": str(search_id or USER_SEARCH_ID), "searchResultPosition": int(position),
                     "maxResults": max(1, min(int(max_results), USER_SEARCH_MAX_RESULTS))}
        # Serialised here so the logged body is byte-for-byte what the device receives.
        body = json.dumps({"UserInfoSearchCond": condition})
        LOG.info("Hikvision user discovery: searchID=%s searchResultPosition=%s maxResults=%s",
                 condition["searchID"], condition["searchResultPosition"], condition["maxResults"])
        LOG.info("Hikvision UserInfoSearch request body: %s", body)
        response = self._request("POST", USER_SEARCH_PATH, data=body.encode("utf-8"),
                                 headers={"Accept": "application/json", "Content-Type": "application/json"})
        try:
            payload = response.json()
            result = payload.get("UserInfoSearch", payload)
            users = result.get("UserInfo", [])
            if users is None:
                users = []
            if isinstance(users, dict):  # some firmware returns a single object instead of a list
                users = [users]
            if not isinstance(users, list) or not all(isinstance(user, dict) for user in users):
                raise ValueError
            status = str(result.get("responseStatusStrg", "")).upper()
            return {
                "users": [_normalise_user(user) for user in users],
                "numOfMatches": _optional_int(result.get("numOfMatches"), default=len(users)),
                "totalMatches": _optional_int(result.get("totalMatches")),
                "responseStatusStrg": status,
                "more": bool(result.get("more", False)) or status == "MORE",
            }
        except (ValueError, TypeError, AttributeError) as exc:
            raise DeviceError("Device returned an invalid UserInfoSearch response.") from exc

    def search_users(self, position: int = 0,
                     page_size: int = USER_SEARCH_MAX_RESULTS) -> tuple[list[dict[str, Any]], bool]:
        """Backward-compatible single page lookup returning (users, more)."""
        page = self.search_users_page(position, max_results=page_size)
        more = page["more"]
        if page["totalMatches"] is not None:
            more = position + len(page["users"]) < page["totalMatches"]
        return page["users"], more

    def discover_users(self) -> dict[str, Any]:
        """Page through every device user, 10 at a time, reusing one searchID for the whole operation."""
        search_id = USER_SEARCH_ID
        users: list[dict[str, Any]] = []
        seen: set[str] = set()
        position = pages = 0
        total_matches = None
        complete = False
        while pages < USER_SEARCH_MAX_PAGES:
            page = self.search_users_page(position, search_id=search_id)
            pages += 1
            if page["totalMatches"] is not None:
                total_matches = page["totalMatches"]
            if page["numOfMatches"] <= 0 or not page["users"]:
                complete = True
                break
            new_users = [user for user in page["users"] if user["employeeNo"] not in seen]
            if not new_users:  # device repeated a page; stop rather than loop
                LOG.warning("Hikvision user search repeated results at position %s; stopping.", position)
                break
            for user in new_users:
                seen.add(user["employeeNo"])
            users.extend(new_users)
            position += page["numOfMatches"]
            if total_matches is not None:
                if position >= total_matches:
                    complete = True
                    break
            elif not page["more"]:
                complete = True
                break
        else:
            LOG.warning("Hikvision user search stopped at the %s page safety limit.", USER_SEARCH_MAX_PAGES)
        LOG.info("Discovered %s device users in %s page(s).", len(users), pages)
        return {"users": users, "count": len(users), "totalMatches": total_matches, "pages": pages,
                "complete": complete}
