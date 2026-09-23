"""Small HTTP Digest client for Hikvision ISAPI attendance endpoints."""

from __future__ import annotations

import uuid
from typing import Any

import requests
from requests.auth import HTTPDigestAuth

try:
    from attendance_agent.config import AgentConfig
except ModuleNotFoundError:
    from config import AgentConfig


class DeviceError(RuntimeError):
    pass


class HikvisionClient:
    def __init__(self, config: AgentConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()
        self.session.auth = HTTPDigestAuth(config.hikvision_username, config.hikvision_password)

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        try:
            response = self.session.request(
                method, f"{self.config.hikvision_base_url}{path}", timeout=self.config.device_timeout_seconds,
                verify=self.config.hikvision_verify_tls, **kwargs,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            raise DeviceError(f"Device request failed: {exc.__class__.__name__}") from exc

    def device_info(self) -> dict[str, Any]:
        response = self._request("GET", "/ISAPI/System/deviceInfo", headers={"Accept": "application/json"})
        try:
            return response.json()
        except ValueError as exc:
            raise DeviceError("Device returned invalid deviceInfo JSON.") from exc

    get_device_info = device_info

    def test_connection(self) -> dict[str, Any]:
        return self.device_info()

    def search_events(self, position: int) -> tuple[list[dict[str, Any]], bool]:
        body = {"AcsEventCond": {"searchID": str(uuid.uuid4()), "searchResultPosition": position,
                                 "maxResults": self.config.event_page_size}}
        response = self._request("POST", "/ISAPI/AccessControl/AcsEvent?format=json", json=body,
                                 headers={"Accept": "application/json"})
        try:
            payload = response.json()
            result = payload.get("AcsEvent", payload)
            events = result.get("InfoList", result.get("infoList", []))
            if not isinstance(events, list):
                raise ValueError
            more = bool(result.get("more", False)) or str(result.get("responseStatusStrg", "")).upper() == "MORE"
            return events, more
        except (ValueError, AttributeError) as exc:
            raise DeviceError("Device returned an invalid AcsEvent response.") from exc

    def get_events(self, position: int = 0) -> tuple[list[dict[str, Any]], bool]:
        return self.search_events(position)

    def search_users(self, position: int = 0, page_size: int = 100) -> tuple[list[dict[str, Any]], bool]:
        body = {"UserInfoSearchCond": {"searchID": str(uuid.uuid4()), "searchResultPosition": position,
                                        "maxResults": page_size}}
        response = self._request("POST", "/ISAPI/AccessControl/UserInfo/Search?format=json", json=body,
                                 headers={"Accept": "application/json"})
        try:
            result = response.json().get("UserInfoSearch", {})
            users = result.get("UserInfo", [])
            return users if isinstance(users, list) else [], bool(result.get("more", False))
        except (ValueError, AttributeError) as exc:
            raise DeviceError("Device returned an invalid UserInfoSearch response.") from exc
