"""HTTPS client for the narrowly scoped EPCA ONE attendance endpoint."""

from __future__ import annotations

from typing import Any

import requests

try:
    from attendance_agent.config import AgentConfig
except ModuleNotFoundError:
    from config import AgentConfig


class CloudError(RuntimeError):
    pass


class CloudAuthenticationError(CloudError):
    pass


class EpcClient:
    def __init__(self, config: AgentConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    def _post(self, payload: dict[str, Any]) -> requests.Response:
        try:
            return self.session.post(self.config.epca_api_url, json=payload, verify=True,
                                     timeout=self.config.cloud_timeout_seconds,
                                     headers={"Authorization": f"Bearer {self.config.epca_device_token}", "Accept": "application/json"})
        except requests.RequestException as exc:
            raise CloudError(f"Cloud request failed: {exc.__class__.__name__}") from exc

    def test_connection(self) -> None:
        response = self._post({"device_code": self.config.epca_device_code, "events": []})
        if response.status_code in {401, 403}:
            raise CloudAuthenticationError("Cloud rejected the configured device credentials.")
        if response.status_code != 400:
            raise CloudError(f"Unexpected cloud test response: HTTP {response.status_code}.")

    def send_events(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        payload = {"device_code": self.config.epca_device_code, "events": [
            {"employee_no": event["employee_no"], "event_time": event["event_time"], "serial_no": event["serial_no"],
             "major": event["major"], "minor": event["minor"], "attendance_status": event["attendance_status"],
             "verification_method": event["verification_method"], "raw_payload": event["raw_payload"]}
            for event in events
        ]}
        response = self._post(payload)
        if response.status_code in {401, 403}:
            raise CloudAuthenticationError("Cloud rejected the configured device credentials.")
        if response.status_code >= 500 or response.status_code == 429:
            raise CloudError(f"Cloud temporarily unavailable: HTTP {response.status_code}.")
        if response.status_code >= 400:
            raise CloudError(f"Cloud rejected the batch: HTTP {response.status_code}.")
        try:
            return response.json()
        except ValueError as exc:
            raise CloudError("Cloud returned invalid JSON.") from exc
