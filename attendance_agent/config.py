"""Configuration loaded only from the agent process environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


class ConfigurationError(ValueError):
    pass


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"{name} is required.")
    return value


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer.") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero.")
    return value


def _boolean(name: str, default: bool) -> bool:
    value = os.getenv(name, str(default)).lower().strip()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be true or false.")


@dataclass(frozen=True)
class AgentConfig:
    hikvision_host: str
    hikvision_username: str
    hikvision_password: str
    epca_api_url: str
    epca_device_code: str
    epca_device_token: str
    poll_interval_seconds: int
    database_path: str
    hikvision_scheme: str = "http"
    hikvision_verify_tls: bool = True
    device_timeout_seconds: int = 15
    cloud_timeout_seconds: int = 20
    batch_size: int = 100
    event_page_size: int = 100
    max_pages_per_poll: int = 100

    @property
    def hikvision_base_url(self) -> str:
        return f"{self.hikvision_scheme}://{self.hikvision_host}".rstrip("/")

    @classmethod
    def from_environment(cls) -> "AgentConfig":
        scheme = os.getenv("HIKVISION_SCHEME", "http").lower().strip()
        if scheme not in {"http", "https"}:
            raise ConfigurationError("HIKVISION_SCHEME must be http or https.")
        api_url = _required("EPCA_API_URL")
        parsed = urlparse(api_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ConfigurationError("EPCA_API_URL must be an HTTPS URL.")
        return cls(
            hikvision_host=_required("HIKVISION_HOST"),
            hikvision_username=_required("HIKVISION_USERNAME"),
            hikvision_password=_required("HIKVISION_PASSWORD"),
            epca_api_url=api_url,
            epca_device_code=_required("EPCA_DEVICE_CODE").upper(),
            epca_device_token=_required("EPCA_DEVICE_TOKEN"),
            poll_interval_seconds=_positive_int("POLL_INTERVAL_SECONDS", 60),
            database_path=os.getenv(
                "AGENT_DATABASE_PATH",
                str(Path(os.getenv("AGENT_DATA_DIR", "/data")) / "attendance_agent.db"),
            ),
            hikvision_scheme=scheme,
            hikvision_verify_tls=_boolean("HIKVISION_VERIFY_TLS", True),
            device_timeout_seconds=_positive_int("DEVICE_TIMEOUT_SECONDS", 15),
            cloud_timeout_seconds=_positive_int("CLOUD_TIMEOUT_SECONDS", 20),
            batch_size=_positive_int("BATCH_SIZE", 100),
            event_page_size=_positive_int("EVENT_PAGE_SIZE", 100),
            max_pages_per_poll=_positive_int("MAX_PAGES_PER_POLL", 100),
        )
