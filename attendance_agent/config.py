"""Configuration loaded from the process environment, optionally seeded from the project .env file."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

LOG = logging.getLogger("epca_attendance_agent.config")

# Fallback project directory, resolved from __file__ and never from the working directory, so a Windows
# Service (working directory usually C:\Windows\System32) still finds it. agent.py passes its own
# directory explicitly, because `python agent.py` may import this module from the nested package copy.
PROJECT_ROOT = Path(__file__).resolve().parent


class ConfigurationError(ValueError):
    pass


MAX_POLL_INTERVAL_SECONDS = 86_400


def load_project_env(env_file: str | os.PathLike[str] | None = None) -> bool:
    """Load KEY=VALUE pairs from the project .env into os.environ without overriding real env vars.

    Returns True when a file was loaded. Only the file path is logged, never keys or values.
    """
    path = Path(env_file) if env_file is not None else PROJECT_ROOT / ".env"
    if not path.is_file():
        LOG.debug("No .env file at %s; using the process environment only.", path)
        return False
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError as exc:
        raise ConfigurationError("python-dotenv is required to read .env; run: pip install -r requirements.txt") from exc
    load_dotenv(dotenv_path=path, override=False)  # OS environment variables always win
    LOG.info("Loaded configuration defaults from %s.", path)
    return True


def default_data_dir(app_dir: Path | None = None) -> Path:
    """/data inside the Linux container; <app_dir>\\data when running natively on Windows."""
    return (app_dir or PROJECT_ROOT) / "data" if os.name == "nt" else Path("/data")


def _path_setting(name: str) -> Path | None:
    value = os.getenv(name, "").strip().strip('"')
    if not value:
        return None
    path = Path(os.path.expandvars(value)).expanduser()
    if os.name == "nt" and path.root and not path.drive:
        LOG.warning("%s=%s has no drive letter; on Windows it resolves to %s. Use a full path such as "
                    r"C:\EPCA\attendance-agent\data.", name, value, path.resolve())
    return path


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


def _non_negative_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer.") from exc
    if value < 0:
        raise ConfigurationError(f"{name} must be zero or greater.")
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
    # DS-K1T8003MF V1.3.37 is proven with maxResults=10, major=5, minor=38 (fingerprint verified).
    event_page_size: int = 10
    max_pages_per_poll: int = 100
    event_major: int = 5
    event_minor: int = 38
    # First start with an empty database only queues this recent window; older device history is skipped.
    initial_sync_lookback_hours: int = 24
    initial_sync_max_events: int = 1000
    initial_sync_full_history: bool = False
    # Failure backoff is independent of the polling interval: 5s, 10s, 20s ... capped at retry_max_seconds.
    retry_initial_seconds: int = 5
    retry_max_seconds: int = 300

    def __post_init__(self) -> None:
        if not 1 <= self.poll_interval_seconds <= MAX_POLL_INTERVAL_SECONDS:
            raise ConfigurationError(f"POLL_INTERVAL_SECONDS must be between 1 and {MAX_POLL_INTERVAL_SECONDS}.")
        if self.retry_initial_seconds < 1 or self.retry_max_seconds < self.retry_initial_seconds:
            raise ConfigurationError("RETRY_MAX_SECONDS must be >= RETRY_INITIAL_SECONDS >= 1.")

    @property
    def hikvision_base_url(self) -> str:
        return f"{self.hikvision_scheme}://{self.hikvision_host}".rstrip("/")

    @classmethod
    def from_environment(cls, app_dir: Path | None = None) -> "AgentConfig":
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
            database_path=str(_path_setting("AGENT_DATABASE_PATH")
                              or (_path_setting("AGENT_DATA_DIR") or default_data_dir(app_dir)) / "attendance_agent.db"),
            hikvision_scheme=scheme,
            hikvision_verify_tls=_boolean("HIKVISION_VERIFY_TLS", True),
            device_timeout_seconds=_positive_int("DEVICE_TIMEOUT_SECONDS", 15),
            cloud_timeout_seconds=_positive_int("CLOUD_TIMEOUT_SECONDS", 20),
            batch_size=_positive_int("BATCH_SIZE", 100),
            event_page_size=_positive_int("EVENT_PAGE_SIZE", 10),
            max_pages_per_poll=_positive_int("MAX_PAGES_PER_POLL", 100),
            event_major=_non_negative_int("HIKVISION_EVENT_MAJOR", 5),
            event_minor=_non_negative_int("HIKVISION_EVENT_MINOR", 38),
            initial_sync_lookback_hours=_non_negative_int("INITIAL_SYNC_LOOKBACK_HOURS", 24),
            initial_sync_max_events=_positive_int("INITIAL_SYNC_MAX_EVENTS", 1000),
            initial_sync_full_history=_boolean("INITIAL_SYNC_FULL_HISTORY", False),
            retry_initial_seconds=_positive_int("RETRY_INITIAL_SECONDS", 5),
            retry_max_seconds=_positive_int("RETRY_MAX_SECONDS", 300),
        )
