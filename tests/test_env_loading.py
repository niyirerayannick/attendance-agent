import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from attendance_agent.config import AgentConfig, ConfigurationError, default_data_dir, load_project_env
except ModuleNotFoundError:
    from config import AgentConfig, ConfigurationError, default_data_dir, load_project_env

SOURCE_DIR = Path(__file__).resolve().parents[1]
CONFIG_KEYS = ("HIKVISION_HOST", "HIKVISION_SCHEME", "HIKVISION_USERNAME", "HIKVISION_PASSWORD",
               "HIKVISION_VERIFY_TLS", "EPCA_API_URL", "EPCA_DEVICE_CODE", "EPCA_DEVICE_TOKEN",
               "POLL_INTERVAL_SECONDS", "AGENT_DATA_DIR", "AGENT_DATABASE_PATH", "DEVICE_TIMEOUT_SECONDS")
# Obviously fake values; the tests assert they never appear in logs or output.
FAKE_PASSWORD = "fake-hik-password-7f3a"
FAKE_TOKEN = "fake-epca-token-91c2"


def env_text(**overrides):
    values = {"HIKVISION_HOST": "192.0.2.10", "HIKVISION_USERNAME": "agent-reader",
              "HIKVISION_PASSWORD": FAKE_PASSWORD, "EPCA_API_URL": "https://epca.example/api/internal/attendance/events/",
              "EPCA_DEVICE_CODE": "EPCA-HQ-01", "EPCA_DEVICE_TOKEN": FAKE_TOKEN, "POLL_INTERVAL_SECONDS": "5"}
    values.update(overrides)
    return "".join(f"{key}={value}\n" for key, value in values.items() if value is not None)


class EnvFileLoadingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        # Isolate from the developer's real environment and any real .env values.
        self.env = patch.dict("os.environ", {k: v for k, v in os.environ.items() if k not in CONFIG_KEYS}, clear=True)
        self.env.start()

    def tearDown(self):
        self.env.stop(); self.tmp.cleanup()

    def write_env(self, text):
        path = self.root / ".env"
        path.write_text(text, encoding="utf-8")
        return path

    def test_project_env_file_is_loaded(self):
        self.assertTrue(load_project_env(self.write_env(env_text(AGENT_DATA_DIR=str(self.root / "data")))))
        config = AgentConfig.from_environment()
        self.assertEqual(config.epca_api_url, "https://epca.example/api/internal/attendance/events/")
        self.assertEqual(config.hikvision_password, FAKE_PASSWORD)
        self.assertEqual(config.poll_interval_seconds, 5)

    def test_os_environment_overrides_env_file(self):
        os.environ["POLL_INTERVAL_SECONDS"] = "42"
        os.environ["HIKVISION_HOST"] = "198.51.100.7"
        load_project_env(self.write_env(env_text()))
        config = AgentConfig.from_environment()
        self.assertEqual(config.poll_interval_seconds, 42)
        self.assertEqual(config.hikvision_host, "198.51.100.7")
        self.assertEqual(config.epca_device_code, "EPCA-HQ-01")  # still filled from .env

    def test_missing_env_file_is_not_an_error(self):
        self.assertFalse(load_project_env(self.root / "missing" / ".env"))

    def test_missing_required_setting_still_fails_clearly(self):
        load_project_env(self.write_env(env_text(EPCA_API_URL=None)))
        with self.assertRaises(ConfigurationError) as ctx:
            AgentConfig.from_environment()
        self.assertEqual(str(ctx.exception), "EPCA_API_URL is required.")

    def test_loading_never_logs_secrets(self):
        with self.assertLogs("epca_attendance_agent.config", level="DEBUG") as logs:
            load_project_env(self.write_env(env_text()))
        output = "\n".join(logs.output)
        self.assertIn(".env", output)
        for secret in (FAKE_PASSWORD, FAKE_TOKEN, "HIKVISION_PASSWORD", "EPCA_DEVICE_TOKEN"):
            self.assertNotIn(secret, output)

    @unittest.skipUnless(os.name == "nt", "Windows drive-letter paths")
    def test_windows_data_dir_path(self):
        load_project_env(self.write_env(env_text(AGENT_DATA_DIR=r"C:\EPCA\attendance-agent\data")))
        config = AgentConfig.from_environment()
        self.assertEqual(Path(config.database_path), Path(r"C:\EPCA\attendance-agent\data\attendance_agent.db"))

    def test_default_data_dir_does_not_assume_slash_data_on_windows(self):
        app_dir = self.root / "app"
        load_project_env(self.write_env(env_text()))
        config = AgentConfig.from_environment(app_dir=app_dir)
        if os.name == "nt":
            self.assertEqual(default_data_dir(app_dir), app_dir / "data")
            self.assertEqual(Path(config.database_path), app_dir / "data" / "attendance_agent.db")
        else:
            self.assertEqual(Path(config.database_path), Path("/data/attendance_agent.db"))


class LaunchFromOtherDirectoryTests(unittest.TestCase):
    """Runs a copy of agent.py exactly as `python <path>\\agent.py test-device` from an unrelated directory."""

    def test_agent_finds_project_env_from_another_working_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            project, elsewhere = Path(tmp) / "project", Path(tmp) / "elsewhere"
            project.mkdir(); elsewhere.mkdir()
            for name in ("agent.py", "config.py", "hikvision.py", "cloud.py", "storage.py"):
                shutil.copy(SOURCE_DIR / name, project / name)
            # Port 9 on loopback refuses fast: the run proves config loaded, then fails at the device.
            (project / ".env").write_text(env_text(HIKVISION_HOST="127.0.0.1:9", DEVICE_TIMEOUT_SECONDS="3",
                                                   AGENT_DATA_DIR=str(project / "data")), encoding="utf-8")
            # A decoy .env in the working directory must be ignored.
            (elsewhere / ".env").write_text(env_text(EPCA_API_URL="http://wrong.example/"), encoding="utf-8")
            env = {k: v for k, v in os.environ.items() if k not in CONFIG_KEYS and k != "PYTHONPATH"}
            result = subprocess.run([sys.executable, str(project / "agent.py"), "test-device"], cwd=elsewhere,
                                    env=env, capture_output=True, text=True, timeout=60)
            output = result.stdout + result.stderr
            self.assertNotIn("is required", output)
            self.assertNotIn("must be an HTTPS URL", output)  # decoy was not used
            self.assertIn(str(project / ".env"), output)
            self.assertIn("Device request failed", output)
            self.assertEqual(result.returncode, 2)
            for secret in (FAKE_PASSWORD, FAKE_TOKEN, "Authorization", "Digest "):
                self.assertNotIn(secret, output)
            self.assertTrue((project / "data" / "attendance_agent.db").exists())


if __name__ == "__main__":
    unittest.main()
