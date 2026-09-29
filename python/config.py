"""Load and validate Mốc 2 settings from the project .env file."""

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    mqtt_host: str
    mqtt_port: int
    mqtt_topic: str
    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str
    influxdb_measurement: str


def _setting(primary: str, legacy: str | None = None) -> str:
    value = os.getenv(primary, "").strip()
    if not value and legacy:
        value = os.getenv(legacy, "").strip()
    return value


def load_settings() -> Settings:
    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise RuntimeError(
            "Missing python-dotenv. Run: pip install -r python/requirements.txt"
        ) from exc

    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    values = {
        "MQTT_HOST": _setting("MQTT_HOST"),
        "MQTT_PORT": _setting("MQTT_PORT"),
        "MQTT_TOPIC": _setting("MQTT_TOPIC"),
        "INFLUX_URL": _setting("INFLUX_URL", "INFLUXDB_URL"),
        "INFLUX_TOKEN": _setting("INFLUX_TOKEN", "INFLUXDB_TOKEN"),
        "INFLUX_ORG": _setting("INFLUX_ORG", "INFLUXDB_ORG"),
        "INFLUX_BUCKET": _setting("INFLUX_BUCKET", "INFLUXDB_BUCKET"),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ValueError("Missing configuration: " + ", ".join(missing))

    try:
        mqtt_port = int(values["MQTT_PORT"])
    except ValueError as exc:
        raise ValueError("MQTT_PORT must be an integer") from exc
    if not 1 <= mqtt_port <= 65535:
        raise ValueError("MQTT_PORT must be between 1 and 65535")

    influx_url = values["INFLUX_URL"].rstrip("/")
    if not influx_url.startswith("https://"):
        raise ValueError("INFLUX_URL must use https:// for InfluxDB Cloud")

    return Settings(
        mqtt_host=values["MQTT_HOST"],
        mqtt_port=mqtt_port,
        mqtt_topic=values["MQTT_TOPIC"],
        influxdb_url=influx_url,
        influxdb_token=values["INFLUX_TOKEN"],
        influxdb_org=values["INFLUX_ORG"],
        influxdb_bucket=values["INFLUX_BUCKET"],
        influxdb_measurement=_setting(
            "INFLUX_MEASUREMENT", "INFLUXDB_MEASUREMENT"
        ) or "sensor_raw",
    )
