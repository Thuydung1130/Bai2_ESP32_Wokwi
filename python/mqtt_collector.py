"""Subscribe to ESP32 telemetry, validate it, and write it to InfluxDB 2.x."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import json
import logging
import math
from datetime import datetime, timezone
from queue import Full, Queue
from threading import Event, Thread
import time
from typing import Any, Protocol

from config import Settings, load_settings


LOGGER = logging.getLogger("mqtt_collector")
REQUIRED_FIELDS = {
    "temperature",
    "humidity",
    "distance_cm",
    "rssi",
    "sequence",
    "uptime_s",
    "device_id",
    "timestamp_ms",
}
MIN_UNIX_TIMESTAMP_MS = 1_700_000_000_000
MAX_FUTURE_MS = 60_000
MAX_MESSAGE_AGE_MS = 86_400_000


class ValidationError(ValueError):
    """Raised when an MQTT payload is not valid telemetry."""


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite_number(data: dict[str, Any], name: str, minimum: float, maximum: float) -> float:
    value = data[name]
    if not _is_number(value) or not math.isfinite(value):
        raise ValidationError(f"{name} must be a finite number")
    value = float(value)
    if not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be in range [{minimum}, {maximum}]")
    return value


def _non_negative_integer(data: dict[str, Any], name: str) -> int:
    value = data[name]
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValidationError(f"{name} must be a non-negative integer")
    return value


def utc_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def validate_payload(raw_payload: bytes | str, received_ms: int) -> dict[str, Any]:
    try:
        text = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
    except UnicodeDecodeError as exc:
        raise ValidationError("payload is not valid UTF-8") from exc

    try:
        data = json.loads(text, parse_constant=lambda value: (_ for _ in ()).throw(
            ValidationError(f"non-finite JSON value: {value}")
        ))
    except ValidationError:
        raise
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValidationError("payload is not valid JSON") from exc

    if not isinstance(data, dict):
        raise ValidationError("payload must be a JSON object")
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValidationError("missing fields: " + ", ".join(sorted(missing)))

    device_id = data["device_id"]
    if not isinstance(device_id, str) or not device_id.strip():
        raise ValidationError("device_id must be a non-empty string")
    if len(device_id) > 64:
        raise ValidationError("device_id must not exceed 64 characters")

    validated = {
        "device_id": device_id.strip(),
        "temperature": _finite_number(data, "temperature", -40.0, 80.0),
        "humidity": _finite_number(data, "humidity", 0.0, 100.0),
        "distance_cm": _finite_number(data, "distance_cm", 0.0, 400.0),
        "sequence": _non_negative_integer(data, "sequence"),
        "uptime_s": _non_negative_integer(data, "uptime_s"),
    }

    rssi = data["rssi"]
    if not isinstance(rssi, int) or isinstance(rssi, bool) or not -127 <= rssi <= 20:
        raise ValidationError("rssi must be an integer in range [-127, 20]")
    validated["rssi"] = rssi

    timestamp_ms = data["timestamp_ms"]
    if not isinstance(timestamp_ms, int) or isinstance(timestamp_ms, bool):
        raise ValidationError("timestamp_ms must be an integer")
    if timestamp_ms < MIN_UNIX_TIMESTAMP_MS:
        raise ValidationError("timestamp_ms is not a synchronized Unix timestamp")
    latency_ms = received_ms - timestamp_ms
    if latency_ms < -MAX_FUTURE_MS:
        raise ValidationError("timestamp_ms is too far in the future")
    if latency_ms > MAX_MESSAGE_AGE_MS:
        raise ValidationError("timestamp_ms is older than 24 hours")
    validated["timestamp_ms"] = timestamp_ms
    validated["latency_ms"] = latency_ms
    return validated


@dataclass
class DeviceState:
    sequence: int
    uptime_s: int
    timestamp_ms: int


@dataclass(frozen=True)
class SequenceCheck:
    duplicate: bool = False
    restarted: bool = False
    warning: str | None = None


class DuplicateTracker:
    """Track the last successfully stored point for each device."""

    def __init__(self) -> None:
        self._states: dict[str, DeviceState] = {}
        self._recent_order: deque[tuple[str, int, int]] = deque()
        self._recent_keys: set[tuple[str, int, int]] = set()
        self._recent_limit = 1000

    def inspect(self, data: dict[str, Any]) -> SequenceCheck:
        key = (data["device_id"], data["sequence"], data["timestamp_ms"])
        if key in self._recent_keys:
            return SequenceCheck(duplicate=True)
        previous = self._states.get(data["device_id"])
        if previous is None:
            return SequenceCheck()
        if data["sequence"] < previous.sequence and data["uptime_s"] < previous.uptime_s:
            return SequenceCheck(restarted=True, warning="device restart detected")
        if data["sequence"] > previous.sequence + 1:
            missed = data["sequence"] - previous.sequence - 1
            return SequenceCheck(warning=f"sequence gap: possibly missed {missed} message(s)")
        if data["sequence"] <= previous.sequence:
            return SequenceCheck(warning="out-of-order or reused sequence detected")
        return SequenceCheck()

    def commit(self, data: dict[str, Any]) -> None:
        key = (data["device_id"], data["sequence"], data["timestamp_ms"])
        if len(self._recent_order) >= self._recent_limit:
            self._recent_keys.discard(self._recent_order.popleft())
        self._recent_order.append(key)
        self._recent_keys.add(key)
        self._states[data["device_id"]] = DeviceState(
            sequence=data["sequence"],
            uptime_s=data["uptime_s"],
            timestamp_ms=data["timestamp_ms"],
        )


class Store(Protocol):
    def write(self, data: dict[str, Any]) -> None: ...
    def close(self) -> None: ...


class InfluxStore:
    def __init__(self, settings: Settings) -> None:
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
        except ImportError as exc:
            raise RuntimeError(
                "Missing influxdb-client. Run: pip install -r python/requirements.txt"
            ) from exc

        self._settings = settings
        self._client = InfluxDBClient(
            url=settings.influxdb_url,
            token=settings.influxdb_token,
            org=settings.influxdb_org,
            timeout=5_000,
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def verify_connection(self) -> None:
        if not self._client.ping():
            raise ConnectionError("InfluxDB ping failed")

    def write(self, data: dict[str, Any]) -> None:
        from influxdb_client import Point, WritePrecision

        point = (
            Point(self._settings.influxdb_measurement)
            .tag("device_id", data["device_id"])
            .field("temperature", float(data["temperature"]))
            .field("humidity", float(data["humidity"]))
            .field("distance_cm", float(data["distance_cm"]))
            .field("rssi", int(data["rssi"]))
            .field("sequence", int(data["sequence"]))
            .field("uptime_s", int(data["uptime_s"]))
            .field("latency_ms", int(data["latency_ms"]))
            .time(data["timestamp_ms"], WritePrecision.MS)
        )
        self._write_api.write(
            bucket=self._settings.influxdb_bucket,
            org=self._settings.influxdb_org,
            record=point,
        )

    def close(self) -> None:
        self._client.close()


class TelemetryCollector:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.tracker = DuplicateTracker()

    def process_payload(self, payload: bytes | str, received_ms: int | None = None) -> bool:
        if received_ms is None:
            received_ms = time.time_ns() // 1_000_000
        try:
            data = validate_payload(payload, received_ms)
        except ValidationError as exc:
            LOGGER.warning("Rejected MQTT message: %s", exc)
            return False

        LOGGER.info(
            "Time diagnostic: device_timestamp_ms=%d python_received_ms=%d "
            "latency_ms=%d device_utc=%s python_received_utc=%s",
            data["timestamp_ms"], received_ms, data["latency_ms"],
            utc_from_ms(data["timestamp_ms"]), utc_from_ms(received_ms),
        )

        check = self.tracker.inspect(data)
        if check.duplicate:
            LOGGER.warning(
                "Duplicate skipped: device_id=%s sequence=%d timestamp_ms=%d",
                data["device_id"], data["sequence"], data["timestamp_ms"],
            )
            return False
        if check.warning:
            LOGGER.warning("%s: device_id=%s sequence=%d", check.warning,
                           data["device_id"], data["sequence"])

        try:
            self.store.write(data)
        except Exception as exc:  # Client exceptions differ by connection/auth failure.
            LOGGER.error(
                "InfluxDB write failed; record was not marked as stored: %s", exc
            )
            return False

        self.tracker.commit(data)
        LOGGER.info(
            "Stored: device_id=%s temperature=%.2f humidity=%.2f "
            "distance_cm=%.2f latency_ms=%d sequence=%d",
            data["device_id"], data["temperature"], data["humidity"],
            data["distance_cm"], data["latency_ms"], data["sequence"],
        )
        return True


class CollectorWorker:
    """Write telemetry outside Paho's network callback thread.

    The receive timestamp is captured by ``on_message`` before an item enters
    this queue. A slow InfluxDB request therefore cannot postpone the receive
    timestamp of the following MQTT message.
    """

    def __init__(self, collector: TelemetryCollector, queue_size: int = 1000) -> None:
        self.collector = collector
        self._queue: Queue[tuple[bytes | str, int] | None] = Queue(maxsize=queue_size)
        self._stopped = Event()
        self._thread = Thread(target=self._run, name="influx-writer", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def submit(self, payload: bytes | str, received_ms: int) -> bool:
        try:
            self._queue.put_nowait((payload, received_ms))
            return True
        except Full:
            LOGGER.error("Collector queue full; MQTT message could not be queued")
            return False

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                payload, received_ms = item
                self.collector.process_payload(payload, received_ms)
            finally:
                self._queue.task_done()

    def stop(self) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        self._queue.put(None)
        self._thread.join()


def create_mqtt_client(settings: Settings, worker: CollectorWorker):
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise RuntimeError(
            "Missing paho-mqtt. Run: pip install -r python/requirements.txt"
        ) from exc

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"iotlab2-collector-{int(time.time())}",
        protocol=mqtt.MQTTv311,
    )

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            LOGGER.info("MQTT connected: %s:%d", settings.mqtt_host, settings.mqtt_port)
            result, _mid = client.subscribe(settings.mqtt_topic, qos=0)
            if result == mqtt.MQTT_ERR_SUCCESS:
                LOGGER.info("Subscribed: %s", settings.mqtt_topic)
            else:
                LOGGER.error("MQTT subscribe failed, result=%s", result)
        else:
            LOGGER.error("MQTT connection rejected: %s", reason_code)

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        if reason_code != 0:
            LOGGER.warning("MQTT disconnected unexpectedly: %s; reconnecting", reason_code)
        else:
            LOGGER.info("MQTT disconnected")

    def on_message(client, userdata, message):
        received_ms = time.time_ns() // 1_000_000
        LOGGER.info("MQTT message received: topic=%s bytes=%d",
                    message.topic, len(message.payload))
        worker.submit(message.payload, received_ms)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        settings = load_settings()
        store = InfluxStore(settings)
        store.verify_connection()
        LOGGER.info("InfluxDB connected: %s bucket=%s",
                    settings.influxdb_url, settings.influxdb_bucket)
        collector = TelemetryCollector(store)
        worker = CollectorWorker(collector)
        worker.start()
        mqtt_client = create_mqtt_client(settings, worker)
        mqtt_client.connect_async(settings.mqtt_host, settings.mqtt_port, keepalive=60)
        LOGGER.info("Starting MQTT collector; press Ctrl+C to stop")
        mqtt_client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        LOGGER.info("Collector stopped by user")
    except Exception as exc:
        LOGGER.error("Collector could not start: %s", exc)
        return 1
    finally:
        if "worker" in locals():
            worker.stop()
        if "store" in locals():
            store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
