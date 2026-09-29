import json
import math
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mqtt_collector import (
    CollectorWorker,
    TelemetryCollector,
    ValidationError,
    utc_from_ms,
    validate_payload,
)
from config import load_settings


NOW_MS = 1_800_000_000_000


def valid_data(**changes):
    data = {
        "temperature": 25.0,
        "humidity": 50.0,
        "distance_cm": 100.0,
        "rssi": -55,
        "sequence": 10,
        "uptime_s": 50,
        "device_id": "esp32-dung059",
        "timestamp_ms": NOW_MS - 100,
    }
    data.update(changes)
    return data


class FakeStore:
    def __init__(self, fail=False):
        self.fail = fail
        self.rows = []

    def write(self, data):
        if self.fail:
            raise ConnectionError("InfluxDB unavailable")
        self.rows.append(data.copy())

    def close(self):
        pass


class ValidationTests(unittest.TestCase):
    def test_utc_diagnostic_uses_milliseconds(self):
        self.assertEqual(utc_from_ms(1_700_000_000_123), "2023-11-14T22:13:20.123Z")

    def test_valid_json(self):
        result = validate_payload(json.dumps(valid_data()), NOW_MS)
        self.assertEqual(result["latency_ms"], 100)
        self.assertEqual(result["device_id"], "esp32-dung059")

    def test_invalid_json(self):
        with self.assertRaises(ValidationError):
            validate_payload("{bad json", NOW_MS)

    def test_missing_field(self):
        data = valid_data()
        del data["humidity"]
        with self.assertRaisesRegex(ValidationError, "missing fields"):
            validate_payload(json.dumps(data), NOW_MS)

    def test_invalid_sensor_ranges(self):
        for changes in (
            {"temperature": 81}, {"humidity": 101}, {"distance_cm": -1},
            {"distance_cm": 401},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validate_payload(json.dumps(valid_data(**changes)), NOW_MS)

    def test_nan_and_infinity_rejected(self):
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_payload(json.dumps(valid_data(temperature=value)), NOW_MS)

    def test_invalid_timestamp(self):
        with self.assertRaises(ValidationError):
            validate_payload(json.dumps(valid_data(timestamp_ms=1234)), NOW_MS)
        with self.assertRaises(ValidationError):
            validate_payload(json.dumps(valid_data(timestamp_ms=NOW_MS + 60_001)), NOW_MS)


class CollectorTests(unittest.TestCase):
    def test_duplicate_is_not_written_twice(self):
        store = FakeStore()
        collector = TelemetryCollector(store)
        payload = json.dumps(valid_data())
        self.assertTrue(collector.process_payload(payload, NOW_MS))
        self.assertFalse(collector.process_payload(payload, NOW_MS))
        self.assertEqual(len(store.rows), 1)

    def test_restart_sequence_is_accepted(self):
        store = FakeStore()
        collector = TelemetryCollector(store)
        collector.process_payload(json.dumps(valid_data(sequence=100, uptime_s=500)), NOW_MS)
        restarted = valid_data(sequence=1, uptime_s=5, timestamp_ms=NOW_MS)
        self.assertTrue(collector.process_payload(json.dumps(restarted), NOW_MS + 100))
        self.assertEqual(len(store.rows), 2)

    def test_failed_write_is_not_committed_as_duplicate(self):
        store = FakeStore(fail=True)
        collector = TelemetryCollector(store)
        payload = json.dumps(valid_data())
        self.assertFalse(collector.process_payload(payload, NOW_MS))
        store.fail = False
        self.assertTrue(collector.process_payload(payload, NOW_MS))
        self.assertEqual(len(store.rows), 1)

    def test_worker_preserves_mqtt_receive_timestamp(self):
        store = FakeStore()
        collector = TelemetryCollector(store)
        worker = CollectorWorker(collector)
        worker.start()
        receive_time = NOW_MS
        self.assertTrue(worker.submit(json.dumps(valid_data()), receive_time))
        worker.stop()
        self.assertEqual(len(store.rows), 1)
        self.assertEqual(store.rows[0]["latency_ms"], 100)


class CloudConfigurationTests(unittest.TestCase):
    def test_cloud_settings_are_loaded(self):
        environment = {
            "MQTT_HOST": "broker.hivemq.com",
            "MQTT_PORT": "1883",
            "MQTT_TOPIC": "iotlab2/dung059/esp32/telemetry",
            "INFLUX_URL": "https://us-east-1-1.aws.cloud2.influxdata.com/",
            "INFLUX_ORG": "iot_lab",
            "INFLUX_BUCKET": "iot_raw",
            "INFLUX_TOKEN": "test-token",
        }
        with patch.dict("os.environ", environment, clear=True):
            settings = load_settings()
        self.assertEqual(
            settings.influxdb_url,
            "https://us-east-1-1.aws.cloud2.influxdata.com",
        )
        self.assertEqual(settings.influxdb_measurement, "sensor_raw")

    def test_non_https_cloud_url_is_rejected(self):
        environment = {
            "MQTT_HOST": "broker.hivemq.com",
            "MQTT_PORT": "1883",
            "MQTT_TOPIC": "iotlab2/dung059/esp32/telemetry",
            "INFLUX_URL": "http://example.invalid",
            "INFLUX_ORG": "iot_lab",
            "INFLUX_BUCKET": "iot_raw",
            "INFLUX_TOKEN": "test-token",
        }
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(ValueError, "must use https"):
                load_settings()

    def test_legacy_variable_names_remain_compatible(self):
        environment = {
            "MQTT_HOST": "broker.hivemq.com",
            "MQTT_PORT": "1883",
            "MQTT_TOPIC": "iotlab2/dung059/esp32/telemetry",
            "INFLUXDB_URL": "https://us-east-1-1.aws.cloud2.influxdata.com",
            "INFLUXDB_ORG": "iot_lab",
            "INFLUXDB_BUCKET": "iot_raw",
            "INFLUXDB_TOKEN": "test-token",
        }
        with patch.dict("os.environ", environment, clear=True):
            settings = load_settings()
        self.assertEqual(settings.influxdb_bucket, "iot_raw")

if __name__ == "__main__":
    unittest.main()
