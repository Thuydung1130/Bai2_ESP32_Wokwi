from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Settings
from dashboard import (
    PROCESSED_FIELDS,
    PROCESSED_MEASUREMENT,
    RAW_FIELDS,
    RAW_MEASUREMENT,
    build_dashboard_query,
    calculate_latency_metrics,
    combine_raw_processed,
    filter_device,
    latest_sensor_metrics,
    normalize_query_result,
    query_measurement,
)


def sample_raw():
    return pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2026-09-29T10:00:00Z", "2026-09-29T10:00:05Z",
            "2026-09-29T10:00:10Z", "2026-09-29T10:00:15Z",
        ], utc=True),
        "device_id": ["a", "a", "b", "a"],
        "temperature": [20.0, 21.0, 30.0, 22.0],
        "humidity": [50.0, 51.0, 60.0, 52.0],
        "distance_cm": [100.0, 101.0, 200.0, 102.0],
        "rssi": [-50, -51, -60, -52],
        "sequence": [1, 2, 1, 3],
        "uptime_s": [5, 10, 5, 15],
        "latency_ms": [100, -20, 300, 200],
    })


class FakeQueryApi:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    def query_data_frame(self, **kwargs):
        if self.error:
            raise self.error
        return self.result


class FakeClient:
    def __init__(self, result=None, error=None):
        self.api = FakeQueryApi(result, error)

    def query_api(self):
        return self.api


SETTINGS = Settings("", 1883, "", "https://example.com", "secret", "org", "bucket", "sensor_raw")


class DashboardDataTests(unittest.TestCase):
    def test_empty_dataframe_is_handled(self):
        result = normalize_query_result(pd.DataFrame(), RAW_FIELDS)
        self.assertTrue(result.empty)
        self.assertIn("timestamp", result.columns)

    def test_latest_sensor_metrics(self):
        latest = latest_sensor_metrics(filter_device(sample_raw(), "a"))
        self.assertEqual(latest["temperature"], 22.0)
        self.assertEqual(latest["device_id"], "a")

    def test_latency_excludes_negative(self):
        metrics = calculate_latency_metrics(filter_device(sample_raw(), "a"))
        self.assertEqual(metrics["invalid_count"], 1)
        self.assertEqual(metrics["valid_count"], 2)
        self.assertEqual(metrics["mean"], 150.0)
        self.assertEqual(metrics["min"], 100.0)
        self.assertEqual(metrics["max"], 200.0)

    def test_device_filter(self):
        filtered = filter_device(sample_raw(), "b")
        self.assertEqual(len(filtered), 1)
        self.assertTrue((filtered["device_id"] == "b").all())

    def test_raw_query_uses_requested_range_and_measurement(self):
        query = build_dashboard_query("iot_raw", RAW_MEASUREMENT, RAW_FIELDS, "-15m")
        self.assertIn("sensor_raw", query)
        self.assertIn("range(start: -15m)", query)

    def test_processed_query_uses_actual_fields(self):
        query = build_dashboard_query(
            "iot_raw", PROCESSED_MEASUREMENT, PROCESSED_FIELDS, "-24h"
        )
        self.assertIn("sensor_processed", query)
        self.assertIn("temp_rolling_mean", query)

    def test_invalid_time_range_is_rejected(self):
        with self.assertRaises(ValueError):
            build_dashboard_query("iot_raw", RAW_MEASUREMENT, RAW_FIELDS, "-1y")

    def test_raw_and_processed_are_combined_on_time_axis(self):
        raw = filter_device(sample_raw(), "a")
        processed = pd.DataFrame({
            "timestamp": pd.to_datetime(["2026-09-29T10:00:00Z"], utc=True),
            "device_id": ["a"],
            "temperature": [21.0],
            "temp_rolling_mean": [21.0],
        })
        result = combine_raw_processed(
            raw, processed, "temperature", ["temperature", "temp_rolling_mean"]
        )
        self.assertEqual(
            set(result["series"]),
            {"temperature raw", "temperature", "temp_rolling_mean"},
        )
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["timestamp"]))

    def test_query_reads_and_normalizes_dataframe(self):
        frame = sample_raw().rename(columns={"timestamp": "_time"})
        result = query_measurement(
            FakeClient(frame), SETTINGS, RAW_MEASUREMENT, RAW_FIELDS, "-1h"
        )
        self.assertEqual(len(result), 4)
        self.assertIn("timestamp", result.columns)

    def test_connection_error_is_propagated_for_ui_to_report(self):
        with self.assertRaises(ConnectionError):
            query_measurement(
                FakeClient(error=ConnectionError("offline")),
                SETTINGS,
                RAW_MEASUREMENT,
                RAW_FIELDS,
                "-1h",
            )


if __name__ == "__main__":
    unittest.main()
