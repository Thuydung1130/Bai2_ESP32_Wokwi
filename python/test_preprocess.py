from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from preprocess import (
    SENSOR_FIELDS,
    clean_raw_dataframe,
    dataframe_to_points,
    point_keys,
    resample_and_engineer,
)


def raw_frame(values, device_id="esp32-a", start="2026-09-29T10:00:00Z", spacing="5s"):
    timestamps = pd.date_range(start, periods=len(values), freq=spacing, tz="UTC")
    rows = []
    for index, value in enumerate(values):
        row = {
            "_time": timestamps[index],
            "device_id": device_id,
            "temperature": value,
            "humidity": 50.0 if value is not None else None,
            "distance_cm": 100.0 if value is not None else None,
            "rssi": -55,
            "sequence": index + 1,
            "uptime_s": index * 5,
            "latency_ms": 100,
        }
        rows.append(row)
    return pd.DataFrame(rows)


class CleaningTests(unittest.TestCase):
    def test_empty_dataframe(self):
        cleaned, stats = clean_raw_dataframe(pd.DataFrame())
        self.assertTrue(cleaned.empty)
        self.assertEqual(stats["input_records"], 0)

    def test_short_missing_gap_is_interpolated(self):
        cleaned, stats = clean_raw_dataframe(raw_frame([20.0, None, 22.0]))
        self.assertAlmostEqual(cleaned.loc[1, "temperature"], 21.0)
        self.assertEqual(stats["records_with_missing_values"], 1)
        self.assertEqual(stats["missing_values"], 3)
        self.assertEqual(stats["interpolated_values"], 3)

    def test_long_time_gap_is_not_interpolated(self):
        frame = raw_frame([20.0, None, 22.0], spacing="20s")
        cleaned, _stats = clean_raw_dataframe(frame)
        self.assertTrue(pd.isna(cleaned.loc[1, "temperature"]))

    def test_iqr_outlier_is_flagged(self):
        frame = raw_frame([20.0, 20.1, 19.9, 20.0, 80.0])
        cleaned, stats = clean_raw_dataframe(frame)
        self.assertEqual(stats["outlier_temperature"], 1)
        self.assertTrue(bool(cleaned.loc[4, "temperature_outlier"]))

    def test_zero_iqr_does_not_mark_outliers(self):
        cleaned, stats = clean_raw_dataframe(raw_frame([20.0] * 6))
        self.assertEqual(stats["outlier_temperature"], 0)
        self.assertFalse(cleaned["temperature_outlier"].any())

    def test_negative_latency_is_separate_and_excluded(self):
        frame = raw_frame([20.0, 21.0])
        frame.loc[0, "latency_ms"] = -25
        cleaned, stats = clean_raw_dataframe(frame)
        self.assertEqual(stats["invalid_latency_records"], 1)
        self.assertTrue(pd.isna(cleaned.loc[0, "latency_valid_ms"]))
        self.assertEqual(cleaned.loc[0, "latency_ms"], -25)

    def test_duplicate_record_is_removed(self):
        frame = raw_frame([20.0, 21.0])
        frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
        cleaned, stats = clean_raw_dataframe(frame)
        self.assertEqual(len(cleaned), 2)
        self.assertEqual(stats["duplicate_records"], 1)


class ResamplingTests(unittest.TestCase):
    def test_one_minute_resampling(self):
        cleaned, _stats = clean_raw_dataframe(raw_frame([20.0] * 13))
        processed = resample_and_engineer(cleaned)
        self.assertEqual(len(processed), 2)
        self.assertEqual(int(processed.loc[0, "sample_count"]), 12)
        self.assertEqual(int(processed.loc[1, "sample_count"]), 1)

    def test_rolling_mean_and_delta(self):
        frame = pd.concat([
            raw_frame([20.0], start="2026-09-29T10:00:00Z"),
            raw_frame([22.0], start="2026-09-29T10:01:00Z"),
            raw_frame([24.0], start="2026-09-29T10:02:00Z"),
        ], ignore_index=True)
        frame["sequence"] = [1, 2, 3]
        cleaned, _stats = clean_raw_dataframe(frame)
        processed = resample_and_engineer(cleaned)
        self.assertAlmostEqual(processed.loc[2, "temp_rolling_mean"], 22.0)
        self.assertAlmostEqual(processed.loc[1, "temp_delta"], 2.0)

    def test_constant_min_max_normalization(self):
        cleaned, _stats = clean_raw_dataframe(raw_frame([20.0] * 13))
        processed = resample_and_engineer(cleaned)
        self.assertTrue((processed["temp_normalized"] == 0.0).all())

    def test_devices_are_not_mixed(self):
        frame = pd.concat([
            raw_frame([20.0, 20.0], device_id="a"),
            raw_frame([70.0, 70.0], device_id="b"),
        ], ignore_index=True)
        cleaned, _stats = clean_raw_dataframe(frame)
        processed = resample_and_engineer(cleaned)
        means = dict(zip(processed["device_id"], processed["temperature"]))
        self.assertEqual(means, {"a": 20.0, "b": 70.0})

    def test_rerun_has_identical_point_keys(self):
        cleaned, _stats = clean_raw_dataframe(raw_frame([20.0] * 13))
        first = resample_and_engineer(cleaned)
        second = resample_and_engineer(cleaned)
        self.assertEqual(point_keys(first), point_keys(second))
        self.assertEqual(len(point_keys(first)), len(set(point_keys(first))))

    def test_nan_and_infinity_are_not_written(self):
        cleaned, _stats = clean_raw_dataframe(raw_frame([20.0]))
        processed = resample_and_engineer(cleaned)
        processed.loc[0, "temp_delta"] = float("inf")
        processed.loc[0, "distance_delta"] = float("nan")
        line = dataframe_to_points(processed)[0].to_line_protocol()
        self.assertNotIn("temp_delta=", line)
        self.assertNotIn("distance_delta=", line)
        self.assertNotIn("nan", line.lower())
        self.assertNotIn("inf", line.lower())


if __name__ == "__main__":
    unittest.main()
