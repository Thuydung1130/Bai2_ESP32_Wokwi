"""Mốc 3: preprocess sensor_raw and write one-minute sensor_processed points."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

from config import Settings, load_settings


LOGGER = logging.getLogger("preprocess")
RAW_MEASUREMENT = "sensor_raw"
PROCESSED_MEASUREMENT = "sensor_processed"
SENSOR_FIELDS = ["temperature", "humidity", "distance_cm"]
NUMERIC_FIELDS = SENSOR_FIELDS + ["rssi", "sequence", "uptime_s", "latency_ms"]
MAX_INTERPOLATION_POINTS = 2
MAX_INTERPOLATION_GAP_SECONDS = 30
MIN_IQR_SAMPLES = 4
RANGE_PATTERN = re.compile(r"^-\d+[smhdw]$")


def validate_range_start(value: str) -> str:
    if not RANGE_PATTERN.fullmatch(value):
        raise ValueError("range must look like -30m, -1h, -2d, or -1w")
    return value


def _flux_string(value: str) -> str:
    return json.dumps(value)


def build_flux_query(settings: Settings, range_start: str) -> str:
    validate_range_start(range_start)
    fields = " or ".join(f'r._field == {_flux_string(name)}' for name in NUMERIC_FIELDS)
    return f'''from(bucket: {_flux_string(settings.influxdb_bucket)})
  |> range(start: {range_start})
  |> filter(fn: (r) => r._measurement == {_flux_string(RAW_MEASUREMENT)})
  |> filter(fn: (r) => {fields})
  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "device_id", {", ".join(_flux_string(f) for f in NUMERIC_FIELDS)}])
  |> sort(columns: ["device_id", "_time"])
'''


def query_raw_dataframe(client: Any, settings: Settings, range_start: str) -> pd.DataFrame:
    result = client.query_api().query_data_frame(
        query=build_flux_query(settings, range_start),
        org=settings.influxdb_org,
    )
    if isinstance(result, list):
        frames = [frame for frame in result if not frame.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return result.copy() if isinstance(result, pd.DataFrame) else pd.DataFrame()


def interpolate_short_gaps(group: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    result = group.sort_values("timestamp").reset_index(drop=True).copy()
    filled_count = 0
    for field in SENSOR_FIELDS:
        values = result[field].copy()
        missing_positions = [index for index, value in enumerate(values) if pd.isna(value)]
        cursor = 0
        while cursor < len(missing_positions):
            run = [missing_positions[cursor]]
            cursor += 1
            while cursor < len(missing_positions) and missing_positions[cursor] == run[-1] + 1:
                run.append(missing_positions[cursor])
                cursor += 1
            before, after = run[0] - 1, run[-1] + 1
            if len(run) > MAX_INTERPOLATION_POINTS or before < 0 or after >= len(result):
                continue
            if pd.isna(values.iloc[before]) or pd.isna(values.iloc[after]):
                continue
            total_gap = (result.loc[after, "timestamp"] - result.loc[before, "timestamp"]).total_seconds()
            if total_gap <= 0 or total_gap > MAX_INTERPOLATION_GAP_SECONDS:
                continue
            start_value, end_value = float(values.iloc[before]), float(values.iloc[after])
            start_time = result.loc[before, "timestamp"]
            for position in run:
                elapsed = (result.loc[position, "timestamp"] - start_time).total_seconds()
                values.iloc[position] = start_value + (end_value - start_value) * elapsed / total_gap
                filled_count += 1
        result[field] = values
    return result, filled_count


def mark_iqr_outliers(group: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    result = group.copy()
    counts: dict[str, int] = {}
    for field in SENSOR_FIELDS:
        flag = f"{field}_outlier"
        result[flag] = False
        valid = result[field].dropna()
        if len(valid) < MIN_IQR_SAMPLES:
            counts[field] = 0
            continue
        q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
        iqr = q3 - q1
        if not math.isfinite(iqr) or iqr <= 0:
            counts[field] = 0
            continue
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        mask = result[field].notna() & ~result[field].between(lower, upper)
        result.loc[mask, flag] = True
        result.loc[mask, field] = pd.NA
        counts[field] = int(mask.sum())
    return result, counts


def clean_raw_dataframe(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    statistics = {
        "input_records": int(len(raw)),
        "records_after_cleaning": 0,
        "records_with_missing_values": 0,
        "missing_values": 0,
        "duplicate_records": 0,
        "interpolated_values": 0,
        "outlier_temperature": 0,
        "outlier_humidity": 0,
        "outlier_distance_cm": 0,
        "invalid_latency_records": 0,
    }
    if raw.empty:
        return pd.DataFrame(), statistics

    data = raw.copy()
    if "_time" in data.columns and "timestamp" not in data.columns:
        data = data.rename(columns={"_time": "timestamp"})
    required = {"timestamp", "device_id", *NUMERIC_FIELDS}
    missing_columns = sorted(required - set(data.columns))
    if missing_columns:
        raise ValueError("query result missing columns: " + ", ".join(missing_columns))

    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    data["device_id"] = data["device_id"].astype("string").str.strip()
    for field in NUMERIC_FIELDS:
        data[field] = pd.to_numeric(data[field], errors="coerce")
    data[NUMERIC_FIELDS] = data[NUMERIC_FIELDS].replace([math.inf, -math.inf], pd.NA)

    valid_identity = data["timestamp"].notna() & data["device_id"].notna() & data["device_id"].ne("")
    data = data.loc[valid_identity].copy()
    duplicate_mask = data.duplicated(
        subset=["device_id", "timestamp", "sequence"], keep="last"
    )
    statistics["duplicate_records"] = int(duplicate_mask.sum())
    data = data.loc[~duplicate_mask]
    statistics["records_with_missing_values"] = int(data[SENSOR_FIELDS].isna().any(axis=1).sum())
    statistics["missing_values"] = int(data[SENSOR_FIELDS].isna().sum().sum())

    cleaned_groups = []
    for _device_id, group in data.groupby("device_id", sort=False):
        interpolated, filled = interpolate_short_gaps(group)
        marked, outliers = mark_iqr_outliers(interpolated)
        marked, filled_after_outlier = interpolate_short_gaps(marked)
        statistics["interpolated_values"] += filled + filled_after_outlier
        for field in SENSOR_FIELDS:
            statistics[f"outlier_{field}"] += outliers[field]
        cleaned_groups.append(marked)

    cleaned = pd.concat(cleaned_groups, ignore_index=True) if cleaned_groups else pd.DataFrame()
    if cleaned.empty:
        return cleaned, statistics
    cleaned["latency_invalid"] = cleaned["latency_ms"].notna() & cleaned["latency_ms"].lt(0)
    cleaned["latency_valid_ms"] = cleaned["latency_ms"].where(~cleaned["latency_invalid"])
    statistics["invalid_latency_records"] = int(cleaned["latency_invalid"].sum())
    cleaned["valid_sensor_record"] = cleaned[SENSOR_FIELDS].notna().all(axis=1)
    cleaned = cleaned.sort_values(["device_id", "timestamp"]).reset_index(drop=True)
    statistics["records_after_cleaning"] = int(len(cleaned))
    return cleaned, statistics


def _rolling_without_gaps(series: pd.Series) -> pd.Series:
    blocks = series.isna().cumsum()
    return series.groupby(blocks).transform(lambda values: values.rolling(3, min_periods=1).mean())


def _min_max(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    result = pd.Series(float("nan"), index=series.index, dtype="float64")
    if valid.empty:
        return result
    minimum, maximum = valid.min(), valid.max()
    if maximum == minimum:
        result.loc[series.notna()] = 0.0
    else:
        result = (series - minimum) / (maximum - minimum)
    return result


def resample_and_engineer(cleaned: pd.DataFrame) -> pd.DataFrame:
    if cleaned.empty:
        return pd.DataFrame()
    outputs = []
    for device_id, group in cleaned.groupby("device_id", sort=False):
        indexed = group.set_index("timestamp").sort_index()
        aggregated = indexed.resample("1min").agg(
            temperature=("temperature", "mean"),
            humidity=("humidity", "mean"),
            distance_cm=("distance_cm", "mean"),
            sample_count=("valid_sensor_record", "sum"),
            latency_mean_ms=("latency_valid_ms", "mean"),
            invalid_latency_count=("latency_invalid", "sum"),
            temperature_outlier_count=("temperature_outlier", "sum"),
            humidity_outlier_count=("humidity_outlier", "sum"),
            distance_outlier_count=("distance_cm_outlier", "sum"),
        )
        aggregated["device_id"] = str(device_id)
        aggregated["temp_rolling_mean"] = _rolling_without_gaps(aggregated["temperature"])
        aggregated["humidity_rolling_mean"] = _rolling_without_gaps(aggregated["humidity"])
        aggregated["temp_delta"] = aggregated["temperature"].diff()
        aggregated["distance_delta"] = aggregated["distance_cm"].diff()
        aggregated["temp_normalized"] = _min_max(aggregated["temperature"])
        aggregated["humidity_normalized"] = _min_max(aggregated["humidity"])
        outputs.append(aggregated.reset_index())
    return pd.concat(outputs, ignore_index=True).sort_values(
        ["device_id", "timestamp"]
    ).reset_index(drop=True)


def dataframe_to_points(processed: pd.DataFrame, measurement: str = PROCESSED_MEASUREMENT):
    from influxdb_client import Point

    integer_fields = {
        "sample_count", "invalid_latency_count", "temperature_outlier_count",
        "humidity_outlier_count", "distance_outlier_count",
    }
    field_names = [
        "temperature", "humidity", "distance_cm", "temp_rolling_mean",
        "humidity_rolling_mean", "temp_delta", "distance_delta",
        "temp_normalized", "humidity_normalized", "sample_count",
        "latency_mean_ms", "invalid_latency_count", "temperature_outlier_count",
        "humidity_outlier_count", "distance_outlier_count",
    ]
    points = []
    for row in processed.to_dict("records"):
        point = Point(measurement).tag("device_id", str(row["device_id"]))
        field_count = 0
        for name in field_names:
            value = row.get(name)
            if value is None or pd.isna(value) or not math.isfinite(float(value)):
                continue
            point = point.field(name, int(value) if name in integer_fields else float(value))
            field_count += 1
        if field_count:
            point = point.time(pd.Timestamp(row["timestamp"]).to_pydatetime())
            points.append(point)
    return points


def point_keys(processed: pd.DataFrame) -> list[tuple[str, str, pd.Timestamp]]:
    return [
        (PROCESSED_MEASUREMENT, str(row.device_id), pd.Timestamp(row.timestamp))
        for row in processed.itertuples()
    ]


def write_processed(client: Any, settings: Settings, processed: pd.DataFrame) -> int:
    from influxdb_client.client.write_api import SYNCHRONOUS

    points = dataframe_to_points(processed)
    if not points:
        return 0
    client.write_api(write_options=SYNCHRONOUS).write(
        bucket=settings.influxdb_bucket,
        org=settings.influxdb_org,
        record=points,
    )
    return len(points)


def verify_processed_windows(client: Any, settings: Settings, range_start: str) -> int:
    query = f'''from(bucket: {_flux_string(settings.influxdb_bucket)})
  |> range(start: {validate_range_start(range_start)})
  |> filter(fn: (r) => r._measurement == {_flux_string(PROCESSED_MEASUREMENT)})
  |> filter(fn: (r) => r._field == "sample_count")
'''
    tables = client.query_api().query(query=query, org=settings.influxdb_org)
    return sum(len(table.records) for table in tables)


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess sensor_raw from InfluxDB Cloud 2")
    parser.add_argument("--range", default="-1h", dest="range_start",
                        help="Flux range such as -30m, -1h, -2d, or -1w")
    parser.add_argument("--report", default="python/preprocessing_report.json")
    parser.add_argument("--csv", help="Optional path for processed CSV output")
    parser.add_argument("--dry-run", action="store_true", help="Query and process without writing Cloud")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    client = None
    report: dict[str, Any] = {
        "query_range": validate_range_start(args.range_start),
        "run_time_utc": datetime.now(timezone.utc).isoformat(),
        "raw_measurement": RAW_MEASUREMENT,
        "processed_measurement": PROCESSED_MEASUREMENT,
    }
    try:
        from influxdb_client import InfluxDBClient

        settings = load_settings()
        client = InfluxDBClient(
            url=settings.influxdb_url, token=settings.influxdb_token,
            org=settings.influxdb_org, timeout=30_000,
        )
        if not client.ping():
            raise ConnectionError("InfluxDB Cloud ping failed")
        LOGGER.info("Connected to InfluxDB Cloud; querying %s", args.range_start)
        raw = query_raw_dataframe(client, settings, args.range_start)
        cleaned, statistics = clean_raw_dataframe(raw)
        report.update(statistics)
        if cleaned.empty:
            report.update({
                "resampling_windows": 0,
                "records_written": 0,
                "verified_processed_windows": 0,
            })
            write_report(Path(args.report), report)
            LOGGER.warning("No sensor_raw data found in the selected range")
            return 0

        processed = resample_and_engineer(cleaned)
        report["resampling_windows"] = int(len(processed))
        if args.csv:
            Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
            processed.to_csv(args.csv, index=False)
        report["records_written"] = 0 if args.dry_run else write_processed(
            client, settings, processed
        )
        report["verified_processed_windows"] = (
            0 if args.dry_run else verify_processed_windows(
                client, settings, args.range_start
            )
        )
        report["dry_run"] = bool(args.dry_run)
        write_report(Path(args.report), report)
        LOGGER.info(
            "Preprocessing complete: input=%d missing=%d duplicates=%d "
            "outliers=(T:%d H:%d D:%d) invalid_latency=%d windows=%d written=%d verified=%d",
            report["input_records"], report["records_with_missing_values"],
            report["duplicate_records"], report["outlier_temperature"],
            report["outlier_humidity"], report["outlier_distance_cm"],
            report["invalid_latency_records"], report["resampling_windows"],
            report["records_written"], report["verified_processed_windows"],
        )
        return 0
    except Exception as exc:
        LOGGER.error("Preprocessing failed: %s", exc)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
