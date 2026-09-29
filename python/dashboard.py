"""Mốc 4 Streamlit dashboard for raw and processed IoT telemetry."""

from __future__ import annotations

import json
import math
from pathlib import Path
import time
from typing import Any

import pandas as pd
import streamlit as st

from config import Settings, load_settings


RAW_MEASUREMENT = "sensor_raw"
PROCESSED_MEASUREMENT = "sensor_processed"
RAW_FIELDS = [
    "temperature", "humidity", "distance_cm", "rssi", "sequence",
    "uptime_s", "latency_ms",
]
PROCESSED_FIELDS = [
    "temperature", "humidity", "distance_cm", "temp_rolling_mean",
    "humidity_rolling_mean", "temp_delta", "distance_delta",
    "temp_normalized", "humidity_normalized", "sample_count",
    "latency_mean_ms", "invalid_latency_count", "temperature_outlier_count",
    "humidity_outlier_count", "distance_outlier_count",
]
TIME_RANGES = {"15 phút": "-15m", "1 giờ": "-1h", "6 giờ": "-6h", "24 giờ": "-24h"}
REPORT_PATH = Path(__file__).resolve().parent / "preprocessing_report.json"


def _flux_string(value: str) -> str:
    return json.dumps(value)


def build_dashboard_query(bucket: str, measurement: str, fields: list[str], range_start: str) -> str:
    if range_start not in TIME_RANGES.values():
        raise ValueError("unsupported dashboard time range")
    if measurement not in {RAW_MEASUREMENT, PROCESSED_MEASUREMENT}:
        raise ValueError("unsupported measurement")
    field_filter = " or ".join(f'r._field == {_flux_string(field)}' for field in fields)
    return f'''from(bucket: {_flux_string(bucket)})
  |> range(start: {range_start})
  |> filter(fn: (r) => r._measurement == {_flux_string(measurement)})
  |> filter(fn: (r) => {field_filter})
  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "device_id", {", ".join(_flux_string(f) for f in fields)}])
  |> sort(columns: ["device_id", "_time"])
'''


def normalize_query_result(result: Any, fields: list[str]) -> pd.DataFrame:
    if isinstance(result, list):
        frames = [frame for frame in result if isinstance(frame, pd.DataFrame) and not frame.empty]
        data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    elif isinstance(result, pd.DataFrame):
        data = result.copy()
    else:
        data = pd.DataFrame()
    if data.empty:
        return pd.DataFrame(columns=["timestamp", "device_id", *fields])
    if "_time" in data.columns:
        data = data.rename(columns={"_time": "timestamp"})
    if "timestamp" not in data.columns or "device_id" not in data.columns:
        return pd.DataFrame(columns=["timestamp", "device_id", *fields])
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    data["device_id"] = data["device_id"].astype("string").str.strip()
    for field in fields:
        if field not in data.columns:
            data[field] = pd.NA
        data[field] = pd.to_numeric(data[field], errors="coerce")
        data[field] = data[field].replace([math.inf, -math.inf], pd.NA)
    valid = data["timestamp"].notna() & data["device_id"].notna() & data["device_id"].ne("")
    return data.loc[valid, ["timestamp", "device_id", *fields]].sort_values(
        ["device_id", "timestamp"]
    ).reset_index(drop=True)


def query_measurement(client: Any, settings: Settings, measurement: str,
                      fields: list[str], range_start: str) -> pd.DataFrame:
    result = client.query_api().query_data_frame(
        query=build_dashboard_query(
            settings.influxdb_bucket, measurement, fields, range_start
        ),
        org=settings.influxdb_org,
    )
    return normalize_query_result(result, fields)


def filter_device(data: pd.DataFrame, device_id: str) -> pd.DataFrame:
    if data.empty:
        return data.copy()
    return data.loc[data["device_id"] == device_id].sort_values("timestamp").reset_index(drop=True)


def latest_sensor_metrics(raw: pd.DataFrame) -> dict[str, Any] | None:
    if raw.empty:
        return None
    row = raw.sort_values("timestamp").iloc[-1]
    return {
        "device_id": str(row["device_id"]),
        "temperature": row.get("temperature"),
        "humidity": row.get("humidity"),
        "distance_cm": row.get("distance_cm"),
        "rssi": row.get("rssi"),
        "timestamp": row["timestamp"],
    }


def calculate_latency_metrics(raw: pd.DataFrame) -> dict[str, Any]:
    if raw.empty or "latency_ms" not in raw.columns:
        return {"valid_count": 0, "invalid_count": 0, "mean": None, "min": None, "max": None}
    latency = pd.to_numeric(raw["latency_ms"], errors="coerce")
    invalid = latency.notna() & latency.lt(0)
    valid = latency[latency.notna() & latency.ge(0)]
    return {
        "valid_count": int(len(valid)),
        "invalid_count": int(invalid.sum()),
        "mean": float(valid.mean()) if not valid.empty else None,
        "min": float(valid.min()) if not valid.empty else None,
        "max": float(valid.max()) if not valid.empty else None,
    }


def combine_raw_processed(raw: pd.DataFrame, processed: pd.DataFrame,
                          raw_field: str, processed_fields: list[str]) -> pd.DataFrame:
    frames = []
    if not raw.empty and raw_field in raw.columns:
        part = raw[["timestamp", raw_field]].rename(columns={raw_field: "value"}).copy()
        part["series"] = f"{raw_field} raw"
        part["gap_limit_seconds"] = 20
        frames.append(part)
    for field in processed_fields:
        if not processed.empty and field in processed.columns:
            part = processed[["timestamp", field]].rename(columns={field: "value"}).copy()
            part["series"] = field
            part["gap_limit_seconds"] = 120
            frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["timestamp", "value", "series", "segment"])
    combined = pd.concat(frames, ignore_index=True).dropna(subset=["timestamp", "value"])
    combined = combined.sort_values(["series", "timestamp"])
    gaps = combined.groupby("series")["timestamp"].diff().dt.total_seconds()
    combined["segment"] = (
        (gaps > combined["gap_limit_seconds"]).groupby(combined["series"]).cumsum()
    ).astype(int)
    combined["segment_key"] = combined["series"] + "-" + combined["segment"].astype(str)
    return combined.reset_index(drop=True)


def load_preprocessing_report() -> dict[str, Any]:
    try:
        return json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


@st.cache_data(ttl=10, show_spinner=False)
def fetch_cloud_data(url: str, token: str, org: str, bucket: str,
                     range_start: str) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    from influxdb_client import InfluxDBClient

    started = time.perf_counter()
    client = InfluxDBClient(url=url, token=token, org=org, timeout=20_000)
    try:
        raw = query_measurement(
            client,
            Settings("", 1883, "", url, token, org, bucket, RAW_MEASUREMENT),
            RAW_MEASUREMENT,
            RAW_FIELDS,
            range_start,
        )
        processed = query_measurement(
            client,
            Settings("", 1883, "", url, token, org, bucket, RAW_MEASUREMENT),
            PROCESSED_MEASUREMENT,
            PROCESSED_FIELDS,
            range_start,
        )
    finally:
        client.close()
    return raw, processed, (time.perf_counter() - started) * 1000.0


def _format_metric(value: Any, suffix: str, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.{decimals}f} {suffix}".strip()


def _line_chart(data: pd.DataFrame, title: str, y_title: str):
    if data.empty:
        st.info(f"Không có dữ liệu cho {title} trong khoảng thời gian đã chọn.")
        return
    chart_data = data[["timestamp", "value", "series", "segment_key"]].copy()
    chart_data["timestamp"] = chart_data["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    spec = {
        "data": {"values": chart_data.to_dict("records")},
        "mark": {"type": "line", "strokeWidth": 2},
        "encoding": {
            "x": {"field": "timestamp", "type": "temporal", "title": "Thời gian UTC"},
            "y": {"field": "value", "type": "quantitative", "title": y_title,
                  "scale": {"zero": False}},
            "color": {"field": "series", "type": "nominal", "title": "Dữ liệu"},
            "detail": {"field": "segment_key", "type": "nominal"},
            "tooltip": [
                {"field": "timestamp", "type": "temporal", "title": "UTC"},
                {"field": "series", "type": "nominal", "title": "Series"},
                {"field": "value", "type": "quantitative", "title": "Giá trị", "format": ".2f"},
            ],
        },
        "title": title,
        "height": 280,
    }
    st.vega_lite_chart(spec, use_container_width=True)


def _html_table(data: pd.DataFrame) -> None:
    st.markdown(
        data.to_html(index=False, classes="dataframe", border=0, na_rep="—"),
        unsafe_allow_html=True,
    )


def render_dashboard(raw_all: pd.DataFrame, processed_all: pd.DataFrame,
                     device_id: str, query_ms: float, range_label: str) -> None:
    raw = filter_device(raw_all, device_id)
    processed = filter_device(processed_all, device_id)
    latest = latest_sensor_metrics(raw)

    st.header("1. Tổng quan thiết bị")
    if latest is None:
        st.warning("Không có dữ liệu sensor_raw cho thiết bị và khoảng thời gian đã chọn.")
    else:
        columns = st.columns(5)
        columns[0].metric("Device ID", latest["device_id"])
        columns[1].metric("Nhiệt độ", _format_metric(latest["temperature"], "°C"))
        columns[2].metric("Độ ẩm", _format_metric(latest["humidity"], "%"))
        columns[3].metric("Khoảng cách", _format_metric(latest["distance_cm"], "cm"))
        columns[4].metric("RSSI", _format_metric(latest["rssi"], "dBm", 0))
        st.caption(f"Bản ghi mới nhất: {latest['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}")

    st.header("2. Giám sát real-time từ sensor_raw")
    for field, title, unit in (
        ("temperature", "Nhiệt độ theo thời gian", "°C"),
        ("humidity", "Độ ẩm theo thời gian", "%"),
        ("distance_cm", "Khoảng cách theo thời gian", "cm"),
    ):
        chart_data = combine_raw_processed(raw, pd.DataFrame(), field, [])
        _line_chart(chart_data, title, unit)

    st.header("3. So sánh Raw và Processed")
    if processed.empty:
        st.info("Chưa có sensor_processed trong khoảng đã chọn. Hãy chạy preprocess.py rồi Refresh.")
    else:
        _line_chart(
            combine_raw_processed(raw, processed, "temperature", ["temperature", "temp_rolling_mean"]),
            "Temperature raw, processed và rolling mean",
            "°C",
        )
        _line_chart(
            combine_raw_processed(raw, processed, "humidity", ["humidity"]),
            "Humidity raw và processed",
            "%",
        )
        table_fields = [
            "timestamp", "device_id", "temperature", "humidity", "distance_cm",
            "temp_rolling_mean", "humidity_rolling_mean", "temp_delta",
            "distance_delta", "sample_count",
        ]
        _html_table(processed[[name for name in table_fields if name in processed.columns]])

    st.header("4. Đánh giá hệ thống")
    latency = calculate_latency_metrics(raw)
    report = load_preprocessing_report()
    metrics = st.columns(4)
    metrics[0].metric("Bản ghi raw", len(raw))
    metrics[1].metric("Cửa sổ processed", len(processed))
    metrics[2].metric("Latency trung bình", _format_metric(latency["mean"], "ms"))
    metrics[3].metric("Latency không hợp lệ", latency["invalid_count"])
    latency_columns = st.columns(4)
    latency_columns[0].metric("Latency nhỏ nhất", _format_metric(latency["min"], "ms"))
    latency_columns[1].metric("Latency lớn nhất", _format_metric(latency["max"], "ms"))
    latency_columns[2].metric("Latency hợp lệ", latency["valid_count"])
    outlier_total = sum(int(report.get(name, 0)) for name in (
        "outlier_temperature", "outlier_humidity", "outlier_distance_cm"
    ))
    latency_columns[3].metric("Outlier lần preprocess gần nhất", outlier_total)

    if not raw.empty and "latency_ms" in raw.columns:
        latency_chart = raw.loc[raw["latency_ms"].ge(0), ["timestamp", "latency_ms"]].copy()
        latency_chart = latency_chart.rename(columns={"latency_ms": "value"})
        latency_chart["series"] = "latency hợp lệ"
        latency_chart["segment_key"] = "latency-0"
        _line_chart(latency_chart, "Latency ESP32 → Python", "ms")

    st.subheader("Đánh giá lưu trữ trong phạm vi đã chọn")
    summary = pd.DataFrame({
        "Chỉ tiêu": [
            "Khoảng thời gian", "Chu kỳ thu thập dự kiến", "Bản ghi raw",
            "Cửa sổ processed", "Duplicate lần preprocess gần nhất",
            "Missing lần preprocess gần nhất", "Thời gian hai truy vấn Cloud",
        ],
        "Giá trị": [
            range_label, "5 giây", len(raw), len(processed),
            report.get("duplicate_records", "N/A"), report.get("missing_values", "N/A"),
            f"{query_ms:.1f} ms",
        ],
    })
    _html_table(summary)
    st.caption(
        "Latency trên dashboard là chênh lệch timestamp ESP32 và thời điểm Python nhận MQTT. "
        "Hệ thống chưa lưu timestamp xác nhận write nên đây không phải latency end-to-end đến database."
    )
    st.caption(f"Dashboard cập nhật lúc {pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d %H:%M:%S UTC')}")


def main() -> None:
    st.set_page_config(page_title="IoT Sensor Monitoring & Data Analytics", page_icon="📡", layout="wide")
    st.title("IoT Sensor Monitoring & Data Analytics")
    st.write("Giám sát ESP32, so sánh dữ liệu raw/processed và đánh giá pipeline IoT.")

    try:
        settings = load_settings()
    except Exception as exc:
        st.error(f"Không thể đọc cấu hình: {exc}")
        st.stop()

    st.sidebar.header("Bộ lọc")
    range_label = st.sidebar.selectbox("Khoảng thời gian", list(TIME_RANGES), index=1)
    auto_refresh = st.sidebar.toggle("Tự động cập nhật mỗi 10 giây", value=False)
    if st.sidebar.button("Refresh ngay", use_container_width=True):
        st.cache_data.clear()

    @st.fragment(run_every="10s" if auto_refresh else None)
    def live_content() -> None:
        try:
            with st.spinner("Đang truy vấn InfluxDB Cloud..."):
                raw_all, processed_all, query_ms = fetch_cloud_data(
                    settings.influxdb_url, settings.influxdb_token,
                    settings.influxdb_org, settings.influxdb_bucket,
                    TIME_RANGES[range_label],
                )
        except Exception as exc:
            st.error(f"Không thể truy vấn InfluxDB Cloud: {exc}")
            return
        devices = sorted(set(raw_all.get("device_id", pd.Series(dtype=str)).dropna()) |
                         set(processed_all.get("device_id", pd.Series(dtype=str)).dropna()))
        if not devices:
            st.warning("Không có device_id trong khoảng thời gian đã chọn.")
            return
        device_id = st.sidebar.selectbox("Thiết bị", devices, key="dashboard_device")
        render_dashboard(raw_all, processed_all, device_id, query_ms, range_label)

    live_content()


if __name__ == "__main__":
    main()
