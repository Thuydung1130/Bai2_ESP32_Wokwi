# Bài thực hành 2 – Mốc 2: MQTT đến InfluxDB Cloud 2

Project giữ nguyên firmware ESP32 của Mốc 1. Python nhận telemetry từ HiveMQ,
kiểm tra dữ liệu rồi ghi dữ liệu gốc vào InfluxDB Cloud 2 (Cloud TSM), AWS
`us-east-1`. Mốc này chưa tiền xử lý và chưa xây dựng dashboard.

## Kiến trúc

```text
ESP32/Wokwi
  ├─> ThingsBoard EU (giữ nguyên)
  └─> broker.hivemq.com:1883
        └─> Python mqtt_collector.py
              ├─ validate JSON, timestamp và miền giá trị
              ├─ phát hiện duplicate, restart và sequence gap
              └─> InfluxDB Cloud 2 / bucket của người dùng / sensor_raw
```

ESP32 publish mỗi 5 giây lên `iotlab2/dung059/esp32/telemetry`. JSON gồm
`temperature`, `humidity`, `distance_cm`, `rssi`, `sequence`, `uptime_s`,
`device_id` và `timestamp_ms`.

## Schema dữ liệu

- Measurement: `sensor_raw`
- Tag: `device_id`
- Float fields: `temperature`, `humidity`, `distance_cm`
- Integer fields: `rssi`, `sequence`, `uptime_s`, `latency_ms`
- Point timestamp: `timestamp_ms` của ESP32, precision milliseconds

`latency_ms` là thời gian Python nhận message trừ `timestamp_ms`. Message cũ
quá 24 giờ hoặc đi trước đồng hồ Python quá 60 giây bị từ chối.

InfluxDB nhận diện point bằng measurement, tag set và timestamp. Point trùng có
thể bị hợp nhất hoặc ghi đè. Collector nhớ 1.000 khóa gần nhất
`(device_id, sequence, timestamp_ms)` trong RAM và chỉ thêm khóa sau khi Cloud
xác nhận write thành công. Khi `sequence` và `uptime_s` cùng giảm, collector
ghi nhận ESP32 restart và vẫn chấp nhận bản ghi mới.

Retention do bucket và gói InfluxDB Cloud quyết định. Collector không thay đổi
retention; hãy dùng lựa chọn mà giao diện tài khoản hỗ trợ.

## 1. Tạo file `.env`

Trong Terminal PowerShell tại thư mục project:

```powershell
Copy-Item .env.example .env
```

Mở `.env` và điền đúng thông tin từ InfluxDB Cloud:

```dotenv
INFLUX_URL=https://us-east-1-1.aws.cloud2.influxdata.com
INFLUX_ORG=ten-organization-chinh-xac
INFLUX_BUCKET=ten-bucket-da-tao
INFLUX_TOKEN=api-token-that
```

- Giữ nguyên Cloud URL trên, không thêm `/api/v2`.
- `INFLUX_ORG` là tên hoặc ID organization trên Cloud.
- `INFLUX_BUCKET` là bucket đã tạo, ví dụ `iot_raw` nếu dùng tên cũ.
- Token phải có quyền ghi bucket đó.

Không đặt dấu ngoặc quanh giá trị. `.env` đã nằm trong `.gitignore`; không mở
file này khi chụp ảnh và không commit lên GitHub. Measurement mặc định là
`sensor_raw`; biến tùy chọn `INFLUX_MEASUREMENT` trong file mẫu có thể giữ
nguyên.

## 2. Cài thư viện Python

Project đã có `.venv`. Khi cần tạo lại:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r python\requirements.txt
```

Nếu PowerShell chặn activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Ba dependency là `paho-mqtt`, `influxdb-client` và `python-dotenv`.
`influxdb-client` dùng InfluxDB v2 write API tương thích Cloud TSM.

Chạy test:

```powershell
python -m unittest discover -s python -p "test_*.py" -v
```

Kết quả đúng hiện tại: `Ran 25 tests` và `OK`.

## 3. Chạy collector

Trong Terminal đã activate `.venv`:

```powershell
python python\mqtt_collector.py
```

Khi cấu hình Cloud đúng, Terminal hiển thị:

```text
InfluxDB connected: https://us-east-1-1.aws.cloud2.influxdata.com bucket=...
Starting MQTT collector; press Ctrl+C to stop
MQTT connected: broker.hivemq.com:1883
Subscribed: iotlab2/dung059/esp32/telemetry
```

Token không được đưa vào log. Nhấn `Ctrl+C` để dừng collector.

## 4. Chạy Wokwi và xác nhận ghi Cloud

Trong VS Code, mở Command Palette và chạy Wokwi Simulator. Chờ Serial Monitor
có `[HiveMQ] Publish: SUCCESS`. Terminal Python phải có:

```text
MQTT message received: topic=iotlab2/dung059/esp32/telemetry bytes=...
Stored: device_id=esp32-dung059 temperature=... humidity=... distance_cm=... latency_ms=... sequence=...
```

`Stored` chỉ xuất hiện sau khi write đồng bộ không báo lỗi.

**[CHỤP ẢNH BÁO CÁO]** Chụp VS Code và Terminal khi thấy kết nối MQTT, nhận
message và `Stored`. Không mở `.env`. Lưu `Hinh_03_Python_MQTT.png`.

Chú thích: *Hình 3. Chương trình Python thu thập, kiểm tra và lưu dữ liệu cảm
biến từ MQTT Broker vào InfluxDB Cloud.*

## 5. Kiểm tra trên InfluxDB Cloud

1. Đăng nhập InfluxDB Cloud.
2. Mở **Data Explorer**.
3. Chọn bucket đúng với `INFLUX_BUCKET`.
4. Chọn measurement `sensor_raw`.
5. Chọn `device_id = esp32-dung059` nếu giao diện yêu cầu.
6. Chọn `temperature`, `humidity` hoặc `distance_cm`.
7. Chọn **Past 15m**, rồi bấm **Submit**.
8. Chuyển sang **Table** nếu cần xem timestamp và từng field.

**[CHỤP ẢNH BÁO CÁO]** Chỉ chụp sau khi Data Explorer hiển thị dữ liệu thực
tế. Không mở trang API Token. Lưu `Hinh_04_InfluxDB_Cloud.png`.

Chú thích: *Hình 4. Dữ liệu cảm biến ESP32 được lưu và truy vấn trên InfluxDB
Cloud.*

Ảnh dự phòng: chụp cấu hình bucket và retention nếu giao diện gói Cloud hiển
thị; lưu `Hinh_05_InfluxDB_Cloud_Config.png` và che thông tin nhạy cảm.

## Lỗi thường gặp và giới hạn

- `Missing configuration`: kiểm tra `.env` và bốn biến `INFLUX_URL`,
  `INFLUX_ORG`, `INFLUX_BUCKET`, `INFLUX_TOKEN`.
- `INFLUX_URL must use https://`: Cloud URL phải dùng HTTPS.
- `InfluxDB ping failed`: kiểm tra Internet, URL region và firewall.
- HTTP 401/unauthorized: token sai, hết hiệu lực hoặc thiếu quyền ghi bucket.
- HTTP 404/not found: kiểm tra organization và bucket.
- Không thấy MQTT message: chạy Wokwi, kiểm tra Serial có
  `[HiveMQ] Publish: SUCCESS` và topic hai phía giống nhau.
- `Rejected MQTT message`: xem lý do trong log; thường do thiếu field, sai kiểu,
  timestamp chưa đồng bộ hoặc giá trị ngoài miền.
- `sequence gap`: có thể mất message QoS 0; collector vẫn lưu bản hiện tại.

HiveMQ port 1883 là broker công cộng, không mã hóa và không bảo đảm chuyển mọi
message. Collector chưa có hàng đợi bền vững; nếu Cloud write lỗi, bản ghi được
log là chưa lưu và không đánh dấu duplicate, nhưng QoS 0 có thể không phát lại.
Bộ nhớ duplicate mất khi collector restart và firmware chưa có `boot_id`.

## Mốc 3: tiền xử lý dữ liệu IoT

Script `python/preprocess.py` đọc `sensor_raw` bằng Flux Query API, giữ thời
gian UTC, làm sạch riêng từng `device_id`, tổng hợp theo cửa sổ 1 phút và ghi
sang measurement `sensor_processed`. Dữ liệu `sensor_raw` không bị sửa hoặc
xóa.

Quy trình xử lý:

1. Chuyển các field về kiểu số; NaN, Infinity và giá trị thiếu được nhận diện.
2. Xóa bản ghi trùng theo `device_id`, timestamp và sequence.
3. Nội suy tuyến tính tối đa 2 điểm cho temperature, humidity, distance_cm khi
   hai đầu hợp lệ cách nhau không quá 30 giây. Không nội suy sequence, uptime
   hoặc device ID.
4. Phát hiện IQR riêng từng thiết bị với ngưỡng `Q1 - 1.5×IQR` và
   `Q3 + 1.5×IQR`. Nhóm dưới 4 mẫu hoặc IQR bằng 0 không bị tự động gắn outlier.
5. Đánh dấu latency âm và loại nó khỏi `latency_mean_ms`, không đổi thành 0.
6. Resample 1 phút riêng từng thiết bị. Dữ liệu gốc gửi khoảng 5 giây/lần nên
   một cửa sổ đầy đủ thường có khoảng 12 mẫu.
7. Tạo rolling mean 3 cửa sổ, delta giữa hai cửa sổ liên tiếp và Min-Max
   normalization. Chuỗi hằng được chuẩn hóa thành 0 để tránh chia cho 0.

Chạy mặc định với 1 giờ gần nhất:

```powershell
python python\preprocess.py
```

Đổi khoảng truy vấn, ví dụ 6 giờ hoặc 2 ngày:

```powershell
python python\preprocess.py --range=-6h
python python\preprocess.py --range=-2d
```

Kiểm tra mà chưa ghi Cloud:

```powershell
python python\preprocess.py --range=-1h --dry-run
```

Thêm `--csv python/processed_result.csv` nếu cần xuất bảng phục vụ đối chiếu.
Báo cáo mặc định được tạo tại `python/preprocessing_report.json`.

Đọc báo cáo bằng PowerShell:

```powershell
Get-Content python\preprocessing_report.json
```

Schema `sensor_processed`:

- Tag: `device_id`.
- Timestamp: đầu cửa sổ UTC 1 phút.
- Fields: `temperature`, `humidity`, `distance_cm`, `temp_rolling_mean`,
  `humidity_rolling_mean`, `temp_delta`, `distance_delta`, `temp_normalized`,
  `humidity_normalized`, `sample_count`, `latency_mean_ms` và các bộ đếm lỗi.

Chạy lại cùng khoảng thời gian tạo cùng measurement, tag và timestamp, nên
InfluxDB cập nhật cùng point thay vì tạo thêm series hoặc timestamp mới.

**[CHỤP ẢNH BÁO CÁO]** Sau khi lệnh thật báo `Preprocessing complete`, chụp
Terminal thấy input, missing, outlier, windows và written. Lưu
`Hinh_05_Preprocessing_Result.png`.

Chú thích: *Hình 5. Kết quả làm sạch, phát hiện bất thường và tổng hợp dữ liệu
cảm biến IoT.*

Trong InfluxDB Cloud Data Explorer, chọn bucket `iot_raw`, measurement
`sensor_processed`, khoảng thời gian phù hợp và các field `temperature`,
`humidity`, `temp_rolling_mean`.

**[CHỤP ẢNH BÁO CÁO]** Khi dữ liệu xử lý thật xuất hiện, lưu
`Hinh_06_InfluxDB_Processed.png`.

Chú thích: *Hình 6. Dữ liệu cảm biến sau tiền xử lý được lưu trữ trong InfluxDB
Cloud.*

Ảnh đối chiếu tùy chọn `Hinh_07_Raw_vs_Processed.png` có thể chụp hai truy vấn
`sensor_raw` và `sensor_processed`; không mở `.env` hoặc trang API Token.

## Mốc 4: Streamlit Dashboard

Ứng dụng `python/dashboard.py` có bốn khu vực:

1. Tổng quan thiết bị bằng metric cards.
2. Biểu đồ real-time temperature, humidity và distance từ `sensor_raw`.
3. So sánh raw với `sensor_processed` và temperature rolling mean.
4. Đánh giá số bản ghi, số cửa sổ, latency hợp lệ, invalid latency, outlier và
   thời gian truy vấn Cloud.

Sidebar cho phép chọn 15 phút, 1 giờ, 6 giờ hoặc 24 giờ; chọn thiết bị; Refresh
thủ công; hoặc auto-refresh 10 giây. Dữ liệu Cloud được cache 10 giây để tránh
truy vấn dồn dập. Đường biểu đồ được chia đoạn khi có khoảng trống dài.

### Chạy dashboard trên Windows

Kiểm tra `.env` đã có URL, organization, bucket và token. Không mở `.env` khi
demo hoặc chụp ảnh. Trong Terminal PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r python\requirements.txt
python -m streamlit run python\dashboard.py
```

Mở URL Streamlit in ra, mặc định <http://localhost:8501>. Nếu processed chưa
mới, mở Terminal khác và chạy:

```powershell
.\.venv\Scripts\Activate.ps1
python python\preprocess.py --range=-1h
```

Sau đó bấm **Refresh ngay** trên dashboard. `sensor_raw` tiếp tục cập nhật khi
Wokwi và `python/mqtt_collector.py` đang chạy; `sensor_processed` chỉ cập nhật
khi chạy lại preprocessing.

Latency được trình bày là thời gian từ timestamp ESP32 tới lúc Python nhận
MQTT. Chỉ giá trị không âm tham gia min/max/mean. Vì pipeline không lưu
timestamp xác nhận write database, đây không phải latency end-to-end tới
InfluxDB.

Lỗi thường gặp:

- Cloud query error: kiểm tra Internet, region URL, organization, bucket và
  quyền token.
- Không có device: mở rộng time range và kiểm tra `sensor_raw` trên Cloud.
- Không có processed: chạy lại `preprocess.py` với cùng hoặc rộng hơn time
  range rồi Refresh.
- Port 8501 bận: chạy `python -m streamlit run python\dashboard.py --server.port 8502`.
- Thiếu package: activate đúng `.venv` rồi cài lại `requirements.txt`.

### Ảnh minh chứng Mốc 4

**[CHỤP ẢNH BÁO CÁO]** Chọn thiết bị, khoảng thời gian có dữ liệu và chụp tiêu
đề, metric cards cùng biểu đồ real-time. Lưu `Hinh_07_IoT_Dashboard.png`.

Chú thích: *Hình 7. Dashboard giám sát dữ liệu cảm biến IoT theo thời gian thực.*

**[CHỤP ẢNH BÁO CÁO]** Cuộn tới Raw vs Processed, chụp chart có temperature
raw, temperature processed và rolling mean. Lưu `Hinh_08_Raw_vs_Processed.png`.

Chú thích: *Hình 8. So sánh dữ liệu cảm biến trước và sau tiền xử lý.*

**[CHỤP ẢNH BÁO CÁO]** Cuộn tới Đánh giá hệ thống, chụp average/min/max
latency, invalid latency và số bản ghi. Lưu `Hinh_09_Latency_Evaluation.png`.

Chú thích: *Hình 9. Kết quả thống kê độ trễ và đánh giá hệ thống thu thập dữ
liệu IoT.*
### Kiểm tra latency Mốc 4

Collector đóng dấu thời gian nhận ngay trong callback MQTT rồi chuyển bản tin
sang hàng đợi của worker ghi InfluxDB. Thời gian chờ một lần ghi Cloud vì thế
không làm trễ timestamp nhận của bản tin MQTT tiếp theo. Giá trị vẫn phụ thuộc
vào độ đồng bộ giữa NTP của ESP32 và đồng hồ Windows; hãy bật **Set time
automatically** và bấm **Sync now** trước khi đo. Dashboard giữ nguyên mọi giá
trị latency cao đã lưu, không tự sửa hoặc loại chúng khỏi thống kê.

`latency_ms` chỉ đo từ timestamp ESP32 đến lúc callback Python nhận bản tin.
Pipeline chưa lưu thời điểm InfluxDB xác nhận ghi, nên chỉ số này không phải độ
trễ end-to-end đến database.

