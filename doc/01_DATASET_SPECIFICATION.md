# ĐẶC TẢ DỮ LIỆU (DATASET SPECIFICATION)

> **Tệp dữ liệu gốc:** `flight_data_2024.csv`  
> **Đường dẫn thư mục:** [`Flight Delay Dataset — 2024/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024)  
> **Nguồn:** [Kaggle - Flight Data 2024 (Hrishit Patil)](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) / BTS DOT Hoa Kỳ.

---

## 1. THÔNG SỐ CƠ BẢN

* **Tổng số bản ghi (Rows):** `7,079,081` (Hơn 7.07 triệu dòng dữ liệu chuyến bay).
* **Số trường thông tin (Columns):** `35` thuộc tính.
* **Dung lượng file CSV thô:** `~1.22 GB` (`1,309,010,752` bytes).
* **Tệp mẫu kiểm thử:** `flight_data_2024_sample.csv` (`10,000` dòng, `1.85 MB`).

---

## 2. BẢNG MÃ TỪ ĐIỂN DỮ LIỆU (DATA DICTIONARY)

| STT | Tên cột | Kiểu dữ liệu | Tỷ lệ Null (%) | Ví dụ | Ý nghĩa nghiệp vụ |
| :---: | :--- | :--- | :---: | :--- | :--- |
| 1 | `year` | Int64 | 0.0% | `2024` | Năm diễn ra chuyến bay |
| 2 | `month` | Int64 | 0.0% | `1` | Tháng trong năm (1 - 12) |
| 3 | `day_of_month` | Int64 | 0.0% | `18` | Ngày trong tháng (1 - 31) |
| 4 | `day_of_week` | Int64 | 0.0% | `4` | Thứ trong tuần (1 = Thứ 2, ..., 7 = Chủ nhật) |
| 5 | `fl_date` | Date / String | 0.0% | `2024-04-18` | Ngày bay đầy đủ định dạng YYYY-MM-DD |
| 6 | `op_unique_carrier` | String | 0.0% | `AA` | Mã hãng hàng không khai thác (IATA code) |
| 7 | `op_carrier_fl_num` | Float64 / String | 0.0% | `3535.0` | Số hiệu chuyến bay |
| 8 | `origin` | String | 0.0% | `DFW` | Mã IATA của sân bay xuất phát |
| 9 | `origin_city_name` | String | 0.0% | `Dallas/Fort Worth, TX` | Tên thành phố và bang xuất phát |
| 10 | `origin_state_nm` | String | 0.0% | `Texas` | Tên đầy đủ của bang xuất phát |
| 11 | `dest` | String | 0.0% | `RAP` | Mã IATA của sân bay đích |
| 12 | `dest_city_name` | String | 0.0% | `Rapid City, SD` | Tên thành phố và bang đến |
| 13 | `dest_state_nm` | String | 0.0% | `South Dakota` | Tên đầy đủ của bang đến |
| 14 | `crs_dep_time` | Int64 | 0.0% | `1018` | Giờ khởi hành theo lịch (HHMM, ví dụ: 10:18) |
| 15 | `dep_time` | Float64 | 1.31% | `1015.0` | Giờ khởi hành thực tế (HHMM, rỗng nếu bị hủy) |
| 16 | `dep_delay` | Float64 | 1.31% | `-3.0` | Số phút chênh lệch cất cánh (+: trễ, -: sớm) |
| 17 | `taxi_out` | Float64 | 1.35% | `21.0` | Thời gian lăn bánh từ cửa ga ra đường băng (phút) |
| 18 | `wheels_off` | Float64 | 1.35% | `1036.0` | Giờ bánh máy bay rời mặt đất (HHMM) |
| 19 | `wheels_on` | Float64 | 1.38% | `1135.0` | Giờ bánh máy bay chạm đường băng đến (HHMM) |
| 20 | `taxi_in` | Float64 | 1.38% | `4.0` | Thời gian lăn từ đường băng vào cổng ga (phút) |
| 21 | `crs_arr_time` | Int64 | 0.0% | `1149` | Giờ hạ cánh dự kiến theo lịch (HHMM) |
| 22 | `arr_time` | Float64 | 1.38% | `1139.0` | Giờ hạ cánh thực tế (HHMM) |
| 23 | `arr_delay` | Float64 | 1.61% | `-10.0` | **Độ trễ hạ cánh (phút)** — Chỉ số xác định hoãn |
| 24 | `cancelled` | Int64 | 0.0% | `0` | Cờ hủy chuyến (0: không hủy, 1: bị hủy) |
| 25 | `cancellation_code` | String | 98.64% | `B` | Lý do hủy: A (Carrier), B (Weather), C (NAS), D (Security) |
| 26 | `diverted` | Int64 | 0.0% | `0` | Cờ chuyển hướng sân bay (0: không, 1: có) |
| 27 | `crs_elapsed_time` | Float64 | 0.0% | `151.0` | Thời gian hành trình dự kiến (gate-to-gate, phút) |
| 28 | `actual_elapsed_time`| Float64 | 1.61% | `144.0` | Thời gian hành trình thực tế (phút) |
| 29 | `air_time` | Float64 | 1.61% | `119.0` | Thời gian bay thực trên không (phút) |
| 30 | `distance` | Float64 | 0.0% | `835.0` | Khoảng cách bay giữa 2 sân bay (dặm - miles) |
| 31 | `carrier_delay` | Int64 / Float | 0.0% | `0` | Số phút trễ do lỗi hãng hàng không |
| 32 | `weather_delay` | Int64 / Float | 0.0% | `0` | Số phút trễ do điều kiện thời tiết |
| 33 | `nas_delay` | Int64 / Float | 0.0% | `0` | Số phút trễ do điều phối không lưu quốc gia |
| 34 | `security_delay` | Int64 / Float | 0.0% | `0` | Số phút trễ do sự cố an ninh |
| 35 | `late_aircraft_delay`| Int64 / Float | 0.0% | `0` | Số phút trễ dây chuyền do tàu bay chặng trước trễ |

---

## 3. ĐỊNH NGHĨA BIẾN MỤC TIÊU (TARGET LABELING)

Theo đề cương đề tài: **Dự đoán nguyên nhân trễ chuyến bay thương mại**.

1. **Quy định trễ chuyến:**
   * Theo chuẩn FAA / DOT Hoa Kỳ, một chuyến bay được coi là trễ chuyến chính thức nếu:
     $$\text{is\_delayed} = 1 \iff \text{arr\_delay} \ge 15 \text{ phút}$$
2. **Gán nhãn nguyên nhân trễ chính (Multi-class Classification):**
   * Khi một chuyến bay bị trễ ($\ge 15$ phút), thời gian trễ sẽ được phân rã thành 5 nguyên nhân:
     1. `Carrier` (Hãng bay)
     2. `Weather` (Thời tiết)
     3. `NAS` (Không lưu quốc gia)
     4. `Security` (An ninh)
     5. `LateAircraft` (Tàu bay đến muộn)
   * Trường hợp chuyến bay có nhiều nguyên nhân đồng thời: Nhãn đại diện sẽ được gán cho nguyên nhân có **thời gian đóng góp trễ lớn nhất** (`argmax(carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay)`).

---

## 4. QUY TẮC PHÒNG TRÁNH RÒ RỈ DỮ LIỆU (DATA LEAKAGE PREVENTION)

> [!CAUTION]
> **Cực kỳ quan trọng cho các AI Agent khi xây dựng pipeline huấn luyện:**  
> Nếu mục tiêu của mô hình là dự báo trước khi chuyến bay diễn ra (Pre-flight Prediction), bạn **KHÔNG ĐƯỢC** đưa vào các biến chỉ phát sinh trong hoặc sau khi chuyến bay cất cánh.

### Các thuộc tính ĐƯỢC PHÉP dùng làm đặc trưng (Features):
* `month`, `day_of_month`, `day_of_week`
* `crs_dep_time` (Đã chuẩn hóa / binning khung giờ: Sáng, Trưa, Chiều, Tối)
* `crs_arr_time`
* `op_unique_carrier`
* `origin`, `origin_city_name`, `origin_state_nm`
* `dest`, `dest_city_name`, `dest_state_nm`
* `distance`, `crs_elapsed_time`

### Các thuộc tính CẤM DÙNG làm đặc trưng đầu vào (Data Leakage):
* `dep_time`, `dep_delay`, `wheels_off`, `taxi_out` (Đã diễn ra khi cất cánh)
* `arr_time`, `wheels_on`, `taxi_in`, `actual_elapsed_time`, `air_time` (Chỉ có khi kết thúc chuyến bay)
* `cancelled`, `cancellation_code`, `diverted`
* `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay` (Đây là các thành phần của Target, chỉ dùng để tạo nhãn).

---

## 5. BẢNG QUY CHUẨN TỐI ƯU HÓA KIỂU DỮ LIỆU (DOWNCASTING MEMORY SPECIFICATION)

Để xử lý trọn vẹn toàn bộ 7.07 triệu dòng dữ liệu trên máy tính cá nhân (16GB RAM) mà không gây tràn bộ nhớ, hệ thống áp dụng kỹ thuật **Downcasting (Chuyển đổi kiểu dữ liệu tương thích thấp nhất)**. Đây là kỹ thuật bảo toàn dữ liệu 100% không suy hao (Lossless Transformation):

| Tên trường dữ liệu | Dải giá trị thực tế | Kiểu mặc định (64-bit) | Kiểu tối ưu (Downcasted) | Dung lượng mỗi dòng | Tỷ lệ tiết kiệm RAM | Đảm bảo tính toàn vẹn (Lossless Proof) |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| `month` | $1 \to 12$ | `int64` (8 bytes) | `int8` (1 byte) | 1 byte | Giảm **87.5%** | `int8` chứa từ $-128 \to 127$. Tháng $1 \to 12$ hoàn toàn chính xác. |
| `day_of_month` | $1 \to 31$ | `int64` (8 bytes) | `int8` (1 byte) | 1 byte | Giảm **87.5%** | $1 \to 31$ nằm gọn trong $[-128, 127]$. |
| `day_of_week` | $1 \to 7$ | `int64` (8 bytes) | `int8` (1 byte) | 1 byte | Giảm **87.5%** | $1 \to 7$ nằm gọn trong $[-128, 127]$. |
| `dep_hour` / `arr_hour` | $0 \to 23$ | `int64` (8 bytes) | `int8` (1 byte) | 1 byte | Giảm **87.5%** | $0 \to 23$ nằm gọn trong $[-128, 127]$. |
| `cancelled` / `diverted` | $0 \text{ hoặc } 1$ | `int64` (8 bytes) | `int8` (1 byte) | 1 byte | Giảm **87.5%** | Cờ nhị phân $0/1$. |
| `crs_dep_time` / `crs_arr_time` | $0 \to 2400$ | `int64` (8 bytes) | `int16` (2 bytes) | 2 bytes | Giảm **75.0%** | `int16` chứa tới $32.767$. Định dạng HHMM ($\le 2400$) an toàn tuyệt đối. |
| `distance` | $31 \to 5.095$ dặm | `float64` (8 bytes) | `int16` (2 bytes) | 2 bytes | Giảm **75.0%** | Khoảng cách nội địa Mỹ max $\approx 5.100$ dặm, nằm an toàn trong $[0, 32.767]$. |
| `crs_elapsed_time` | $20 \to 700$ phút | `float64` (8 bytes) | `int16` (2 bytes) | 2 bytes | Giảm **75.0%** | Thời gian bay tối đa $\approx 700$ phút, an toàn trong $[0, 32.767]$. |
| `dep_delay` / `arr_delay` | $-100 \to 2.500$ phút | `float64` (8 bytes) | `float32` (4 bytes) | 4 bytes | Giảm **50.0%** | `float32` có độ chính xác 7 chữ số có nghĩa, bảo toàn từng phút trễ. |
| `carrier_delay` ... `late_aircraft_delay` | $0 \to 2.500$ phút | `float64` (8 bytes) | `float32` (4 bytes) | 4 bytes | Giảm **50.0%** | Độ trễ phân rã bảo toàn chính xác. |
| `op_unique_carrier` | 15 hãng bay | `object` (> 32 bytes) | `category` (1 byte) | 1 byte | Giảm **> 95%** | 15 hãng bay được mã hóa dạng số nguyên nhỏ (Dictionary Lookup). |
| `origin` / `dest` | 310 sân bay IATA | `object` (> 32 bytes) | `category` (2 bytes) | 2 bytes | Giảm **> 90%** | 310 sân bay được đánh số từ $0 \to 309$ trong từ điển chuỗi. |
| `dep_time_of_day` | 4 khung giờ | `object` (> 32 bytes) | `category` (1 byte) | 1 byte | Giảm **> 95%** | 4 giá trị Morning, Afternoon, Evening, Night. |

> **Tổng kết hiệu quả:** Toàn bộ bảng 7.079.081 dòng khi nạp vào RAM giảm dung lượng từ **~7.5 GB - 8.5 GB** xuống chỉ còn **~2.2 GB - 2.8 GB**, giúp toàn bộ quá trình tiền xử lý chạy trọn vẹn trong RAM mà không cần dùng đến ổ cứng ảo Swap/Pagefile.

