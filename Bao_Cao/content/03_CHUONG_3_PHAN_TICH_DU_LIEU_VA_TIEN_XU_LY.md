# CHƯƠNG 3: PHÂN TÍCH DỮ LIỆU VÀ TIỀN XỬ LÝ DỮ LIỆU LỚN

Chương này trình bày chi tiết về nguồn gốc, cấu trúc và đặc điểm của bộ dữ liệu hàng không thương mại nội địa Hoa Kỳ năm 2024, các phát hiện từ quá trình phân tích khám phá dữ liệu (EDA), giải pháp tối ưu hóa lưu trữ phân tán với Apache Parquet, phương pháp xây dựng biến mục tiêu phân loại 5 nhóm nguyên nhân trễ và kỹ thuật trích xuất đặc trưng loại bỏ triệt để hiện tượng rò rỉ dữ liệu (Data Leakage).

---

## 3.1. Giới thiệu bộ dữ liệu hàng không Hoa Kỳ 2024

### 3.1.1. Nguồn gốc dữ liệu
Bộ dữ liệu được thu thập từ nguồn dữ liệu mở chính thức của **Cục Thống kê Vận tải Hoa Kỳ (Bureau of Transportation Statistics - BTS)** thuộc Bộ Giao thông Vận tải Hoa Kỳ (USDOT), được tổng hợp và xuất bản thông qua bộ *Flight Delay Dataset 2024*. Đây là nguồn dữ liệu chuẩn mực và có độ tin cậy cao nhất, ghi nhận thông tin khai thác của tất cả các chuyến bay thương mại nội địa có chứng chỉ khai thác tại Hoa Kỳ.

### 3.1.2. Quy mô dữ liệu
* **Tổng số lượng bản ghi:** Chính xác **7,079,081 chuyến bay** được ghi nhận trong toàn bộ 12 tháng của năm 2024.
* **Số lượng thuộc tính gốc:** **35 thuộc tính** đo lường chi tiết mọi khía cạnh của chuyến bay.
* **Dung lượng tệp văn bản thô (CSV):** Khoảng **2.3 GB**, đặt ra thách thức lớn đối với việc đọc, tiền xử lý và lưu giữ trong bộ nhớ RAM của một máy trạm đơn lẻ.

### 3.1.3. Từ điển thuộc tính dữ liệu gốc
Bảng 3.1 liệt kê và giải thích ý nghĩa nghiệp vụ của các thuộc tính cốt lõi trong bộ dữ liệu gốc:

*Bảng 3.1: Danh mục các thuộc tính cốt lõi trong bộ dữ liệu BTS 2024*

| Tên thuộc tính | Kiểu dữ liệu | Ý nghĩa nghiệp vụ | Phân loại trong bài toán |
| :--- | :--- | :--- | :--- |
| `FlightDate` | Date (YYYY-MM-DD) | Ngày thực hiện chuyến bay | Thuộc tính thời gian |
| `Month` | Integer (1 - 12) | Tháng trong năm | Thuộc tính mùa vụ |
| `DayOfWeek` | Integer (1 - 7) | Ngày trong tuần (1 = Thứ Hai, 7 = Chủ Nhật) | Thuộc tính chu kỳ tuần |
| `Operating_Airline` | String / Varchar | Mã định danh hãng hàng không vận hành (IATA Code) | Biến phân loại (Categorical) |
| `Origin` | String (3 ký tự) | Mã sân bay khởi hành theo tiêu chuẩn IATA | Biến phân loại không gian |
| `Dest` | String (3 ký tự) | Mã sân bay đến theo tiêu chuẩn IATA | Biến phân loại không gian |
| `CRSDepTime` | Integer (HHMM) | Giờ khởi hành dự kiến theo lịch trình (CRS) | Đặc trưng đầu vào hợp lệ |
| `DepTime` | Float (HHMM) | Giờ khởi hành thực tế | Biến vận hành thực tế |
| `DepDelayMinutes` | Float (Phút) | Thời gian trễ khởi hành tính bằng phút ($\ge 0$) | Biến xác định ngưỡng trễ |
| `CRSArrTime` | Integer (HHMM) | Giờ đến dự kiến theo lịch trình (CRS) | Đặc trưng đầu vào hợp lệ |
| `ArrTime` | Float (HHMM) | Giờ đến thực tế | Biến kết thúc chuyến bay |
| `ArrDelayMinutes` | Float (Phút) | Thời gian trễ hạ cánh tính bằng phút ($\ge 0$) | Biến đánh giá trễ đến |
| `Distance` | Float (Dặm) | Khoảng cách đường bay giữa sân bay đi và đến | Đặc trưng số học hợp lệ |
| `Cancelled` | Binary (0 / 1) | Chuyến bay bị hủy chuyến (1 = Bị hủy, 0 = Khai thác) | Biến lọc dữ liệu |
| `Diverted` | Binary (0 / 1) | Chuyến bay bị chuyển hướng hạ cánh sân bay khác | Biến lọc dữ liệu |
| `AirTime` | Float (Phút) | Thời gian thực tế máy bay bay trên không | **Biến rò rỉ (Data Leakage)** |
| `ActualElapsedTime`| Float (Phút) | Tổng thời gian thực tế từ cổng đi đến cổng đến | **Biến rò rỉ (Data Leakage)** |
| `TaxiOut` | Float (Phút) | Thời gian máy bay lăn từ cửa ra đến khi cất cánh | **Biến rò rỉ (Data Leakage)** |
| `TaxiIn` | Float (Phút) | Thời gian máy bay lăn từ khi hạ cánh vào cửa đỗ | **Biến rò rỉ (Data Leakage)** |
| `CarrierDelay` | Float (Phút) | Số phút trễ do lỗi hãng hàng không | Nhóm tạo biến mục tiêu |
| `WeatherDelay` | Float (Phút) | Số phút trễ do thời tiết cực đoan | Nhóm tạo biến mục tiêu |
| `NASDelay` | Float (Phút) | Số phút trễ do hệ thống không lưu quốc gia | Nhóm tạo biến mục tiêu |
| `SecurityDelay` | Float (Phút) | Số phút trễ do an ninh | Nhóm tạo biến mục tiêu |
| `LateAircraftDelay`| Float (Phút) | Số phút trễ do máy bay chặng trước đến muộn | Nhóm tạo biến mục tiêu |

---

## 3.2. Khảo sát và phân tích khám phá dữ liệu (EDA)

Quá trình phân tích khám phá dữ liệu (EDA) trên toàn bộ 7.08 triệu dòng dữ liệu đã làm sáng tỏ nhiều đặc điểm phân phối quan trọng:

### 3.2.1. Tỷ lệ trễ chuyến và phân tích dữ liệu bất thường
1. **Chuyến bay bị hủy và chuyển hướng:**
   * Trong 7,079,081 chuyến bay, có khoảng **1.5% chuyến bay bị hủy (`Cancelled == 1`)** và **0.25% chuyến bay bị chuyển hướng (`Diverted == 1`)**.
   * Những chuyến bay này không hoàn thành hành trình bình thường, do đó không có thông tin về thời gian trễ hạ cánh và các nguyên nhân phân rã. Nhóm quyết định loại bỏ hoàn toàn các bản ghi này khỏi tập dữ liệu huấn luyện phân loại nguyên nhân trễ.
2. **Quy ước trễ chuyến bay chuẩn (15-Minute Rule):**
   * Theo quy định quốc tế của FAA và BTS, một chuyến bay chỉ được xem là trễ chuyến chính thức (Flight Delay) khi thời gian trễ giờ đến hoặc trễ giờ khởi hành **từ 15 phút trở lên** ($\text{DepDelayMinutes} \ge 15$ hoặc $\text{ArrDelayMinutes} \ge 15$). Các chuyến bay có độ lệch dưới 15 phút được quy ước là chuyến bay đúng giờ (On-time).
   * Trong số các chuyến bay khai thác hoàn tất, tỷ lệ chuyến bay trễ $\ge 15$ phút chiếm khoảng **21.8%** (xấp xỉ 1.5 triệu chuyến bay). Đối với các chuyến bay trễ này, BTS mới tiến hành phân rã và ghi nhận chi tiết 5 cột số phút trễ thành phần (`CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`).

### 3.2.2. Phân bố theo thời gian và không gian
* **Phân bố theo mùa vụ (Month):** Tỷ lệ trễ chuyến tăng đột biến vào hai giai đoạn cao điểm trong năm 2024: giai đoạn mùa hè (Tháng 6, Tháng 7) do mật độ bay du lịch tăng cao kết hợp bão nhiệt đới; và giai đoạn mùa đông (Tháng 12, Tháng 1) do bão tuyết và điều kiện đóng băng tại các sân bay miền Bắc Hoa Kỳ.
* **Phân bố theo ngày trong tuần (Day of Week):** Thứ Sáu và Chủ Nhật là hai ngày có mật độ chuyến bay và tỷ lệ trễ cao nhất trong tuần, phản ánh lưu lượng di chuyển công tác và du lịch cuối tuần. Ngược lại, Thứ Ba và Thứ Tư là hai ngày có tỷ lệ đúng giờ cao nhất.
* **Mật độ tại các sân bay cửa ngõ (Hub Airports):** 10 sân bay lớn nhất (bao gồm ATL, ORD, DFW, DEN, CLT, LAX, JFK, LAS, MCO, PHX) phục vụ hơn 45% tổng lưu lượng bay toàn quốc. Hiện tượng trễ chuyến tại các sân bay này có sức lan tỏa dây chuyền rất mạnh mẽ sang toàn bộ các sân bay vệ tinh còn lại.

---

## 3.3. Tổ chức lưu trữ dữ liệu tối ưu với Apache Parquet

Một trong những đóng góp kỹ thuật quan trọng của đồ án về mặt công nghệ Big Data là chuyển đổi toàn bộ tập dữ liệu từ định dạng văn bản CSV truyền thống sang định dạng cột phân tán **Apache Parquet nén Snappy**.

### 3.3.1. So sánh hiệu quả lưu trữ giữa CSV và Parquet Snappy
Dữ liệu 7.08 triệu dòng ban đầu được lưu trữ dưới dạng một tệp CSV duy nhất với dung lượng 2.3 GB. Nhóm đã thực hiện chuyển đổi và đo đạc kết quả lưu trữ:

*Bảng 3.2: So sánh hiệu quả lưu trữ giữa CSV và Apache Parquet Snappy*

| Tiêu chí so sánh | Tệp gốc CSV | Định dạng Apache Parquet Snappy | Mức độ cải thiện |
| :--- | :---: | :---: | :---: |
| **Dung lượng lưu trữ đĩa** | **2,348 MB (~2.3 GB)** | **409.8 MB (~0.41 GB)** | **Giảm 82.5% dung lượng** |
| **Cấu trúc lưu trữ** | Dạng dòng (Row-oriented) | Dạng cột (Column-oriented) | Hỗ trợ đọc chọn lọc cột |
| **Kiểu nén** | Không nén (Uncompressed text) | Snappy Compression | Tối ưu hóa CPU & I/O |
| **Bảo toàn lược đồ (Schema)** | Không (mỗi lần đọc phải đoán kiểu) | Có (Schema Metadata nhúng sẵn) | Tránh sai lệch kiểu dữ liệu |
| **Thời gian đọc trích xuất 10 cột** | 18.42 giây | 2.15 giây | **Nhanh hơn 8.5 lần** |

### 3.3.2. Chiến lược phân vùng dữ liệu theo tháng (`partitionBy("Month")`)
Để tối ưu hóa hơn nữa cho các truy vấn phân tích chuỗi thời gian, nhóm áp dụng chiến lược phân vùng thư mục theo tháng:
```python
# Đoạn mã chuyển đổi và phân vùng Parquet trong PySpark
df_cleaned.write \
    .partitionBy("Month") \
    .mode("overwrite") \
    .parquet("hdfs://master:9000/data/flights_2024_parquet/")
```
Cấu trúc thư mục được tổ chức thành 12 thư mục con riêng biệt (`Month=1/`, `Month=2/`, ..., `Month=12/`). Khi một truy vấn hoặc mô hình chỉ cần phân tích dữ liệu của một quý hoặc một tháng cụ thể, cơ chế **Partition Pruning** của Apache Spark sẽ trực tiếp bỏ qua các thư mục của các tháng còn lại, giảm thiểu triệt để số lượng khối dữ liệu phải đọc từ đĩa hoặc mạng.

---

## 3.4. Tiền xử lý dữ liệu quy mô lớn (PySpark & Polars)

Để phục vụ hai nhánh nghiên cứu (Đơn máy tối ưu và Cụm Spark phân tán), quy trình tiền xử lý được chuẩn hóa song song:
1. **Lọc dữ liệu rác và dữ liệu ngoại lai phi lý:**
   * Loại bỏ các chuyến bay có trạng thái `Cancelled == 1` hoặc `Diverted == 1`.
   * Loại bỏ các chuyến bay có thời gian bay âm hoặc khoảng cách bay bằng 0.
   * Lọc lấy tập dữ liệu các chuyến bay có sự cố trễ ($\text{DepDelayMinutes} \ge 15$ hoặc $\text{ArrDelayMinutes} \ge 15$) để tiến hành phân loại nguyên nhân.
2. **Xử lý giá trị khuyết thiếu (Null/Missing Values):**
   * Đối với 5 cột số phút trễ thành phần (`CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`), các bản ghi có giá trị `Null` được điền tự động bằng giá trị `0.0`.
   * Đối với các thuộc tính phân loại mã sân bay hoặc hãng bay, các bản ghi bị khuyết thông tin chiếm tỷ lệ cực nhỏ (<0.01%) được loại bỏ trực tiếp.
3. **Ép kiểu dữ liệu tối ưu (Data Type Downcasting):**
   * Chuyển đổi các cột số nguyên lớn sang `Int16` hoặc `Int8` (ví dụ `Month`: 1 - 12; `DayOfWeek`: 1 - 7).
   * Chuyển đổi các cột số thực sang `Float32` thay vì `Float64` mặc định, giúp tiết kiệm chính xác 50% dung lượng RAM trong suốt quá trình xử lý.

---

## 3.5. Xây dựng biến mục tiêu và phân loại 5 nhóm nguyên nhân trễ

### 3.5.1. Thách thức trễ đa nguyên nhân (Multi-causal Delays)
Trong thực tế khai thác bay, một chuyến bay có thể bị trễ đồng thời do nhiều nguyên nhân khác nhau (ví dụ: máy bay đến trễ 20 phút do chặng trước, sau đó mất thêm 15 phút bảo trì kỹ thuật của hãng bay và thêm 10 phút chờ đường băng do không lưu). Khi đó, các cột trễ thành phần đều có giá trị dương: `LateAircraftDelay = 20`, `CarrierDelay = 15`, `NASDelay = 10`.

Để chuyển bài toán về bài toán phân loại đa lớp đơn nhãn (Single-label Multi-class Classification) phục vụ việc ra quyết định can thiệp trọng tâm, nhóm thiết lập nguyên tắc **Nguyên nhân chi phối lớn nhất (Dominant Delay Cause)**:

$$\text{Dominant Cause} = \arg\max_{c \in \mathcal{C}} (\text{DelayMinutes}_c)$$
với $\mathcal{C} = \{\text{Carrier}, \text{Weather}, \text{NAS}, \text{Security}, \text{LateAircraft}\}$.

Trong trường hợp có hai nguyên nhân có số phút trễ bằng nhau tuyệt đối, thứ tự ưu tiên nghiệp vụ được áp dụng: `Carrier` > `Weather` > `NAS` > `LateAircraft` > `Security`.

### 3.5.2. Mã hóa biến mục tiêu (`delay_cause_label`)
Biến mục tiêu được mã hóa thành các số nguyên từ 0 đến 4:
* **Lớp 0 - `Carrier`:** Trễ do hãng hàng không.
* **Lớp 1 - `Weather`:** Trễ do thời tiết cực đoan.
* **Lớp 2 - `NAS`:** Trễ do hệ thống không lưu quốc gia.
* **Lớp 3 - `Security`:** Trễ do an ninh sân bay.
* **Lớp 4 - `LateAircraft`:** Trễ do máy bay đến muộn từ chặng trước.

### 3.5.3. Phân tích hiện tượng mất cân bằng lớp (Class Imbalance)
Kết quả thống kê phân bố 5 lớp trên toàn bộ tập dữ liệu trễ năm 2024 cho thấy mức độ mất cân bằng rất nghiêm trọng:
* **LateAircraftDelay (Lớp 4):** Chiếm tỷ lệ cao nhất (~38.5% tổng số ca trễ) — phản ánh tính chất dây chuyền lan truyền chậm trễ đặc trưng của ngành hàng không khi một máy bay phải thực hiện từ 4 đến 6 chặng bay mỗi ngày.
* **CarrierDelay (Lớp 0):** Chiếm tỷ lệ lớn thứ hai (~31.2%).
* **NASDelay (Lớp 2):** Chiếm khoảng ~23.8%.
* **WeatherDelay (Lớp 1):** Chiếm khoảng ~6.1% (thời tiết tuy chiếm tỷ lệ số ca không lớn nhưng mỗi khi xảy ra thường có số phút trễ rất nghiêm trọng).
* **SecurityDelay (Lớp 3):** Chiếm tỷ lệ cực nhỏ (<0.4%) — là lớp siêu thiểu số (Extreme Minority Class).

Hiện tượng mất cân bằng này là lý do giải thích vì sao nhóm bắt buộc phải sử dụng **Macro F1-Score** và **Ma trận nhầm lẫn** bên cạnh chỉ số Accuracy để đánh giá mô hình một cách khách quan nhất.

---

## 3.6. Trích xuất đặc trưng và kỹ thuật chống rò rỉ dữ liệu (Data Leakage)

### 3.6.1. Hiểm họa rò rỉ dữ liệu (Data Leakage) trong bài toán dự đoán bay
Một trong những lỗi nghiêm trọng nhất thường gặp trong các nghiên cứu học máy về hàng không là hiện tượng **Rò rỉ dữ liệu (Data Leakage)** — tức là đưa vào tập đặc trưng huấn luyện những thông tin mà trên thực tế tại thời điểm cần dự đoán chúng ta chưa thể biết được.

Nếu đưa các thuộc tính như `ArrTime` (Giờ đến thực tế), `ActualElapsedTime` (Thời gian bay thực tế), `TaxiIn`, `TaxiOut` hoặc chính 5 cột số phút trễ thành phần (`CarrierDelay`, v.v.) vào làm đặc trưng đầu vào, mô hình sẽ đạt độ chính xác ảo lên đến 99% - 100%, nhưng hoàn toàn vô giá trị trong thực tế vì khi máy bay chuẩn bị cất cánh, các sự kiện này chưa hề xảy ra.

### 3.6.2. Danh mục đặc trưng đầu vào hợp lệ (Pre-flight Features)
Để đảm bảo mô hình có tính ứng dụng thực tế cao (có thể dự đoán trước giờ bay từ 2 - 6 tiếng), nhóm chỉ lựa chọn các thuộc tính có thể biết trước theo lịch trình:
1. **Đặc trưng lịch trình thời gian:**
   * `Month`: Tháng trong năm (Mùa vụ).
   * `DayOfWeek`: Ngày trong tuần (Chu kỳ công tác/cuối tuần).
   * `DepHour`: Giờ cất cánh dự kiến theo lịch trình (trích xuất từ `CRSDepTime` / 100). Khung giờ bay trong ngày ảnh hưởng lớn đến nguy cơ tắc nghẽn (buổi chiều tối luôn trễ hơn sáng sớm).
   * `ArrHour`: Giờ hạ cánh dự kiến theo lịch trình (trích xuất từ `CRSArrTime` / 100).
2. **Đặc trưng tuyến bay và không gian:**
   * `Origin`: Mã sân bay xuất phát (được mã hóa số).
   * `Dest`: Mã sân bay đến (được mã hóa số).
   * `Distance`: Khoảng cách đường bay (dặm).
   * `DistanceGroup`: Phân nhóm cự ly bay (Chặng ngắn <500 dặm, Trung bình 500-1500 dặm, Chặng dài >1500 dặm).
3. **Đặc trưng hãng khai thác:**
   * `Operating_Airline`: Hãng hàng không vận hành (ảnh hưởng bởi quy trình bảo dưỡng và năng lực đội bay của từng hãng).

---

## 3.7. Chiến lược phân chia tập dữ liệu

Để phục vụ quá trình huấn luyện và thẩm định mô hình đạt chuẩn mực khoa học, toàn bộ tập dữ liệu hơn 7 triệu dòng (sau khi làm sạch) được phân chia ngẫu nhiên có bảo toàn tỷ lệ nhãn (Stratified Split) theo tỷ lệ chuẩn:
* **Tập Huấn luyện (Training Set - 70%):** Khoảng **4,955,357 dòng**, được sử dụng độc quyền để các mô hình học các trọng số và cấu trúc cây quyết định.
* **Tập Kiểm định (Validation Set - 15%):** Khoảng **1,061,862 dòng**, được sử dụng để tinh chỉnh siêu tham số (Hyperparameter Tuning) và lựa chọn ngưỡng dừng sớm (Early Stopping) cho các mô hình Boosting.
* **Tập Kiểm tra độc lập (Test Set - 15%):** Khoảng **1,061,862 dòng**, được giữ kín hoàn toàn trong suốt quá trình huấn luyện và chỉ được sử dụng một lần duy nhất ở bước cuối cùng để đo đạc và đánh giá khách quan hiệu năng của tất cả các mô hình.

---

## 3.8. Tổng kết chương

Chương 3 đã hoàn thành toàn bộ các bước nền móng cốt lõi về mặt kỹ thuật dữ liệu: tiếp nhận trọn vẹn 7.08 triệu bản ghi của năm 2024, chứng minh hiệu quả giảm hơn 82% dung lượng của định dạng Parquet Snappy phân vùng theo tháng, giải quyết bài toán phân loại đa lớp 5 nhóm nguyên nhân thực tế và xây dựng bộ đặc trưng chuẩn mực nói không với rò rỉ dữ liệu. Toàn bộ các tập dữ liệu đã sẵn sàng cho giai đoạn xây dựng và huấn luyện mô hình phân tán tại Chương 4.
