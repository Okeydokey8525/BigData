# BÁO CÁO GIẢI ĐÁP THẮC MẮC: BỘ NHỚ RAM, PHẦN CỨNG VÀ KỸ THUẬT TIỀN XỬ LÝ 7 TRIỆU DÒNG

> **Môn học:** Nhập môn Dữ liệu lớn (Big Data)  
> **Nhóm thực hiện:** Nhóm 6 (Lê Đức Lương, Cù Văn Vĩ An, Trần Huỳnh Tuấn Anh)  
> **Tài liệu tham chiếu:** [`results/baseline/metrics/etl_all_at_once_7m.json`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/baseline/metrics/etl_all_at_once_7m.json), [`src/etl/to_parquet.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/to_parquet.py), [`doc/15_BAO_CAO_TIEN_XU_LY_ALL_AT_ONCE_7M.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/15_BAO_CAO_TIEN_XU_LY_ALL_AT_ONCE_7M.md)  
> **Mục đích:** Tổng hợp, làm rõ bản chất kỹ thuật và các thắc mắc cốt lõi xoay quanh quá trình tiền xử lý nạp 1 lần (All-at-Once), cơ chế tiêu thụ RAM, sự phụ thuộc phần cứng và lý do ra đời của cụm phân tán Spark 3 máy.

---

## MỤC LỤC
1. [Làm rõ khái niệm cốt lõi: Bit vs Byte trong Pandas](#1-lam-ro-khai-niem-cot-loi-bit-vs-byte-trong-pandas)
2. [Sơ đồ dòng chảy dữ liệu & bộ nhớ (Memory & Data Pipeline)](#2-so-do-dong-chay-du-lieu--bo-nho-memory--data-pipeline)
3. [So sánh trực diện: Code thông thường vs Code tối ưu của Nhóm](#3-so-sanh-truc-dien-code-thong-thuong-vs-code-toi-uu-cua-nhom)
4. [Mức độ phụ thuộc vào phần cứng máy (Lenovo LOQ 15AHP10)](#4-muc-do-phu-thuoc-vao-phan-cung-may-lenovo-loq-15ahp10)
5. [Khả năng tương thích khi đưa code cho máy khác chạy](#5-kha-nang-tuong-thich-khi-dua-code-cho-may-khac-chay)
6. [Bài toán giới hạn bộ nhớ: Dữ liệu bao nhiêu triệu dòng thì đầy RAM?](#6-bai-toan-gioi-han-bo-nho-du-lieu-bao-nhieu-trieu-dong-thi-day-ram)
7. [Mối liên hệ học thuật: Vì sao bắt buộc phải chuyển sang cụm Spark 3 máy?](#7-moi-lien-he-hoc-thuat-vi-sao-bat-buoc-phai-chuyen-sang-cum-spark-3-may)

---

## 1. LÀM RÕ KHÁI NIỆM CỐT LÕI: BIT VS BYTE TRONG PANDAS

Một hiểu lầm rất phổ biến là nhầm lẫn giữa **Bit** và **Byte**:
- **1 Byte = 8 bits**.
- **Mặc định của Pandas:** Khi đọc CSV mà không định nghĩa kiểu, Pandas tự gán kiểu **64-bit**, nghĩa là **$64 / 8 = 8\text{ Bytes}$** cho mỗi ô số (`int64`, `float64`).
- **Kỹ thuật Ép kiểu (Downcasting) của nhóm:** Thay vì để 8 Bytes lãng phí:
  - Cột `Month` (giá trị 1 đến 12), `DayOfWeek` (1 đến 7) chỉ cần kiểu **`int8` = 8 bits = 1 Byte** (lưu được từ -128 đến 127).
  - Cột `CRSDepTime` (0 đến 2359), `Distance` (khoảng cách bay 0 đến 5000 dặm) chỉ cần kiểu **`int16` = 16 bits = 2 Bytes** (lưu được từ -32,768 đến 32,767).
  - Cột cờ trễ chuyến `IsDelayed` (chỉ có 0 hoặc 1) chỉ cần kiểu **`int8` = 1 Byte**.

👉 **Kết quả:** Giảm kích thước mỗi ô số từ 8 Bytes xuống còn 1 đến 2 Bytes, **tiết kiệm từ 75% đến 87.5% dung lượng lưu trữ trên từng trường số** ngay khi dữ liệu vừa chạm vào RAM!

---

## 2. SƠ ĐỒ DÒNG CHẢY DỮ LIỆU & BỘ NHỚ (MEMORY & DATA PIPELINE)

```mermaid
flowchart TD
    subgraph S1 [BƯỚC 1: ĐỌC DỮ LIỆU TỪ Ổ CỨNG]
        A["12 File Raw CSV (12 tháng 2024: ~2.3 GB trên SSD)"] 
        -->|pd.read_csv với DTYPE_SPEC chỉ định sẵn| B["Nạp vào RAM: 7,079,081 dòng thô"]
    end

    subgraph S2 [BƯỚC 2: TỐI ƯU DUNG LƯỢNG NGAY KHI VÀO RAM]
        B --> C{"Cơ chế Ép kiểu (Downcasting)"}
        C -->|Tháng, Ngày tuần, Trễ chuyến| D["int8: Tốn đúng 1 Byte (thay vì 8 Bytes mặc định)"]
        C -->|Giờ bay, Cự ly khoảng cách| E["int16 / float32: Tốn 2-4 Bytes (thay vì 8 Bytes)"]
        C -->|Mã hãng bay, Mã sân bay| F["string / category: Tối ưu hoá con trỏ chuỗi"]
        D & E & F --> G["DataFrame gọn nhẹ: ~2.1 GB RAM (Giảm hơn 50% so với mặc định)"]
    end

    subgraph S3 [BƯỚC 3: TIỀN XỬ LÝ SIÊU TỐC TRONG RAM]
        G --> H["Bộ lọc: Loại bỏ chuyến hủy/đổi hướng (Cancelled = 1, Diverted = 1)"]
        H --> I["Còn lại: 6,965,267 dòng hợp lệ"]
        I --> J["Tạo đặc trưng (Feature Engineering): Giờ bay, Tốc độ, Nhãn IsDelayed..."]
        J -->|Chạy bằng NumPy C-Vectorized (AVX-512)| K["Xử lý đồng loạt toàn bộ 7 triệu dòng chỉ trong 7.7 giây!"]
        K -.->|RAM đỉnh sinh ra biến phụ tạm thời| P["Mức RAM Đỉnh (Peak RAM): 4.79 GB"]
    end

    subgraph S4 [BƯỚC 4: NÉN VÀ GHI RA Ổ CỨNG]
        K --> L["DataFrame hoàn chỉnh (Clean Data)"]
        L -->|PyArrow Engine nén Snappy theo cột| M["cleaned_flight_data_2024.parquet"]
        M --> N["Dung lượng ổ cứng: Chỉ còn 228 MB (Chia 12 thư mục tháng)"]
    end

    style S1 fill:#e3f2fd,stroke:#1565c0
    style S2 fill:#fff3e0,stroke:#e65100
    style S3 fill:#e8f5e9,stroke:#2e7d32
    style S4 fill:#f3e5f5,stroke:#7b1fa2
```

---

## 3. SO SÁNH TRỰC DIỆN: CODE THÔNG THƯỜNG VS CODE TỐI ƯU CỦA NHÓM

| Tiêu chí | Code thông thường (Người khác viết) | Code của nhóm mình ([`src/etl/to_parquet.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/to_parquet.py)) | Lợi ích vượt trội |
| :--- | :--- | :--- | :--- |
| **Khai báo kiểu dữ liệu** | Không khai báo `dtype` $\to$ Pandas tự dò và gán `int64` (8 Bytes), `float64` (8 Bytes). | Khai báo từ điển `DTYPE_SPEC` trước: tháng gán `int8` (1 Byte), giờ gán `int16` (2 Bytes)... | **Tránh bùng nổ RAM:** Tiết kiệm hơn 50% RAM ngay từ giây đầu tiên nạp vào bộ nhớ. |
| **Phương pháp tính toán** | Dùng vòng lặp `for` hoặc hàm `df.apply(lambda ...)` (chạy từng dòng bằng thông dịch Python). | Dùng toán tử vector hóa của NumPy/Pandas (xử lý cả mảng triệu phần tử bằng C/C++ AVX-512). | **Thời gian:** Rút ngắn từ **15–30 phút** xuống còn **7.7 giây**! |
| **Mức RAM đỉnh (Peak)** | Vọt lên **7.0 GB – 8.5 GB** (do sinh thêm cột phụ 64-bit trong quá trình tính toán). | Đạt đỉnh tối đa **4.79 GB** (bao gồm dữ liệu bảng + các mảng phụ tạm thời trước khi Garbage Collector giải phóng). | Máy 16GB chạy mượt mà, không bị tràn sang ổ cứng ảo (`pagefile.sys`). |
| **Định dạng lưu trữ** | Lưu ra file `.csv` mới ($\approx$ 2 GB, đọc lại rất chậm). | Lưu ra định dạng cột `.parquet` nén Snappy phân vùng 12 tháng. | Dung lượng thu nhỏ về **228 MB** (nhẹ hơn gấp 10 lần), Spark đọc lại siêu nhanh. |

---

## 4. MỨC ĐỘ PHỤ THUỘC VÀO PHẦN CỨNG MÁY (LENOVO LOQ 15AHP10)

Thực nghiệm cho thấy quá trình chạy phụ thuộc chặt chẽ vào phần cứng, nhưng mối quan hệ này là: **Phần cứng mạnh là ĐIỀU KIỆN CẦN, Thuật toán tối ưu là ĐIỀU KIỆN ĐỦ**.

### 4.1. Phần cứng máy đã hỗ trợ những gì?
- **Ổ cứng Micron SSD NVMe PCIe 4.0 (Đọc ~4.500 MB/s):** Đọc 2.3 GB CSV thô vào RAM chỉ mất **32.22 giây**. Nếu dùng HDD cơ cũ (~100 MB/s), khâu đọc đĩa sẽ ngốn mất 3 – 5 phút.
- **RAM 16 GB DDR5 5600 MHz (Băng thông ~50.000 MB/s):** Băng thông cực lớn giúp nạp xả mảng dữ liệu tức thì. Quan trọng nhất, 16GB RAM cung cấp đủ ~10GB RAM trống cho Python, giúp mức đỉnh **4.79 GB** hoàn toàn nằm trong bộ nhớ vật lý.
- **CPU AMD Ryzen 7 250 (8 nhân 16 luồng, kiến trúc Zen 4, có tập lệnh AVX-512):** Thực thi các phép toán ma trận của NumPy C-Extension với tốc độ hàng tỷ phép tính/giây.
- **Tại sao máy không nóng?**
  - Công thức nhiệt lượng: $Q = P \times t$.
  - Giai đoạn CPU gánh tải nặng nhất chỉ kéo dài vỏn vẹn **7.7 giây**! Hệ thống tản nhiệt buồng hơi (Vapor Chamber) và cụm heatsink của máy hấp thụ nhiệt ngay lập tức trước khi quạt kịp rú lên.

### 4.2. Tại sao nếu phần cứng mạnh mà code dở thì máy vẫn sập?
- Nếu cũng trên chiếc máy Lenovo LOQ này mà viết `df.apply` lặp 7 triệu dòng: Python sẽ bị vướng **GIL (Global Interpreter Lock)**, chỉ chạy trên 1 luồng duy nhất $\to$ Thời gian xử lý kéo dài **20 – 30 phút** $\to$ 1 nhân CPU bị ép chạy 100% liên tục 30 phút sẽ làm máy nóng ran và quạt rú hết công suất!
- Nếu không ép kiểu `DTYPE_SPEC`: RAM vọt lên **8.5 GB**, cộng với Windows 5.5 GB $\to$ Tổng RAM vượt 14 GB $\to$ Máy bắt đầu giật lag, đơ chuột và đứng hình.

---

## 5. KHẢ NĂNG TƯƠNG THÍCH KHI ĐƯA CODE CHO MÁY KHÁC CHẠY

Nếu đưa code tiền xử lý cho các thành viên khác trong nhóm (Cù Văn Vĩ An, Trần Huỳnh Tuấn Anh) chạy thử:

### 5.1. Thời gian dự kiến trên các loại máy
1. **Máy bạn có 16GB RAM + SSD (Laptop lập trình tiêu chuẩn hiện nay):**
   - Mất khoảng **1 phút – 1 phút 30 giây** (Đọc file: ~50-60s, Xử lý: ~10-15s, Ghi: ~8s).
   - Chạy hoàn toàn bình thường, không lo tràn RAM.
2. **Máy bạn chỉ có 8GB RAM + SSD (Laptop phổ thông):**
   - Nếu chạy nạp 1 lần 7 triệu dòng: RAM trống chỉ có ~3.5 GB, thiếu 1.3 GB so với mức đỉnh 4.79 GB $\to$ Windows sẽ kích hoạt cơ chế Swapping đẩy dữ liệu sang ổ ảo `pagefile.sys`.
   - Thời gian sẽ bị kéo dài lên **5 – 10 phút**, ổ đĩa chạm 100%, máy giật lag hoặc báo lỗi `MemoryError`.

### 5.2. Có cần phải viết lại code để thích ứng không?
👉 **HOÀN TOÀN KHÔNG CẦN SỬA CODE!** Nhóm đã thiết kế sẵn kiến trúc đa chế độ trong [`src/etl/to_parquet.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/to_parquet.py):

- **Trên máy 16GB RAM (như máy Lương):**
  ```powershell
  venv\Scripts\python.exe src/etl/to_parquet.py --mode all-at-once
  ```
  *(Nạp 1 lần 7 triệu dòng đúng theo yêu cầu của thầy, ngốn 4.79 GB RAM, 45 giây).*

- **Trên máy 8GB RAM (như máy An hoặc Tuấn Anh):**
  ```powershell
  venv\Scripts\python.exe src/etl/to_parquet.py --mode stream --chunksize 1000000
  ```
  *(Tự động chia nhỏ thành từng khúc 1 triệu dòng, ngốn chỉ **~700 MB – 800 MB RAM**, máy 8GB chạy êm ái trong 1.5 – 2 phút, cho ra file Parquet chuẩn xác 100% y hệt).*

---

## 6. BÀI TOÁN GIỚI HẠN BỘ NHỚ: DỮ LIỆU BAO NHIÊU TRIỆU DÒNG THÌ ĐẦY RAM?

Dựa trên số liệu đo đạc thực nghiệm từ hệ thống:
$$\text{Suất tiêu hao RAM đỉnh} = \frac{4.79\text{ GB}}{7.08\text{ triệu dòng}} \approx 0.68\text{ GB / 1 triệu dòng}$$

Bảng quy đổi giới hạn vật lý bộ nhớ RAM cho các cấu hình máy tính:

| Cấu hình máy | RAM hệ điều hành & App ngầm | RAM trống tối đa cho Python | **Lượng dữ liệu tối đa nạp 1 lần vừa đủ RAM** | Dung lượng file CSV thô tương ứng |
| :--- | :--- | :--- | :--- | :--- |
| **Máy 8 GB RAM** | ~4.0 GB – 4.5 GB | **Chỉ còn ~3.5 GB** | **Tối đa ~5.0 triệu dòng** *(Nếu ép chạy 7 triệu dòng $\to$ Thiếu 1.3 GB $\to$ Tràn RAM)* | ~1.6 GB CSV |
| **Máy 16 GB RAM** *(Lenovo LOQ)* | ~5.5 GB – 6.0 GB | **Còn khoảng ~10.0 GB** | **Vừa đủ: ~14.0 – 15.0 triệu dòng** *(Gấp đôi tập dữ liệu 2024 hiện tại)* | **~4.5 – 5.0 GB CSV** |
| **Máy 32 GB RAM** *(PC khủng / Workstation)* | ~6.0 GB | **Còn khoảng ~26.0 GB** | **Vừa đủ: ~35.0 – 38.0 triệu dòng** *(Dữ liệu hàng không của 5 năm liên tiếp)* | ~12.0 GB CSV |

> 📌 **Kết luận:**
> Trên máy 16GB của bạn, tập dữ liệu 7 triệu dòng mới chỉ sử dụng hết **~48%** dung lượng RAM trống dành cho Python. Ngưỡng giới hạn tối đa mà máy bạn có thể nạp 1 lần mà không bị tràn RAM là **khoảng 14 đến 15 triệu dòng** (tương đương file CSV nặng khoảng 4.5 – 5.0 GB).

---

## 7. MỐI LIÊN HỆ HỌC THUẬT: VÌ SAO BẮT BUỘC PHẢI CHUYỂN SANG CỤM SPARK 3 MÁY?

*(Gợi ý luận điểm trả lời xuất sắc nếu Giảng viên / Hội đồng phản biện đặt câu hỏi: "Tại sao nhóm đã tối ưu Pandas chạy trong 45 giây rồi mà vẫn phải triển khai Apache Spark?")*

Nhóm tự tin giải trình dựa trên 3 luận điểm cốt lõi của ngành Khoa học Dữ liệu lớn:

1. **Rào cản vật lý của máy đơn (Single-machine Memory Wall):**
   - Dù có áp dụng kỹ thuật ép kiểu tối ưu đến mức nào, một chiếc laptop 16GB cũng chạm trần ở mốc **15 triệu dòng**.
   - Nếu bài toán mở rộng ra dữ liệu bay của 3 năm (21 triệu dòng) hay 10 năm (70 triệu dòng), **bất kỳ máy tính cá nhân đơn lẻ nào cũng sẽ bất lực vì cạn kiệt bộ nhớ vật lý.**
2. **Khả năng mở rộng theo chiều ngang (Horizontal Scalability) của Cụm Spark 3 máy:**
   - Thay vì tốn tiền mua máy trạm 64GB hay 128GB RAM đắt đỏ (Scale-Up), nhóm tận dụng 3 chiếc laptop sẵn có của 3 sinh viên, kết nối mạng qua **Tailscale Mesh VPN** để gộp tài nguyên.
   - Cụm 3 máy sở hữu tổng cộng **36 – 40 GB RAM cụm** và **24 nhân / 48 luồng CPU**, cho phép chia nhỏ bài toán (Partitioning) và tính toán song song trên RDD/DataFrame.
3. **Mục tiêu so sánh học thuật (Baseline vs Distributed Showdown):**
   - Đồ án này xây dựng một bài so sánh đối chuẩn khoa học: Chứng minh ranh giới rõ ràng giữa **khi nào nên dùng Single-machine Optimization** (dữ liệu vừa và nhỏ, chi phí mạng bằng 0) và **khi nào bắt buộc phải dùng Distributed Spark** (dữ liệu quy mô lớn vượt ngưỡng RAM máy đơn).

---
*Tài liệu được biên soạn phục vụ báo cáo bảo vệ Đồ án môn học Nhập môn Dữ liệu lớn - Nhóm 6.*
