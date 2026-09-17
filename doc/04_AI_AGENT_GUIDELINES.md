# HƯỚNG DẪN DÀNH CHO AI AGENT (AI AGENT OPERATIONAL GUIDELINES)

> **Dành cho mọi AI Agent (Gemini, Claude, GPT, v.v.) khi làm việc trong workspace này.**  
> Vui lòng tuân thủ nghiêm ngặt các nguyên tắc dưới đây để đảm bảo an toàn hệ thống, hiệu năng và tính nhất quán của đồ án.

---

## 1. NGUYÊN TẮC AN TOÀN TÀI NGUYÊN BỘ NHỚ (CRITICAL MEMORY & BIG DATA RULES)

> [!CAUTION]
> **CẢNH BÁO TRAN BỘ NHỚ (OUT OF MEMORY - OOM):**  
> Tệp `flight_data_2024.csv` có dung lượng hơn **1.22 GB** và chứa **7,079,081 dòng**. Máy tính người dùng đang chạy môi trường Windows cá nhân.

1. **TUYỆT ĐỐI KHÔNG:**
   * Không chạy lệnh `pandas.read_csv('...flight_data_2024.csv')` đọc toàn bộ file vào RAM trong một lần. Việc này sẽ tốn từ 4 GB đến 6 GB RAM uncompressed và có thể gây treo IDE hoặc sập tiến trình Python.
2. **LUÔN ÁP DỤNG:**
   * **Trong giai đoạn phát triển & thử nghiệm (Dev/Test):** Sử dụng tệp mẫu [`flight_data_2024_sample.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/flight_data_2024_sample.csv) (10,000 dòng, 1.85 MB) để kiểm tra logic, schema và hàm xử lý trước.
   * **Khi xử lý trên dữ liệu lớn 7 triệu dòng:** Bắt buộc sử dụng **Apache Spark (PySpark)** với cơ chế tính toán lười (**Lazy Evaluation**), phân vùng (`repartition` / `partitionBy`), hoặc nếu dùng Python script đơn giản thì phải stream theo từng chunk (`chunksize=100000`).

---

## 2. QUY CHUẨN TỔ CHỨC THƯ MỤC VÀ TỆP TIN

Khi tạo mới các thành phần mã nguồn hoặc tài liệu, hãy đặt vào đúng vị trí quy định:

```text
c:\LeDucLuong\HK VII\NhapMonBigData\DoAn\
├── Flight Delay Dataset — 2024/    # Dữ liệu gốc (CHỈ ĐỌC, KHÔNG GHI ĐÈ FILE CSV GỐC)
├── doc/                            # Tài liệu kỹ thuật và hướng dẫn hệ thống
│   ├── README.md                   # Tổng quan dự án & ngữ cảnh
│   ├── 01_DATASET_SPECIFICATION.md # Đặc tả chi tiết 35 cột & Target Labeling
│   ├── 02_ARCHITECTURE_AND_TECHSTACK.md # Kiến trúc phân tán HDFS/Spark/Mongo/FastAPI/React
│   ├── 03_ROADMAP_AND_TASKS.md     # Tiến độ 11 tuần & Task tracking
│   └── 04_AI_AGENT_GUIDELINES.md   # File hướng dẫn này
├── Bao_Cao/                        # Báo cáo học phần Word/PDF/Slide nghiệm thu
├── src/                            # Mã nguồn chính (Sẽ tạo khi bắt đầu viết code)
│   ├── etl/                        # Script tiền xử lý dữ liệu PySpark
│   ├── ml/                         # Huấn luyện Spark MLlib (Random Forest, Decision Tree, LR)
│   └── utils/                      # Các hàm tiện ích hỗ trợ
├── backend/                        # Mã nguồn FastAPI REST API
├── dashboard/                      # Mã nguồn Frontend React
└── docker/                         # Docker Compose cụm Hadoop, Spark, MongoDB
```

---

## 3. NGUYÊN TẮC CHỐNG RÒ RỈ DỮ LIỆU KHI LÀM MACHINE LEARNING

Mục tiêu của đề tài là: **Dự đoán nguyên nhân trễ chuyến bay thương mại**.

* **Khi xây dựng Pipeline Spark ML:**
  * Chỉ sử dụng các biến biết trước giờ bay (`month`, `day_of_week`, `op_unique_carrier`, `origin`, `dest`, `crs_dep_time`, `crs_arr_time`, `distance`, `crs_elapsed_time`).
  * **Loại bỏ hoàn toàn** các thuộc tính chỉ xuất hiện sau khi bay: `dep_time`, `dep_delay`, `taxi_out`, `wheels_off`, `wheels_on`, `taxi_in`, `arr_time`, `actual_elapsed_time`, `air_time`, và các cột `*_delay`.
  * Đặt seed cố định (`seed=42`) cho các phép biến đổi ngẫu nhiên và phân chia `train_test_split` (ví dụ tỉ lệ 80/20 hoặc 70/15/15) để đảm bảo kết quả thực nghiệm có thể tái lập được (**Reproducibility**).

---

## 4. QUY TRÌNH PHỐI HỢP VÀ BÀN GIAO BẮT BUỘC (MANDATORY HANDOVER PROTOCOL)

Mọi AI Agent khi nhận hoặc kết thúc một phiên làm việc **BẮT BUỘC** phải thực hiện các bước sau:

1. **BƯỚC 1: CHECK-IN (Đọc trước khi làm):**
   * Mở [`doc/05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md).
   * Đọc phần **"1. TRẠNG THÁI HIỆN TẠI"** và **"1.3. VIỆC TIẾP THEO CẦN LÀM NGAY"** để bắt đúng mạch công việc dở dang, không lặp lại việc đã làm.

2. **BƯỚC 2: THỰC THI & KIỂM THỬ:**
   * Kiểm tra mã nguồn trên tệp mẫu `flight_data_2024_sample.csv` trước khi chạy trên tệp 1.2 GB.
   * Tuân thủ cấu trúc thư mục và quy tắc Data Leakage.

3. **BƯỚC 3: CHECK-OUT & GHI NHẬN TIẾN ĐỘ (Ghi bàn giao trước khi dừng lại):**
   * Ghi dòng mới vào bảng `Worklog` trong [`doc/05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md) (Thời gian, Tác nhân, Việc đã làm, File đã tạo/sửa).
   * Cập nhật lại mục **"1. TRẠNG THÁI HIỆN TẠI"** và chỉ định rõ **"VIỆC TIẾP THEO CẦN LÀM NGAY"** cho AI kế tiếp.
   * Nếu hoàn thành một hạng mục trong tuần, đánh dấu `[x]` hoặc đổi trạng thái trong [`doc/03_ROADMAP_AND_TASKS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/03_ROADMAP_AND_TASKS.md).

