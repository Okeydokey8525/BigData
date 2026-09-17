# KẾ HOẠCH THỰC HIỆN VÀ THEO DÕI TIẾN ĐỘ (ROADMAP & TASKS)

> Theo lộ trình 11 tuần đã thống nhất trong Đề cương đồ án môn học.

---

## 1. TIẾN ĐỘ 11 TUẦN (11-WEEK ROADMAP)

| Tuần | Nội dung công việc theo đề cương | Trạng thái | Đầu ra dự kiến / Ghi chú |
| :---: | :--- | :---: | :--- |
| **Tuần 1** | Khảo sát bài toán trễ chuyến bay hàng không, nghiên cứu lý thuyết hệ sinh thái Hadoop, Spark và quy định đồ án. | **HOÀN THÀNH** | Đề cương chi tiết [`Nhom6_T4_C10-12_BaoCao.docx`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_T4_C10-12_BaoCao.docx) |
| **Tuần 2** | Thu thập và thẩm định bộ dữ liệu `Flight Delay Dataset 2024`; Thiết lập môi trường dự án. | **HOÀN THÀNH** | Đã tải tập dữ liệu 1.3 GB vào [`Flight Delay Dataset — 2024/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024) |
| **Tuần 3** | Cấu hình Apache Hadoop HDFS; Viết kịch bản tự động nạp dữ liệu lớn vào hệ thống tệp phân tán. | **ĐANG THỰC HIỆN** | Docker Compose cho cụm Hadoop/Spark; Script nạp CSV vào HDFS |
| **Tuần 4** | Phát triển pipeline tiền xử lý dữ liệu bằng Apache Spark (PySpark): lọc dữ liệu nhiễu, xử lý khuyết thiếu, chuẩn hóa kiểu và chuyển đổi sang Parquet. | **CHỜ THỰC HIỆN** | Script `preprocess.py` hoặc notebook xử lý phân tán; output Parquet |
| **Tuần 5** | Triển khai kho dữ liệu Apache Hive trên nền HDFS; Thực hiện các truy vấn phân tích tổng hợp (HiveQL). | **CHỜ THỰC HIỆN** | DDL Script tạo Hive External Table & các truy vấn OLAP tổng hợp |
| **Tuần 6** | Kỹ thuật trích xuất đặc trưng phân tán trên PySpark phục vụ bài toán Machine Learning (StringIndexer, VectorAssembler, Binning). | **CHỜ THỰC HIỆN** | Module `feature_engineering.py` và pipeline Spark ML |
| **Tuần 7** | Huấn luyện và đánh giá các mô hình Machine Learning phân tán (Spark ML): Random Forest (chính), Decision Tree, Logistic Regression. | **CHỜ THỰC HIỆN** | Module `train.py`, so sánh độ chính xác và lưu trữ mô hình (Model persistence) |
| **Tuần 8** | Đánh giá hiệu năng mô hình (Accuracy, F1, Confusion Matrix, Training Time); Trích xuất kết quả dự báo và nạp vào MongoDB. | **CHỜ THỰC HIỆN** | Báo cáo số liệu thực nghiệm; Script đẩy kết quả vào MongoDB |
| **Tuần 9** | Xây dựng RESTful API Backend bằng FastAPI để phục vụ truy vấn dữ liệu phân tích và endpoint dự đoán từ mô hình. | **CHỜ THỰC HIỆN** | Mã nguồn Backend FastAPI trong `backend/` |
| **Tuần 10** | Thiết kế giao diện Dashboard (React + Plotly/Chart.js) trực quan hóa biểu đồ phân tích và kiểm thử tích hợp toàn bộ hệ thống. | **CHỜ THỰC HIỆN** | Ứng dụng Dashboard tương tác trong `dashboard/` |
| **Tuần 11** | Hoàn thiện tài liệu báo cáo học phần (7 chương), đóng gói mã nguồn, thiết kế slide trình chiếu và chuẩn bị kịch bản demo. | **CHỜ THỰC HIỆN** | File báo cáo hoàn chỉnh trong [`Bao_Cao/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao) và slide thuyết trình |

---

## 2. NHIỆM VỤ ƯU TIÊN TIẾP THEO (NEXT IMMEDIATE STEPS CHO AI AGENTS)

Khi một AI Agent tiếp nhận công việc tiếp theo từ người dùng, hãy ưu tiên các nhiệm vụ sau theo thứ tự:

1. **Thiết lập môi trường tính toán phân tán (Docker):**
   * Xây dựng file `docker-compose.yml` định nghĩa cụm container:
     * Hadoop NameNode, DataNode
     * Spark Master, Spark Worker
     * (Tùy chọn) MongoDB container cho tầng serving
2. **Kịch bản Tiền xử lý dữ liệu với PySpark (`preprocessing.py`):**
   * Đọc tệp mẫu `flight_data_2024_sample.csv` để thẩm định logic tiền xử lý.
   * Xử lý missing values:
     * Điền `0` cho các cột delay khi không ghi nhận.
     * Xử lý các chuyến bay `cancelled == 1` và `diverted == 1`.
   * Gán nhãn biến mục tiêu `target_cause`: Phân loại 5 nguyên nhân chính (`Carrier`, `Weather`, `NAS`, `Security`, `LateAircraft`).
   * Chuyển đổi toàn bộ tập dữ liệu 7.07M dòng sang định dạng **Parquet** (Partition theo `month` hoặc `op_unique_carrier`).
3. **Thực hiện phân tích khám phá dữ liệu (EDA) trên Spark:**
   * Thống kê các chỉ số mô tả, top sân bay trễ, top hãng hàng không trễ, biểu đồ phân bố độ trễ theo thời gian.
