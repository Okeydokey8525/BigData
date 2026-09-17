# AI READ FIRST

> **Tài liệu điều hướng tối cao dành cho AI Agent tiếp quản dự án.**  
> Bắt buộc đọc file này trước tiên để nắm toàn bộ chỉ mục tri thức và quy định làm việc.

---

## Project
* **Tên đề tài:** Ứng dụng thuật toán Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay thương mại: Trường hợp tại Hoa Kỳ năm 2024.
* **Học phần:** Nhập môn Big Data (HK VII) – Trường Đại học Công Thương TP. Hồ Chí Minh (HUIT).
* **Giảng viên hướng dẫn:** TS. Phan Hồ Viết Trường.
* **Nhóm sinh viên thực hiện:** Nhóm 6 (Cù Văn Vĩ An, Lê Đức Lương, Trần Huỳnh Tuấn Anh).

## Purpose
* Xây dựng hệ thống Big Data toàn diện có khả năng lưu trữ, xử lý phân tán và học máy phân tán trên tập dữ liệu chuyến bay quy mô lớn (7.07 triệu dòng, 1.22 GB).
* **Yêu cầu then chốt từ Giảng viên:** Triển khai đối chứng **2 HƯỚNG SONG SONG**:
  1. **Hướng Thuần (Single-node Baseline):** Sử dụng CPU/GPU đơn máy (Pandas, Scikit-learn Random Forest, RAPIDS/XGBoost) để chỉ ra giới hạn nghẽn tài nguyên của phương pháp truyền thống.
  2. **Hướng Spark (Distributed Big Data):** Sử dụng Apache Spark (PySpark, Parquet, Spark MLlib) để chứng minh năng lực xử lý phân tán vượt trội.

## Current Status
* **Hiện trạng thực tế:** **Đã hoàn thành đề cương & Đã tải bộ dữ liệu**.
* Chưa tạo mã nguồn thực thi hay pipeline nào khác.
* File dữ liệu gốc [`flight_data_2024.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/flight_data_2024.csv) đã sẵn sàng (7,079,081 dòng, 35 cột).
* File từ điển dữ liệu [`flight_data_2024_data_dictionary.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/flight_data_2024_data_dictionary.csv) đã được bổ sung giải thích nghĩa tiếng Việt.

## Architecture
* **Lưu trữ:** HDFS / Parquet nén Snappy phân vùng theo `month`.
* **Xử lý & ML:** Apache Spark (PySpark, Spark SQL, Spark MLlib `RandomForestClassifier`).
* **Kho dữ liệu & Serving:** Apache Hive $\to$ MongoDB $\to$ FastAPI REST API.
* **Trực quan hóa:** React Dashboard + Plotly.js / Chart.js.

## Tech Stack
* **Phần cứng Local xác nhận:** AMD Ryzen 7 250 (8C/16T), RAM 16GB (khả dụng ~4.2GB), GPU NVIDIA RTX 5050 Laptop, SSD C: trống 71.9GB.
* **Môi trường Cloud đối chứng:** Kaggle Notebook (4 vCPU, 30GB RAM, 2x NVIDIA Tesla T4 32GB VRAM).
* **Ngôn ngữ & Runtime:** Python 3.13, OpenJDK 17.0.19 (đã có trên máy).
* **Framework:** PySpark 3.5.x, Scikit-learn, FastAPI, MongoDB, React.

## Critical Files
1. [`doc/nguyen-tac-lam-viec-dai.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/nguyen-tac-lam-viec-dai.md): Bản quy tắc làm việc dài và tiêu chuẩn tri thức AI.
2. [`doc/05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md): Nhật ký bàn giao tiến độ qua từng phiên làm việc.
3. [`doc/08_HARDWARE_AND_ENVIRONMENT_SPECS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/08_HARDWARE_AND_ENVIRONMENT_SPECS.md): Thông số phần cứng đo đạc thực tế của máy bạn và Kaggle.
4. [`doc/09_DUAL_APPROACH_PLAN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/09_DUAL_APPROACH_PLAN.md): Phương pháp luận chi tiết phân định 2 hướng Thuần vs Spark.
5. [`Nhom6_T4_C10-12_BaoCao.docx`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_T4_C10-12_BaoCao.docx): File đề cương Word chính thức nộp cho GVHD.

## Important Rules
1. **Tuyệt đối không dùng `pd.read_csv` nạp trọn vẹn 1.2 GB dữ liệu vào RAM máy Local** $\to$ Sẽ gây sập IDE/OOM (vì RAM trống máy local chỉ còn ~4.2 GB).
2. **Luôn kiểm thử logic trên file mẫu [`flight_data_2024_sample.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/flight_data_2024_sample.csv)** trước khi chạy tập dữ liệu lớn.
3. **Phòng chống Data Leakage:** Không đưa các cột phát sinh sau khi cất cánh/hạ cánh (`dep_time`, `dep_delay`, `arr_time`, `actual_elapsed_time`,...) vào làm feature đầu vào.
4. **Quy định Check-in & Check-out:** Phải cập nhật [`doc/05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md) sau mỗi phiên làm việc.

## Current Work
* Thiết lập toàn bộ tài liệu kiến trúc, xác định phương pháp luận 2 hướng và xây dựng kế hoạch hành động chi tiết.

## Known Issues
* Thư mục dự án hiện tại mới chỉ có dataset thô, chưa có môi trường ảo (`venv`), chưa có mã nguồn trong `src/`.
* RAM khả dụng của máy cục bộ thấp (~4.2 GB), cần chiến lược chạy Hybrid (Dev local trên sample $\to$ Train full trên Kaggle).

## Documentation Map
* [`00_AI_READ_FIRST.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/00_AI_READ_FIRST.md): File này - Điểm xuất phát của mọi AI.
* [`README.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/README.md): Tổng quan dự án và bảng trạng thái thư mục.
* [`01_DATASET_SPECIFICATION.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/01_DATASET_SPECIFICATION.md): Đặc tả 35 cột & quy tắc Data Leakage.
* [`02_ARCHITECTURE_AND_TECHSTACK.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/02_ARCHITECTURE_AND_TECHSTACK.md): Kiến trúc phân tán HDFS/Spark/Mongo/FastAPI/React.
* [`03_ROADMAP_AND_TASKS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/03_ROADMAP_AND_TASKS.md): Kế hoạch 11 tuần bám sát đề cương.
* [`04_AI_AGENT_GUIDELINES.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/04_AI_AGENT_GUIDELINES.md): Quy chuẩn lập trình và an toàn bộ nhớ.
* [`05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md): Nhật ký công việc và cơ chế bàn giao.
* [`06_ENVIRONMENT_SETUP.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/06_ENVIRONMENT_SETUP.md): Hướng dẫn cài đặt Java/Hadoop winutils/Docker.
* [`07_EXPERIMENT_AND_EVALUATION_METRICS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/07_EXPERIMENT_AND_EVALUATION_METRICS.md): Tiêu chuẩn thực nghiệm và bảng số liệu mẫu cho Chương 5.
* [`08_HARDWARE_AND_ENVIRONMENT_SPECS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/08_HARDWARE_AND_ENVIRONMENT_SPECS.md): Đo đạc phần cứng máy Local và so sánh Kaggle.
* [`09_DUAL_APPROACH_PLAN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/09_DUAL_APPROACH_PLAN.md): Kế hoạch triển khai chi tiết 2 hướng Thuần vs Spark.
* [`nguyen-tac-lam-viec-dai.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/nguyen-tac-lam-viec-dai.md): Quy tắc làm việc dài và tiêu chuẩn tài liệu AI.

## Verification Status
* Cấu hình phần cứng: **Đã xác minh (Verified via PowerShell CIM)**.
* Bộ dữ liệu: **Đã xác minh (Verified 7,079,081 dòng)**.
* Môi trường Java: **Đã xác minh (OpenJDK 17.0.19)**.

## Last Updated
* `2026-09-16 17:35:00 (GMT+7)` bởi Antigravity AI.
