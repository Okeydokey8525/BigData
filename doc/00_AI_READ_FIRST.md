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
* **Hiện trạng thực tế:** **Đã hoàn thành toàn bộ Pipeline Big Data & Web App 7M**:
  1. **ETL & Storage:** Đã chuyển đổi thành công 7,079,081 dòng thô thành 6,965,267 dòng sạch định dạng Apache Parquet nén Snappy phân vùng 12 tháng (kích thước 228.4 MB, nén 81.7%, tốc độ 65.42s).
  2. **Huấn luyện Mô hình 100% 7 Triệu Dòng (Phương án B 70/15/15):**
     - Đã huấn luyện thành công 6 mô hình Baseline tận dụng GPU NVIDIA RTX 5050 (XGBoost 19.27s, CatBoost F1 70.83%) và CPU Ryzen 7.
     - Đã huấn luyện thành công mô hình Apache Spark MLlib Random Forest phân tán (49.89s, RAM 1,012 MB).
  3. **Bộ số liệu & Ấn phẩm:** Đã xuất bản bảng đối sánh toàn diện và trọn bộ 24 biểu đồ khoa học chất lượng cao trong `results/baseline/`.
  4. **Web Application:** Đã xây dựng hoàn chỉnh ứng dụng Web Python độc lập tại [`Web_Air/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Web_Air) với giao diện 3 Tab, hỗ trợ suy luận thời gian thực và kịch bản 1-click `run.bat`.

## Architecture
* **Lưu trữ:** HDFS / Parquet nén Snappy phân vùng theo `month`.
* **Xử lý & ML:** Apache Spark (PySpark, Spark SQL, Spark MLlib `RandomForestClassifier`) song song với Hướng Thuần CPU/GPU.
* **Kho dữ liệu & Serving:** MongoDB / File Parquet $\to$ FastAPI REST API (`Web_Air`).
* **Trực quan hóa:** Web_Air Dashboard (FastAPI + Jinja2 + HTML5/CSS Glassmorphic) với 3 Tab chuyên biệt.

## Tech Stack
* **Phần cứng Local xác nhận:** AMD Ryzen 7 250 (8C/16T), RAM 16GB, GPU NVIDIA GeForce RTX 5050 Laptop GPU (CUDA), SSD C: trống >60GB.
* **Môi trường Cloud đối chứng:** Kaggle Notebook (4 vCPU, 30GB RAM, 2x NVIDIA Tesla T4 32GB VRAM).
* **Ngôn ngữ & Runtime:** Python 3.13, OpenJDK 17.0.19 (đã có trên máy).
* **Framework:** PySpark 3.5.x, Scikit-learn, XGBoost, CatBoost, LightGBM, FastAPI, Uvicorn.

## Critical Files
1. [`Web_Air/run.bat`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Web_Air/run.bat): Kịch bản 1-click khởi chạy Web App trực quan.
2. [`doc/13_BAO_CAO_HUAN_LUYEN_FULL_7M_BASELINE_CPU_GPU.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/13_BAO_CAO_HUAN_LUYEN_FULL_7M_BASELINE_CPU_GPU.md): Báo cáo thực nghiệm 7 triệu dòng CPU & GPU RTX 5050.
3. [`doc/14_HUONG_DAN_TRIEN_KHAI_SPARK_CLUSTER_3_LAPTOP_TAILSCALE.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/14_HUONG_DAN_TRIEN_KHAI_SPARK_CLUSTER_3_LAPTOP_TAILSCALE.md): Hướng dẫn kết nối 3 laptop tại nhà thành cụm Spark phân tán qua Tailscale VPN.
4. [`doc/05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md): Nhật ký bàn giao tiến độ qua từng phiên làm việc.
5. [`doc/09_DUAL_APPROACH_PLAN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/09_DUAL_APPROACH_PLAN.md): Phương pháp luận chi tiết phân định 2 hướng Thuần vs Spark.

## Important Rules
1. **Tiết kiệm RAM cục bộ:** Luôn sử dụng ma trận `float32` hoặc xử lý phân khúc (Streaming Chunking) khi đọc dữ liệu lớn, tránh nạp thô CSV bằng `pd.read_csv`.
2. **Phòng chống Data Leakage:** Không đưa các cột phát sinh sau khi cất cánh/hạ cánh (`dep_time`, `dep_delay`, `arr_time`, `actual_elapsed_time`,...) vào làm feature đầu vào.
3. **Quy định Check-in & Check-out:** Phải cập nhật [`doc/05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md) sau mỗi phiên làm việc.

## Current Work
* Kiểm tra trải nghiệm thực tế Web App `Web_Air`, hoàn thiện báo cáo 7 chương cho GVHD và đồng bộ Git repository.

## Known Issues
* Mô hình XGBoost khi chạy suy luận đơn mẫu trên CPU sẽ hiển thị cảnh báo DMatrix do được huấn luyện trên GPU CUDA (đã xử lý mượt mà, không ảnh hưởng kết quả).

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
