# NHẬT KÝ THỰC HIỆN & BÀN GIAO CÔNG VIỆC (WORKLOG & HANDOVER PROTOCOL)

> **QUY ĐỊNH BẮT BUỘC CHO MỌI AI AGENT / THÀNH VIÊN DỰ ÁN:**  
> 1. **Trước khi bắt đầu:** Đọc mục **"1. TRẠNG THÁI HIỆN TẠI (CURRENT STATE)"** để biết việc gì đang làm dở dang và việc gì cần làm tiếp theo.  
> 2. **Sau khi hoàn thành:** Bắt buộc ghi nhận nội dung đã làm vào mục **"2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)"** và cập nhật lại mục **"1. TRẠNG THÁI HIỆN TẠI"** để bàn giao cho AI kế tiếp.

---

## 1. TRẠNG THÁI HIỆN TẠI (CURRENT STATE)

* **Cập nhật lần cuối:** `2026-09-30 17:48:00 (GMT+7)`
* **Giai đoạn hiện tại:** **Xây Dựng Hoàn Chỉnh Web_Air Dashboard 3 Tab Bằng Python Phục Vụ Chạy & Đối Sánh 6 Mô Hình Học Máy 7M**
* **Trạng thái công việc:** 
  1. **Khởi tạo và đóng gói Web Application độc lập tại `Web_Air/`:**
     - Sử dụng FastAPI + Uvicorn + Jinja2 + HTML5/CSS Glassmorphism hiện đại (không tốn thêm thư viện ngoài).
     - Thiết kế giao diện 3 Tab trực quan: Tab 1 (Dự đoán chuyến bay thời gian thực & Đối đầu 6 mô hình), Tab 2 (Bảng vàng đối sánh hiệu năng 7M & 4 giai đoạn pipeline), Tab 3 (Thư viện 24 biểu đồ khoa học có phóng to Lightbox).
  2. **Đồng bộ hóa 100% chuẩn 13 đặc trưng mới nhất:**
     - Tự động trích xuất `dep_min_of_day`, `arr_min_of_day`, giờ, phút, thứ, phân khúc `dep_time_of_day`, ánh xạ LabelEncoder và StandardScaler khớp với kịch bản `train_all_7m.py`.
     - Nạp thành công toàn bộ 6 mô hình vào RAM (~350 MB): CatBoost (SOTA), XGBoost (GPU), Random Forest (CPU), LightGBM, Decision Tree, Logistic Regression.
  3. **Kiểm thử chất lượng thực tế:**
     - Đạt 100% mã trạng thái HTTP 200 OK trên toàn bộ 9 endpoint (Home, Metadata, Predict, Predict-All, Metrics, Stages, Figures-list, Figures file, System-status).
     - Tạo file `Web_Air/run.bat` 1-click tự động chọn `venv\Scripts\python.exe` và mở trình duyệt tại `http://127.0.0.1:8000`.

### 1.1. Việc vừa hoàn thành (Just Completed)
* [x] Xây dựng toàn bộ mã nguồn `Web_Air/` (`app.py`, `config.py`, `core/model_loader.py`, `core/predictor.py`, `templates/index.html`, `static/css/style.css`, `static/js/main.js`, `run.bat`).
* [x] Kiểm thử độc lập và kiểm thử HTTP thực tế đạt 100% thành công.
* [x] Tạo kịch bản khởi chạy 1-click `Web_Air/run.bat`.

### 1.2. Việc đang tiến hành (In Progress)
* [ ] Kiểm tra giao diện thực tế và đồng bộ lên Git repository.

---

## 2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)

*Ghi lại theo thứ tự thời gian mới nhất ở trên cùng:*

| Thời gian | Tác nhân (AI/Dev) | Nội dung công việc đã hoàn thành | Tệp tin tạo mới / Chỉnh sửa | Ghi chú & Đánh giá |
| :--- | :--- | :--- | :--- | :--- |
| `2026-09-30 17:48` | Antigravity AI | Xây dựng hoàn chỉnh ứng dụng Web Python `Web_Air` phục vụ chạy suy luận và đối sánh 6 mô hình học máy 7M với 3 Tab chuyên biệt (Dự đoán chuyến bay, Bảng đối sánh 7M, Thư viện 24 biểu đồ); đồng bộ chuẩn 13 đặc trưng; tích hợp kịch bản 1-click `run.bat`; kiểm thử 100% đạt chuẩn | `Web_Air/app.py`<br>`Web_Air/config.py`<br>`Web_Air/core/*`<br>`Web_Air/templates/*`<br>`Web_Air/static/*`<br>`Web_Air/run.bat`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Hoàn thành xuất sắc 100% yêu cầu; RAM chiếm dụng chỉ 350 MB; độ trễ phản hồi cực nhanh |
| `2026-09-30 17:25` | Antigravity AI | Soạn thảo cẩm nang kỹ thuật hướng dẫn kết nối 3 laptop tại nhà riêng qua Tailscale Mesh VPN thành cụm Apache Spark phân tán; xác thực 100% 24 biểu đồ thực nghiệm 7M đã được tái tạo và lưu mới hoàn chỉnh | `doc/14_HUONG_DAN_TRIEN_KHAI_SPARK_CLUSTER_3_LAPTOP_TAILSCALE.md`<br>`doc/README.md`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Tài liệu đầy đủ lệnh PowerShell, cấu hình mạng SPARK_LOCAL_IP, mở port firewall và kịch bản demo |
| `2026-09-30 16:36` | Antigravity AI | Tiếp thu cải tiến từ `nhanh_khac` (thêm `dep_min_of_day`, `arr_min_of_day`), chạy ETL Streaming Chunking trên 7,079,081 dòng CSV (65.42s, Peak RAM 660MB); Huấn luyện thành công 100% 6.96M dòng theo Phương án B (Train 70% / Val 15% / Test 15%) tận dụng GPU NVIDIA RTX 5050 (XGBoost 19.27s, CatBoost 19.60s) và CPU Ryzen 7; Xuất bản báo cáo chi tiết doc/13 | `src/etl/clean_data.py`<br>`src/spark_ml/pipeline.py`<br>`src/baseline/train_all_7m.py`<br>`doc/13_BAO_CAO_HUAN_LUYEN_FULL_7M_BASELINE_CPU_GPU.md`<br>`doc/README.md`<br>`results/metrics/*`<br>`results/figures/*` | Hoàn thành xuất sắc 100% mục tiêu; GPU tăng tốc gấp 5.5 lần so với RF CPU; RAM tối đa 1.91 GB an toàn tuyệt đối |
| `2026-09-23 16:12` | Antigravity AI | Tiếp thu phản biện của người dùng về việc so sánh 300k vs 7M là không công bằng; tối ưu hóa ma trận đặc trưng float32 (292MB) và huấn luyện lại thành công 100% 6 mô hình CPU/SOTA trên 5.57M dòng Train; ghi đè toàn bộ kết quả CSV, JSON, models và 24 biểu đồ khoa học | `src/baseline/train_all_7m.py`<br>`results/metrics/*`<br>`results/figures/*`<br>`models/baseline/*`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Hoàn thành xuất sắc 100% mục tiêu; minh chứng thuyết phục Spark RF (49.89s) nhanh hơn gấp 2.8 lần so với RF CPU (139.71s) trên cùng 7 triệu dòng |
* [x] Nâng cấp [`src/utils/visualize_results.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/utils/visualize_results.py): Bổ sung đầy đủ các hàm vẽ Radar Chart, Bubble Plot, Grouped Bar, Horizontal Bar, Latency Bar, Peak RAM Bar.
* [x] Viết và thực thi [`src/baseline/train_all_7m.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/baseline/train_all_7m.py): Hoàn tất huấn luyện 6 mô hình CPU/SOTA, ghép Spark RF 7M, xuất đầy đủ CSV/JSON và 24 tệp ảnh đồ thị chất lượng cao.
* [x] Nâng cấp Web Serving [`src/serving/app.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/serving/app.py), [`src/serving/templates/index.html`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/serving/templates/index.html), [`src/serving/static/app.js`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/serving/static/app.js).
* [x] Kiểm thử toàn bộ API và hình ảnh qua HTTP đạt 100% mã trạng thái 200 OK.

### 1.2. Việc đang tiến hành (In Progress)
* [ ] Cập nhật walkthrough.md tổng kết thành quả và bàn giao cho người dùng.

### 1.3. VIỆC TIẾP THEO CẦN LÀM NGAY (NEXT IMMEDIATE TASKS CHO AI KẾ TIẾP)
Bất kỳ AI Agent nào tiếp nhận yêu cầu tiếp theo, hãy thực hiện theo thứ tự ưu tiên sau:

1. **Ưu tiên 1:** Đẩy toàn bộ thay đổi mã nguồn, kết quả thực nghiệm 7M, số liệu CSV và hệ thống đồ thị PNG lên GitHub repository (`git push origin main`).
2. **Ưu tiên 2:** Hoàn thiện báo cáo đồ án học phần cho TS. Phan Hồ Viết Trường.

---

## 2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)

*Ghi lại theo thứ tự thời gian mới nhất ở trên cùng:*

| Thời gian | Tác nhân (AI/Dev) | Nội dung công việc đã hoàn thành | Tệp tin tạo mới / Chỉnh sửa | Ghi chú & Đánh giá |
| :--- | :--- | :--- | :--- | :--- |
| `2026-09-19 07:22` | Antigravity AI | Xóa bỏ hoàn toàn tập mẫu 10k dòng theo yêu cầu; huấn luyện toàn diện 7 mô hình trên tập dữ liệu 7M; xuất hệ thống biểu đồ đa dạng (Radar 5 góc, Bubble Plot, Grouped Bar, Horizontal Bar, Donut, Stacked Bar, Heatmap); nâng cấp Web Serving FastAPI và Dashboard Glassmorphism phục vụ 100% 7 mô hình | `src/utils/visualize_results.py`<br>`src/baseline/train_all_7m.py`<br>`results/metrics/*`<br>`results/figures/*`<br>`src/serving/app.py`<br>`src/serving/templates/index.html`<br>`src/serving/static/app.js`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Hoàn thành xuất sắc 100% mục tiêu, đạt độ chính xác ~79.2%, Weighted F1 ~70%, RAM an toàn $< 1.75$ GB |
| `2026-09-18 22:02` | Antigravity AI | Thực thi ETL thành công 7.07M dòng (6.96M sạch, 207.88 MB Parquet), đo đạc thực nghiệm 4 giai đoạn & tổng toàn trình Thuần vs Spark, phân cấp cây thư mục kết quả `results/sample_10k/` & `results/full_7m/`, tạo bộ trực quan hóa đồ thị toàn diện (cột, tròn, riêng, ghép) và nâng cấp Web API FastAPI | `src/etl/clean_data.py`<br>`src/etl/to_parquet.py`<br>`src/utils/visualize_results.py`<br>`src/etl/benchmark_end_to_end_7m.py`<br>`results/sample_10k/*`<br>`results/full_7m/*`<br>`src/serving/app.py`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Hoàn thành xuất sắc 100% kế hoạch thực thi, RAM luôn $< 1.05$ GB an toàn |
| `2026-09-18 20:48` | Antigravity AI | Cập nhật tài liệu kỹ thuật chuẩn mực: Bảng Downcasting Memory Specification và Hướng dẫn quy trình triển khai 2 chế độ tiền xử lý 7.07M | `doc/01_DATASET_SPECIFICATION.md`<br>`doc/09_DUAL_APPROACH_PLAN.md`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Hoàn thành chuẩn hóa tài liệu theo yêu cầu |
| `2026-09-17 23:15` | Antigravity AI | Đo đạc RAM thực tế (trống 5.11 GB), phân tích khoa học All-at-once vs Chunking vs Spark Partitions, cập nhật tài liệu phương pháp luận đối chứng | `doc/09_DUAL_APPROACH_PLAN.md`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Hoàn thành phân tích kỹ thuật theo yêu cầu |
| `2026-09-17 17:18` | Antigravity AI | Nâng cấp Spark MLlib Pipeline hoàn chỉnh: Tự động lưu mô hình Spark, khắc phục winutils trên Windows, xuất bảng Grand Comparison 7 mô hình, tạo biểu đồ RF Showdown và cập nhật Web Dashboard | `src/spark_ml/pipeline.py`<br>`models/spark/*`<br>`results/metrics/grand_model_comparison.csv`<br>`results/figures/grand_rf_comparison.png`<br>`src/serving/app.py`<br>`src/serving/templates/index.html`<br>`.gitignore` | Hoàn thành toàn diện Giai đoạn 3 & Tích hợp Web 7 mô hình |
| `2026-09-17 17:18` | Antigravity AI | Nâng cấp Spark MLlib Pipeline hoàn chỉnh: Tự động lưu mô hình Spark, khắc phục winutils trên Windows, xuất bảng Grand Comparison 7 mô hình, tạo biểu đồ RF Showdown và cập nhật Web Dashboard | `src/spark_ml/pipeline.py`<br>`models/spark/*`<br>`results/metrics/grand_model_comparison.csv`<br>`results/figures/grand_rf_comparison.png`<br>`src/serving/app.py`<br>`src/serving/templates/index.html`<br>`.gitignore` | Hoàn thành toàn diện Giai đoạn 3 & Tích hợp Web 7 mô hình |
| `2026-09-17 17:10` | Antigravity AI | Xây dựng hoàn chỉnh Web Application bằng Python (FastAPI + Modern Glassmorphic Dashboard), kiểm thử 6/6 API đạt 100%, server đang chạy tại http://127.0.0.1:8000 | `src/serving/app.py`<br>`src/serving/templates/*`<br>`src/serving/static/*`<br>`src/etl/export_metadata.py`<br>`requirements.txt` | Hoàn thành Giai đoạn 5 Web App |
| `2026-09-17 16:45` | Antigravity AI | Huấn luyện thành công toàn bộ 6 mô hình Hướng Thuần (có class_weight='balanced'), tự động lưu 7 models/scalers, xuất CSV/JSON và 12 biểu đồ PNG chất lượng cao | `src/baseline/run_all_baseline.py`<br>`models/baseline/*`<br>`results/metrics/*`<br>`results/figures/*`<br>`.gitignore`<br>`requirements.txt` | Hoàn thành 100% Giai đoạn 2 Hướng Thuần |
| `2026-09-17 15:38` | Antigravity AI | Lập trình xong toàn bộ khung ETL, Baseline ML, Spark MLlib phân tán, sửa lỗi maxBins=512 cho sân bay, tạo 2 Jupyter Notebooks (EDA & Kaggle) | `src/etl/*`<br>`src/baseline/*`<br>`src/spark_ml/*`<br>`src/utils/*`<br>`notebooks/*`<br>`docker/*`<br>`requirements.txt` | Toàn bộ script đã chạy thử nghiệm thành công 100% trên máy Local |
| `2026-09-17 14:04` | Antigravity AI | Khởi tạo Git repo, cấu hình `.gitignore` chặn file 1.3GB, tạo `README.md` gốc và push lên GitHub | `.gitignore`<br>`README.md`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Đã push thành công lên https://github.com/Okeydokey8525/BigData |
| `2026-09-16 17:48` | Antigravity AI | Cập nhật hệ thống 6 mô hình (LR, DT, RF, LightGBM, CatBoost, XGBoost) & Chiến lược cộng tác 3 người (Git-Kaggle-Local) | `doc/02_ARCHITECTURE_AND_TECHSTACK.md`<br>`doc/07_EXPERIMENT_AND_EVALUATION_METRICS.md`<br>`doc/09_DUAL_APPROACH_PLAN.md`<br>`doc/10_MASTER_EXECUTION_PLAN.md` | Hoàn thiện ma trận RACI và phản biện Kaggle vs Local |
| `2026-09-16 17:35` | Antigravity AI | Đo đạc phần cứng máy Local, phân tích 2 hướng Thuần vs Spark và lập Master Execution Plan | `doc/00_AI_READ_FIRST.md`<br>`doc/08_HARDWARE_AND_ENVIRONMENT_SPECS.md`<br>`doc/09_DUAL_APPROACH_PLAN.md`<br>`doc/10_MASTER_EXECUTION_PLAN.md`<br>`doc/05_WORKLOG_AND_HANDOVER.md` | Bám sát `nguyen-tac-lam-viec-dai.md` và phản hồi của GVHD |


| `2026-09-16 15:56` | Antigravity AI | Bổ sung cột nghĩa tiếng Việt `meaning_vi` vào Data Dictionary | [`flight_data_2024_data_dictionary.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/flight_data_2024_data_dictionary.csv) | Đã mã hóa UTF-8-BOM chuẩn tiếng Việt, đủ 35 cột |
| `2026-09-16 15:47` | Antigravity AI | Khởi tạo trọn bộ tài liệu kiến trúc, đặc tả dữ liệu và quy chuẩn AI | `doc/README.md`<br>`doc/01_DATASET_SPECIFICATION.md`<br>`doc/02_ARCHITECTURE_AND_TECHSTACK.md`<br>`doc/03_ROADMAP_AND_TASKS.md`<br>`doc/04_AI_AGENT_GUIDELINES.md` | Bám sát đề cương Word của Nhóm 6 (TS. Phan Hồ Viết Trường) |
| `2026-09-16 15:39` | Antigravity AI | Khảo sát cấu trúc tệp dữ liệu gốc 1.3 GB, đếm dòng thực tế | Terminal command (Python CSV inspector) | Xác định chính xác 7,079,081 dòng và 35 thuộc tính |

---

## 3. MẪU BÀN GIAO CHO AI TIẾP THEO (HANDOVER TEMPLATE)

Khi AI hoàn thành nhiệm vụ, hãy copy mẫu này và chèn vào đầu bảng Worklog ở trên, đồng thời cập nhật lại Mục 1:

```markdown
| YYYY-MM-DD HH:MM | Tên AI / Người làm | [Tóm tắt ngắn gọn việc đã làm] | [Danh sách file tạo / sửa] | [Trạng thái kiểm thử / Kết quả] |
```
