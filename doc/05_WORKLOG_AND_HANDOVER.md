# NHẬT KÝ THỰC HIỆN & BÀN GIAO CÔNG VIỆC (WORKLOG & HANDOVER PROTOCOL)

> **QUY ĐỊNH BẮT BUỘC CHO MỌI AI AGENT / THÀNH VIÊN DỰ ÁN:**  
> 1. **Trước khi bắt đầu:** Đọc mục **"1. TRẠNG THÁI HIỆN TẠI (CURRENT STATE)"** để biết việc gì đang làm dở dang và việc gì cần làm tiếp theo.  
> 2. **Sau khi hoàn thành:** Bắt buộc ghi nhận nội dung đã làm vào mục **"2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)"** và cập nhật lại mục **"1. TRẠNG THÁI HIỆN TẠI"** để bàn giao cho AI kế tiếp.

---

## 1. TRẠNG THÁI HIỆN TẠI (CURRENT STATE)

* **Cập nhật lần cuối:** `2026-09-17 15:38:00 (GMT+7)`
* **Giai đoạn hiện tại:** **Giai đoạn 1 & 2 - Hoàn thành khung Source Code ETL, Baseline ML, Spark MLlib và Bộ Notebooks Thực nghiệm**
* **Trạng thái công việc:** Toàn bộ pipeline ETL, Baseline ML và Spark MLlib đã được kiểm thử chạy thành công 100% trên dữ liệu mẫu với Java 17; Sẵn sàng đồng bộ lên GitHub và đẩy lên Kaggle để huấn luyện toàn bộ 7.07M dòng.

### 1.1. Việc vừa hoàn thành (Just Completed)
* [x] Đã khởi tạo môi trường ảo Python `venv` (Python 3.13.14) và cài đặt đầy đủ các thư viện cốt lõi: `pyspark 4.2.0`, `pyarrow`, `scikit-learn`, `pandas`, `numpy`, `matplotlib`, `seaborn`.
* [x] Đã tạo tệp cấu hình phụ thuộc [`requirements.txt`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/requirements.txt) và [`docker/docker-compose.yml`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/docker/docker-compose.yml) (Spark Master/Worker + HDFS + MongoDB).
* [x] Đã lập trình và kiểm thử thành công [`src/etl/clean_data.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/clean_data.py): Lọc bỏ chuyến bay hủy/chuyển hướng, gán nhãn 6 nhóm nguyên nhân trễ chuẩn BTS, xuất `cleaned_sample.parquet` (9.836 dòng hợp lệ).
* [x] Đã lập trình và kiểm thử thành công [`src/etl/to_parquet.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/to_parquet.py): Phân vùng Parquet theo `month`, đạt tỷ lệ nén 49.97%.
* [x] Đã lập trình module chuẩn hóa đánh giá [`src/utils/metrics.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/utils/metrics.py): Tính Accuracy, Precision, Recall, F1-Score đa lớp và đo độ trễ huấn luyện/dự báo.
* [x] Đã lập trình và kiểm thử thành công [`src/baseline/train_baseline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/baseline/train_baseline.py): Huấn luyện Logistic Regression, Decision Tree, Random Forest CPU (Accuracy ~78.4%).
* [x] Đã lập trình và kiểm thử thành công [`src/spark_ml/pipeline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/spark_ml/pipeline.py): Pipeline phân tán trên PySpark (`StringIndexer` -> `VectorAssembler` -> `RandomForestClassifier`), cấu hình `maxBins=512` xử lý mã sân bay phân loại cao, chạy hoàn tất trong 14.87s với OpenJDK 17.
* [x] Đã khởi tạo 2 Jupyter Notebooks chuẩn mực:
  * [`notebooks/01_eda_sample.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/notebooks/01_eda_sample.ipynb): Phân tích khám phá dữ liệu, vẽ biểu đồ phân bố trễ cho Chương 3 báo cáo.
  * [`notebooks/02_kaggle_distributed_training.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/notebooks/02_kaggle_distributed_training.ipynb): Mẫu chạy thực nghiệm trên Kaggle Cloud cho 3 thành viên nhóm (huấn luyện 7.07M dòng trên 2x Tesla T4).

### 1.2. Việc đang tiến hành (In Progress)
* [ ] Đồng bộ toàn bộ mã nguồn `src/`, `notebooks/`, `docker/` lên GitHub repository (`origin main`).
* [ ] Chuẩn bị huấn luyện quy mô lớn trên Kaggle (7.07 triệu dòng).

### 1.3. VIỆC TIẾP THEO CẦN LÀM NGAY (NEXT IMMEDIATE TASKS CHO AI KẾ TIẾP)
Bất kỳ AI Agent nào tiếp nhận yêu cầu tiếp theo, hãy thực hiện theo thứ tự ưu tiên sau:

1. **Ưu tiên 1:** Đẩy commit mới lên GitHub: `git add .`, `git commit`, `git push origin main`.
2. **Ưu tiên 2:** Mở notebook trên Kaggle hoặc hướng dẫn nhóm tải notebook `notebooks/02_kaggle_distributed_training.ipynb` lên Kaggle để chạy 7.07M dòng trên GPU T4.
3. **Ưu tiên 3:** Bổ sung Feature Engineering vào `src/etl/feature_engineering.py` (tính tỷ lệ trễ lịch sử hãng bay, độ tắc nghẽn sân bay) để nâng cao F1-score cho các lớp thiểu số.
4. **Ưu tiên 4:** Viết khung Web API FastAPI (`src/serving/api.py`) kết nối MongoDB và load mô hình đã lưu.

---

## 2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)

*Ghi lại theo thứ tự thời gian mới nhất ở trên cùng:*

| Thời gian | Tác nhân (AI/Dev) | Nội dung công việc đã hoàn thành | Tệp tin tạo mới / Chỉnh sửa | Ghi chú & Đánh giá |
| :--- | :--- | :--- | :--- | :--- |
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
