# NHẬT KÝ THỰC HIỆN & BÀN GIAO CÔNG VIỆC (WORKLOG & HANDOVER PROTOCOL)

> **QUY ĐỊNH BẮT BUỘC CHO MỌI AI AGENT / THÀNH VIÊN DỰ ÁN:**  
> 1. **Trước khi bắt đầu:** Đọc mục **"1. TRẠNG THÁI HIỆN TẠI (CURRENT STATE)"** để biết việc gì đang làm dở dang và việc gì cần làm tiếp theo.  
> 2. **Sau khi hoàn thành:** Bắt buộc ghi nhận nội dung đã làm vào mục **"2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)"** và cập nhật lại mục **"1. TRẠNG THÁI HIỆN TẠI"** để bàn giao cho AI kế tiếp.

---

## 1. TRẠNG THÁI HIỆN TẠI (CURRENT STATE)

* **Cập nhật lần cuối:** `2026-09-17 16:45:00 (GMT+7)`
* **Giai đoạn hiện tại:** **Giai đoạn 2 - Hoàn thành Thực nghiệm Toàn diện 6 Mô hình Hướng Thuần & Cơ chế Lưu Trữ Tự Động**
* **Trạng thái công việc:** Đã hoàn thành 100% huấn luyện và đối sánh 6 mô hình Hướng Thuần (LR, DT, RF CPU, LightGBM, CatBoost, XGBoost) trên tập mẫu 10.000 dòng có xử lý mất cân bằng lớp (`class_weight='balanced'`). Toàn bộ trọng số mô hình, bảng số liệu CSV/JSON và 12 biểu đồ trực quan đã được tự động xuất lưu vào `models/baseline/`, `results/metrics/`, `results/figures/`.

### 1.1. Việc vừa hoàn thành (Just Completed)
* [x] Cài đặt thành công các thư viện học máy hiện đại: `lightgbm 4.7.0`, `xgboost 3.4.1`, `catboost 1.2.10`, `psutil 7.2.2`, `joblib 1.6.0` vào môi trường ảo `venv`.
* [x] Khởi tạo hệ thống thư mục lưu trữ kết quả phân tầng:
  * `models/baseline/`: Lưu trữ 7 tệp mô hình và scaler `.joblib`.
  * `results/metrics/`: Lưu trữ `baseline_model_comparison.csv` và `baseline_summary.json`.
  * `results/figures/`: Lưu trữ 12 ảnh đồ thị khoa học 300 DPI (Dashboard đối sánh, Confusion Matrix, Feature Importance).
* [x] Lập trình script điều phối Master [`src/baseline/run_all_baseline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/baseline/run_all_baseline.py): Tự động hóa quy trình Train -> Evaluate -> Measure RAM/Time -> Save Models -> Export Metrics & Plots.
* [x] Thực thi kiểm thử và thu thập số liệu thực nghiệm:
  * **Random Forest CPU (Scikit-Learn):** Huấn luyện trong 0.222s, Accuracy `56.86%`, Weighted F1 `60.90%`, Macro F1 `18.28%`.
  * **LightGBM:** Huấn luyện trong 0.357s, Accuracy `55.13%`, Weighted F1 `60.18%`, Macro F1 `20.32%`.
  * **XGBoost:** Huấn luyện trong 0.693s, Accuracy `48.07%`, Weighted F1 `55.28%`, Macro F1 `20.57%`.
* [x] Cập nhật `.gitignore` loại trừ `catboost_info/` và các tệp `.joblib` nặng, giữ lại thư mục `results/` để đồng bộ lên GitHub.

### 1.2. Việc đang tiến hành (In Progress)
* [ ] Đồng bộ mã nguồn và kết quả (`results/`) lên GitHub repository (`origin main`).
* [ ] Chuẩn bị bước Feature Engineering nâng cao (tỷ lệ trễ lịch sử theo hãng bay, chỉ số tắc nghẽn sân bay).

### 1.3. VIỆC TIẾP THEO CẦN LÀM NGAY (NEXT IMMEDIATE TASKS CHO AI KẾ TIẾP)
Bất kỳ AI Agent nào tiếp nhận yêu cầu tiếp theo, hãy thực hiện theo thứ tự ưu tiên sau:

1. **Ưu tiên 1:** Đẩy commit mới lên GitHub: `git add .`, `git commit`, `git push origin main`.
2. **Ưu tiên 2:** Chạy thử nghiệm trên Kaggle đối với toàn bộ 7.07 triệu dòng sử dụng notebook [`notebooks/02_kaggle_distributed_training.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/notebooks/02_kaggle_distributed_training.ipynb).
3. **Ưu tiên 3:** Bổ sung Feature Engineering vào `src/etl/feature_engineering.py` để kéo chỉ số Macro F1 lên cao hơn.
4. **Ưu tiên 4:** Xây dựng Web API FastAPI nạp các tệp `.joblib` từ `models/baseline/` để demo giao diện người dùng.

---

## 2. NHẬT KÝ CÁC PHIÊN LÀM VIỆC (WORKLOG)

*Ghi lại theo thứ tự thời gian mới nhất ở trên cùng:*

| Thời gian | Tác nhân (AI/Dev) | Nội dung công việc đã hoàn thành | Tệp tin tạo mới / Chỉnh sửa | Ghi chú & Đánh giá |
| :--- | :--- | :--- | :--- | :--- |
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
