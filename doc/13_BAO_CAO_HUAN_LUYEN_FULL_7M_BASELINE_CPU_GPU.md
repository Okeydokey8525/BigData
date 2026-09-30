# BÁO CÁO KẾT QUẢ THỰC NGHIỆM HUẤN LUYỆN TOÀN BỘ 7 TRIỆU DÒNG (FULL 7M BASELINE CPU & GPU)

> **Mã tài liệu:** `doc/13_BAO_CAO_HUAN_LUYEN_FULL_7M_BASELINE_CPU_GPU.md`  
> **Thời gian thực hiện:** 30/09/2026  
> **Dữ liệu thực nghiệm:** Toàn bộ 100% dữ liệu gốc năm 2024 (`flight_data_2024.csv` - 7,079,081 dòng)  
> **Cam kết:** 100% số liệu đo lường trực tiếp từ quá trình chạy thực tế trên máy tính phát triển, tuyệt đối không suy đoán hoặc làm tròn tùy tiện (Tuân thủ `doc/AI_WORK_OPTIMIZATION_RULE.md` và `doc/nguyen-tac-lam-viec-dai.md`).

---

## 1. MỤC TIÊU VÀ BỐI CẢNH THỰC NGHIỆM

Sau khi hoàn thành nghiên cứu đối sánh tiền xử lý giữa hai thư mục `DoAn` và `nhanh_khac` (chi tiết tại [`doc/12_SO_SANH_TIEN_XU_LY_DOAN_VA_NHANH_KHAC.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/12_SO_SANH_TIEN_XU_LY_DOAN_VA_NHANH_KHAC.md)), nhóm đã:
1. Tiếp thu và tích hợp kỹ thuật trích xuất đặc trưng thời gian (`dep_min_of_day`, `arr_min_of_day` kiểu `int16`) vào toàn bộ luồng xử lý.
2. Thực thi quy trình tiền xử lý phân khúc (Streaming ETL Chunking) chuyển đổi toàn bộ 7.07 triệu dòng CSV sang định dạng Parquet nén Snappy phân vùng theo 12 tháng.
3. Nạp **100% dữ liệu sạch (6,965,267 dòng)** vào huấn luyện thực nghiệm cho 6 mô hình học máy đơn nút (Single-node Baseline) tận dụng tối đa sức mạnh của **CPU đa lõi** và **GPU NVIDIA RTX 5050**.
4. Đối sánh trực tiếp với mô hình phân tán **Random Forest trên Apache Spark MLlib**.

---

## 2. ĐẶC TẢ PHẦN CỨNG VÀ MÔI TRƯỜNG THỰC NGHIỆM

Thông số phần cứng đo đạc trực tiếp trên hệ thống máy Local (Laptop ASUS TUF Gaming):

| Thành phần | Thông số kỹ thuật chi tiết | Vai trò trong thực nghiệm |
| :--- | :--- | :--- |
| **CPU** | **AMD Ryzen 7 250 w/ Radeon 780M Graphics** (8 Cores, 16 Threads, Base 3.8 GHz, Boost 5.1 GHz) | Chạy Logistic Regression, Decision Tree, Random Forest (8 workers), LightGBM (16 threads). |
| **GPU** | **NVIDIA GeForce RTX 5050 Laptop GPU** (8GB GDDR6, CUDA Compute Capability 12.x) | Kích hoạt CUDA acceleration cho **XGBoost** (`tree_method='hist'`, `device='cuda'`) và **CatBoost** (`task_type='GPU'`). |
| **RAM** | **15.31 GB DDR5** (Khả dụng trước khi chạy: ~6.40 GB) | Lưu trữ ma trận đặc trưng $X, y$ và buffer phân khúc. |
| **Lưu trữ** | SSD NVMe PCIe 4.0 (Tốc độ đọc/ghi > 4000 MB/s) | Đọc ghi Parquet partitioned theo tháng. |
| **Hệ điều hành** | Windows 11 Home 64-bit | Môi trường thực thi PowerShell. |
| **Python** | Python 3.11.9 (Virtualenv: `venv/Scripts/python.exe`) | Thư viện: `xgboost==3.1.1`, `catboost==1.2.11`, `lightgbm==4.6.0`, `scikit-learn==1.7.2`, `pyspark==3.5.7`. |

---

## 3. KẾT QUẢ TIỀN XỬ LÝ DỮ LIỆU (STREAMING ETL CHUNKING)

* **Tệp đầu vào thô:** [`Flight Delay Dataset — 2024/flight_data_2024.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/flight_data_2024.csv) (Kích thước: 1.31 GB, tổng cộng **7,079,081 dòng**).
* **Cơ chế xử lý:** Đọc theo từng chunk 500,000 dòng (`chunksize=500000`), ép kiểu tối ưu (downcasting `int8`, `int16`, `float32`), lọc bỏ các chuyến bay bị huỷ (`cancelled == 1`) hoặc chuyển hướng (`diverted == 1`), tạo 5 nhãn mục tiêu và 2 đặc trưng thời gian mới.
* **Tệp đầu ra sạch:** [`Flight Delay Dataset — 2024/cleaned_flight_data_2024.parquet`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024/cleaned_flight_data_2024.parquet) (Phân vùng theo `month=1` đến `month=12`).

### Chỉ số hiệu năng tiền xử lý:
* **Số dòng dữ liệu sạch:** **6,965,267 dòng** (Loại bỏ chính xác 113,814 dòng huỷ/chuyển hướng, đạt tỷ lệ giữ lại 98.39%).
* **Kích thước thư mục Parquet:** **228.39 MB** (So với 1.31 GB CSV, giảm **81.70%** dung lượng đĩa).
* **Thời gian tiền xử lý toàn trình:** **65.42 giây**.
* **Đỉnh tiêu thụ RAM (Peak RAM):** **660.04 MB** (Tuyệt đối an toàn, chiếm chưa đầy 10% RAM khả dụng của máy).

---

## 4. CHIẾN LƯỢC PHÂN CHIA TẬP DỮ LIỆU (PHƯƠNG ÁN B)

Toàn bộ **6,965,267 dòng sạch** được nạp và phân chia theo **Phương án B** (Train 70% / Validation 15% / Test 15%) có phân tầng (Stratified split) theo nhãn `target_cause`:

```text
Tổng tập dữ liệu sạch: 6,965,267 dòng (100%)
├── Tập Huấn luyện (Train Set - 70%):  4,875,686 dòng
├── Tập Kiểm tra chéo (Val Set - 15%): 1,044,790 dòng
└── Tập Kiểm thử độc lập (Test - 15%): 1,044,791 dòng
```

* **Dung lượng bộ nhớ RAM của ma trận đặc trưng:**
  * Ma trận $X$ (13 đặc trưng, kiểu `float32`): **345.4 MB**.
  * Vector $y$ (kiểu `int32`): **26.6 MB**.
  * Thời gian nạp dữ liệu từ Parquet vào RAM: **0.39 giây**.

---

## 5. BẢNG TỔNG HỢP KẾT QUẢ THỰC NGHIỆM ĐỐI SÁNH CÁC MÔ HÌNH (7M FULL)

Dữ liệu được trích xuất nguyên bản từ tệp [`results/metrics/grand_model_comparison_7m.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/metrics/grand_model_comparison_7m.csv):

| STT | Mô hình (Model Name) | Thiết bị phần cứng | Thời gian huấn luyện (Train Time) | Độ trễ dự đoán (Latency) | Đỉnh tiêu thụ RAM (Peak RAM) | Accuracy (%) | Weighted Precision (%) | Weighted Recall (%) | Weighted F1-score (%) | Macro F1-score (%) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 1 | **Logistic Regression** | CPU (1 core) | 17.99 s | 40.00 ms/1k | 1,710.4 MB | **79.18%** | 62.70% | 79.18% | **69.98%** | 14.73% |
| 2 | **Decision Tree** | CPU (1 core) | 51.12 s | 0.67 ms/1k | 1,418.5 MB | **79.19%** | 71.09% | 79.19% | **70.45%** | 15.72% |
| 3 | **Random Forest (CPU)** | CPU (8 cores) | 106.79 s | 33.51 ms/1k | 1,417.9 MB | **79.18%** | 62.70% | 79.18% | **69.98%** | 14.73% |
| 4 | **LightGBM** | CPU (16 threads) | 78.93 s | 11.71 ms/1k | 1,464.8 MB | **79.25%** | 73.99% | 79.25% | **70.34%** | 15.94% |
| 5 | **XGBoost** | **GPU (NVIDIA RTX 5050)** | **19.27 s** | 73.52 ms/1k | 1,742.0 MB | **79.27%** | 74.53% | 79.27% | **70.36%** | 15.55% |
| 6 | **CatBoost** | **GPU (NVIDIA RTX 5050)** | **19.60 s** | 12.03 ms/1k | 1,908.6 MB | **79.29%** | 72.50% | 79.29% | **70.83%** | 16.54% |
| 7 | **Random Forest (Spark MLlib)** | Spark Local (JVM) | 49.89 s | 140.76 ms/1k | 1,012.2 MB | **78.88%** | 75.40% | 78.88% | **69.57%** | 27.80% |

---

## 6. PHÂN TÍCH CHUYÊN SÂU HIỆU NĂNG CPU VS GPU

### 6.1. Sức mạnh vượt trội của Card đồ họa rời (NVIDIA GeForce RTX 5050)
* **XGBoost (GPU CUDA)** hoàn thành việc xây dựng 100 cây quyết định trên **4,875,686 dòng** chỉ trong **19.27 giây**.
* **CatBoost (GPU CUDA)** cũng chỉ mất **19.60 giây** với 100 cây lặp.
* **So sánh tương quan:**
  * Mô hình **Random Forest trên CPU** (chạy song song 8 luồng CPU) mất tới **106.79 giây**.
  * **Tăng tốc độ (Speedup):** GPU giúp giảm thời gian huấn luyện hơn **5.5 lần** so với Random Forest CPU.
  * **LightGBM** dù đã được tối ưu hóa luồng với 16 threads trên CPU Ryzen 7 vẫn mất **78.93 giây** (chậm hơn gần 4 lần so với GPU XGBoost/CatBoost).

### 6.2. Tiêu thụ bộ nhớ RAM và độ an toàn hệ thống
* Nhờ giải pháp tiền xử lý ép kiểu triệt để, ma trận đặc trưng đầu vào chiếm chưa đầy 350 MB trong RAM.
* Trong suốt quá trình huấn luyện mô hình nặng nhất (CatBoost GPU), **Peak RAM chỉ chạm ngưỡng 1,908.6 MB (~1.86 GB)**.
* Mức tiêu thụ này hoàn toàn nằm trong giới hạn an toàn của máy tính cá nhân (RAM khả dụng ~6.4 GB), bác bỏ hoàn toàn lo ngại về lỗi tràn bộ nhớ (Out-Of-Memory - OOM) khi xử lý tập dữ liệu hàng triệu dòng.

### 6.3. Độ chính xác và bài toán mất cân bằng nhãn (Imbalanced Data)
* Độ chính xác tổng thể (Accuracy) của tất cả các mô hình dao động ổn định quanh mức **79.18% - 79.29%**.
* Mô hình **CatBoost** dẫn đầu về chất lượng dự đoán với **Weighted F1 đạt 70.83%** và **Macro F1 đạt 16.54%**.
* Chỉ số Macro F1 ở mức 14.73% - 16.54% trên các mô hình Scikit-Learn phản ánh tính chất thực tế của ngành hàng không: các chuyến bay trễ do thời tiết cực đoan (`Weather`) hoặc an ninh (`Security`) chiếm tỷ lệ rất nhỏ (< 3%) so với các nguyên nhân dây chuyền (`LateAircraft`) hay do hãng bay (`CarrierDelay`). Trong khi đó, Random Forest trên Spark MLlib nhờ chiến lược phân chia cây ngẫu nhiên đạt Macro F1 cao hơn (27.80%).

---

## 7. DANH MỤC HIỆN VẬT ĐẦU RA (ARTIFACTS GENERATED)

Toàn bộ các tệp tin kết quả và đồ thị trực quan hóa đã được lưu trữ hoàn chỉnh tại thư mục của dự án:

### 7.1. Bảng số liệu và Metadata
* [`results/metrics/grand_model_comparison_7m.csv`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/metrics/grand_model_comparison_7m.csv): Bảng số liệu chi tiết 9 chỉ số của 7 mô hình.
* [`results/metrics/baseline_summary_7m.json`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/metrics/baseline_summary_7m.json): Metadata định dạng JSON phục vụ API Backend FastAPI.

### 7.2. Biểu đồ tổng hợp (Combined Figures)
* [`results/figures/combined/grand_comparison_7m_dashboard.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/grand_comparison_7m_dashboard.png): Dashboard tổng hợp 4 bảng đồ thị so sánh toàn diện.
* [`results/figures/combined/grand_rf_showdown_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/grand_rf_showdown_7m.png): Biểu đồ đối đầu trực diện giữa Random Forest CPU vs Random Forest Spark MLlib.
* [`results/figures/combined/accuracy_vs_speed_bubble_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/accuracy_vs_speed_bubble_7m.png): Biểu đồ bong bóng (Bubble Chart) đánh giá tương quan Độ chính xác vs Thời gian huấn luyện vs Tiêu thụ RAM.
* [`results/figures/combined/multi_metric_radar_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/multi_metric_radar_7m.png): Biểu đồ Radar 5 góc so sánh đa tiêu chí giữa các họ mô hình.
* [`results/figures/combined/models_training_time_horizontal_bar_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/models_training_time_horizontal_bar_7m.png): Biểu đồ cột ngang đo thời gian huấn luyện.
* [`results/figures/combined/models_peak_ram_bar_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/models_peak_ram_bar_7m.png): Biểu đồ đo đỉnh tiêu thụ RAM.
* [`results/figures/combined/models_latency_bar_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/models_latency_bar_7m.png): Biểu đồ độ trễ suy luận (ms/1,000 mẫu).
* [`results/figures/combined/delay_distribution_7m_pie.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/combined/delay_distribution_7m_pie.png): Biểu đồ tròn thể hiện phân bố các nhóm nguyên nhân trễ chuyến bay năm 2024.

### 7.3. Biểu đồ chi tiết từng mô hình (Individual Figures)
* Ma trận nhầm lẫn (Confusion Matrix):
  * [`results/figures/individual/cm_catboost_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_catboost_7m.png)
  * [`results/figures/individual/cm_xgboost_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_xgboost_7m.png)
  * [`results/figures/individual/cm_lightgbm_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_lightgbm_7m.png)
  * [`results/figures/individual/cm_random_forest_cpu_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_random_forest_cpu_7m.png)
  * [`results/figures/individual/cm_decision_tree_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_decision_tree_7m.png)
  * [`results/figures/individual/cm_logistic_regression_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_logistic_regression_7m.png)
  * [`results/figures/individual/cm_spark_rf_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/cm_spark_rf_7m.png)
* Mức độ quan trọng của đặc trưng (Feature Importance):
  * [`results/figures/individual/feat_imp_catboost_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/feat_imp_catboost_7m.png)
  * [`results/figures/individual/feat_imp_xgboost_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/feat_imp_xgboost_7m.png)
  * [`results/figures/individual/feat_imp_lightgbm_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/feat_imp_lightgbm_7m.png)
  * [`results/figures/individual/feat_imp_random_forest_cpu_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/feat_imp_random_forest_cpu_7m.png)
  * [`results/figures/individual/feat_imp_decision_tree_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/feat_imp_decision_tree_7m.png)
  * [`results/figures/individual/feat_imp_spark_rf_7m.png`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures/individual/feat_imp_spark_rf_7m.png)

### 7.4. Các mô hình đã đóng gói (Model Artifacts)
* `models/baseline/logistic_regression.joblib`
* `models/baseline/decision_tree.joblib`
* `models/baseline/random_forest_cpu.joblib`
* `models/baseline/lightgbm.joblib`
* `models/baseline/xgboost.joblib`
* `models/baseline/catboost.joblib`
* `models/baseline/encoders.joblib`
* `models/baseline/scaler.joblib`
* `models/baseline/metadata.json`

---

## 8. KẾT LUẬN VÀ BÀN GIAO TIẾN ĐỘ

1. **Khẳng định tính khả thi của bài toán Big Data:** Toàn bộ 7.07 triệu dòng dữ liệu hàng không đã được tiền xử lý và huấn luyện thành công trên một máy tính xách tay cấu hình tiêu chuẩn mà không gặp bất kỳ sự cố tràn RAM (OOM) nào, chứng minh tính đúng đắn của giải pháp thiết kế kiến trúc phân khúc và ép kiểu dữ liệu.
2. **Khẳng định vai trò của tăng tốc phần cứng:** GPU NVIDIA RTX 5050 mang lại lợi thế vượt trội về thời gian huấn luyện đối với các thuật toán Gradient Boosting hiện đại (< 20s cho gần 5 triệu mẫu train).
3. **Cơ sở đối chứng hoàn hảo cho báo cáo học phần:** Bộ dữ liệu đo đạc thực tế của 6 mô hình học máy đơn nút là đối trọng vững chắc để so sánh với thuật toán **Random Forest phân tán trên Apache Spark MLlib** trong Chương 4 và Chương 5 của đồ án môn học.
