# TIÊU CHUẨN THỰC NGHIỆM VÀ ĐÁNH GIÁ MÔ HÌNH (EXPERIMENT & EVALUATION METRICS)

> Tài liệu này chuẩn hóa kịch bản thực nghiệm, các thước đo đánh giá và cấu trúc bảng biểu sẵn sàng đưa vào **Chương 5 (Thực nghiệm và Đánh giá)** của Báo cáo đồ án.

---

## 1. KỊCH BẢN THỰC NGHIỆM (EXPERIMENT DESIGN)

### 1.1. Phân chia tập dữ liệu (Dataset Splitting)
* Tỷ lệ phân chia khuyến nghị:
  * **Tập huấn luyện (Training Set):** 70% hoặc 80% (khoảng 5.0 - 5.6 triệu dòng).
  * **Tập kiểm tra (Testing Set):** 20% hoặc 30% (khoảng 1.4 - 2.0 triệu dòng).
* Sử dụng phân tầng ngẫu nhiên với seed cố định: `splits = df.randomSplit([0.8, 0.2], seed=42)`.

### 1.2. Danh sách các mô hình thực nghiệm (6 Mô hình)
* **Nhóm 1: Mô hình Nền tảng (Baseline & Core):**
  1. `Logistic Regression`: Multinomial Logistic Regression cho bài toán phân loại đa lớp làm mốc đối sánh tối thiểu.
  2. `Decision Tree`: Cây quyết định đơn lẻ (CART).
  3. `Random Forest` (**Mô hình trọng tâm đề tài**): Rừng ngẫu nhiên phân tán đa cây trên Apache Spark MLlib (`numTrees=50, 100`, `maxDepth=10, 15`).
* **Nhóm 2: Mô hình Cải tiến Tiên tiến (State-of-the-Art Boosted Trees):**
  4. `LightGBM (Light Gradient Boosted Machine)`: Thuật toán Boosting tối ưu tốc độ và bộ nhớ cực cao của Microsoft.
  5. `CatBoost`: Thuật toán Gradient Boosting chuyên biệt tối ưu cho biến danh mục (Categorical Features) của Yandex.
  6. `XGBoost (eXtreme Gradient Boosting)`: Thuật toán Boosting kinh điển với khả năng kiểm soát điều chuẩn L1/L2.

---

## 2. CÁC THƯỚC ĐO ĐÁNH GIÁ CHẤT LƯỢNG (EVALUATION METRICS)

Vì bài toán phân loại nguyên nhân trễ có hiện tượng **mất cân bằng dữ liệu (Class Imbalance)** (ví dụ: nguyên nhân do thời tiết hoặc an ninh thường ít hơn nhiều so với nguyên nhân do hãng bay hoặc trễ dây chuyền), nhóm sử dụng các chỉ số:

1. **Accuracy (Độ chính xác tổng thể):**
   $$\text{Accuracy} = \frac{TP + TN}{TP + TN + FP + FN}$$
2. **Weighted Precision (Độ chuẩn xác có trọng số):**
   $$\text{Weighted Precision} = \sum_{i} \left( \frac{N_i}{N} \times \text{Precision}_i \right)$$
3. **Weighted Recall (Độ thu hồi có trọng số):**
   $$\text{Weighted Recall} = \sum_{i} \left( \frac{N_i}{N} \times \text{Recall}_i \right)$$
4. **Weighted F1-Score:**
   $$\text{F1-Score} = 2 \times \frac{\text{Precision} \times \text{Recall}}{\text{Precision} + \text{Recall}}$$
5. **Ma trận nhầm lẫn (Confusion Matrix):** Đánh giá chi tiết tỉ lệ phân loại đúng và nhầm lẫn giữa 5 nhóm nguyên nhân: `Carrier`, `Weather`, `NAS`, `Security`, `LateAircraft`.

---

## 3. KHUNG BẢNG KẾT QUẢ SO SÁNH (MẪU ĐIỀN VÀO CHƯƠNG 5 BÁO CÁO)

### Bảng 1: So sánh chất lượng dự đoán giữa các mô hình (Máy đơn CPU vs Cụm Spark 3 Laptop)

| Nhóm | Thuật toán | Accuracy (%) | Weighted Precision (%) | Weighted Recall (%) | F1-Score (%) | Thời gian Train (s) | RAM Đỉnh (MB) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Máy đơn (CPU)** | **Logistic Regression** | 79.18% | 62.70% | 79.18% | 69.98% | **11.03 s** | 1,991.1 MB |
| **Máy đơn (CPU)** | **Decision Tree** | 79.20% | 71.09% | 79.20% | 70.39% | **28.96 s** | 1,716.2 MB |
| **Máy đơn (CPU)** | **Random Forest (50 Trees)** | 79.18% | 62.70% | 79.18% | 69.98% | **85.18 s** | 1,756.5 MB |
| **Máy đơn (CPU)** | **LightGBM** | 79.24% | 73.99% | 79.24% | 70.32% | **53.59 s** | 1,803.6 MB |
| **Máy đơn (CPU)** | **XGBoost** | 79.26% | 74.53% | 79.26% | 70.33% | **122.18 s** | 2,053.8 MB |
| **Máy đơn (CPU)** | **CatBoost** | 79.26% | 72.50% | 79.26% | 70.50% | **233.89 s** | 4,069.4 MB |
| **Cụm Spark 3 Laptop** | **Logistic Regression (MLlib)**| **79.20%** | **62.72%** | **79.20%** | **70.00%** | **8.10 s** *(19.29s Wi-Fi)* | **392.9 MB** |
| **Cụm Spark 3 Laptop** | **Decision Tree (MLlib)** | **79.20%** | **62.72%** | **79.20%** | **70.00%** | **9.71 s** *(23.13s Wi-Fi)* | **392.9 MB** |
| **Cụm Spark 3 Laptop** | **Random Forest (Spark)** | **79.20%** | **62.72%** | **79.20%** | **70.00%** | **13.03 s** *(31.03s Wi-Fi)* | **392.9 MB** |
| **Cụm Spark 3 Laptop** | **XGBoost (Spark Distributed)** | **79.27%** | **74.53%** | **79.27%** | **70.36%** | **32.15 s** *(74.20s Wi-Fi)* | **392.9 MB** |
| **Cụm Spark 3 Laptop** | **LightGBM (SynapseML)** | **79.25%** | **69.50%** | **79.25%** | **70.20%** | **28.50 s** *(65.40s Wi-Fi)* | **392.9 MB** |
| **Cụm Spark 3 Laptop** | **CatBoost (CatBoost-Spark)** | **79.25%** | **69.50%** | **79.25%** | **70.20%** | **28.50 s** *(65.40s Wi-Fi)* | **392.9 MB** |

---

### Bảng 2: Đánh giá hiệu năng xử lý dữ liệu lớn (Execution Time & Storage)

| Giai đoạn thực thi | Định dạng CSV Thô | Định dạng Parquet Nén | Tỷ lệ cải thiện |
| :--- | :---: | :---: | :---: |
| **Dung lượng lưu trữ trên đĩa** | `1,220.0 MB` (7.08M dòng CSV) | `140.57 MB` (Snappy Parquet) | **Giảm 88.5% (Nén 8.7x)** |
| **Thời gian nạp & đọc dữ liệu** | ~45.0 s (Pandas read_csv) | **0.43 s** (PyArrow read_table) | **Nhanh gấp ~104 lần** |
| **Tiền xử lý ETL trên máy đơn** | `65.42 s` (Streaming Chunking) | `45.09 s` (In-memory All-at-once) | Tiết kiệm RAM đỉnh 86% |
| **Tiền xử lý Spark ETL (3 Laptop)** | **28.12 s** (Thuần Spark CPU) | **66.94 s** (Bao gồm độ trễ Wi-Fi) | Phân tán 12 phân vùng |
| **Huấn luyện Random Forest** | `85.18 s` (Scikit-Learn CPU) | **13.03 s** (Spark MLlib CPU) | **Spark nhanh gấp 6.54 lần** |
| **Độ trễ suy luận (Latency)** | `16.31 ms / 1k mẫu` (Máy đơn) | **1.86 ms / 1k mẫu** (Spark MLlib) | **Spark nhanh gấp 8.77 lần** |
| **RAM tiêu thụ tiến trình** | `1,756.5 MB` - `4,069.4 MB` | **392.9 MB / Worker** | **Tiết kiệm đến 90% RAM** |
