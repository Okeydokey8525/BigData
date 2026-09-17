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

### Bảng 1: So sánh chất lượng dự đoán giữa các mô hình (Nền tảng vs Cải tiến)

| Nhóm | Thuật toán | Accuracy (%) | Weighted Precision (%) | Weighted Recall (%) | F1-Score (%) | Thời gian huấn luyện |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Nền tảng** | **Logistic Regression** (Baseline) | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* |
| **Nền tảng** | **Decision Tree** | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* |
| **Trọng tâm**| **Random Forest** (Đề xuất chính) | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* |
| **Cải tiến** | **LightGBM** (Microsoft) | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* |
| **Cải tiến** | **CatBoost** (Yandex) | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* |
| **Cải tiến** | **XGBoost** (eXtreme Boosting) | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* | *[Điền]* |


### Bảng 2: Đánh giá hiệu năng xử lý dữ liệu lớn (Execution Time)

| Giai đoạn thực thi | Định dạng CSV (Thời gian) | Định dạng Parquet (Thời gian) | Tỷ lệ tăng tốc (Speedup) |
| :--- | :---: | :---: | :---: |
| **Thời gian nạp & đọc dữ liệu** | *[VD: ~45s]* | *[VD: ~5s]* | *[VD: 9.0x]* |
| **Thời gian tiền xử lý & biến đổi** | *[Điền]* | *[Điền]* | *[Điền]* |
| **Thời gian Feature Engineering** | *[Điền]* | *[Điền]* | *[Điền]* |
| **Thời gian huấn luyện Random Forest**| *[Điền]* | *[Điền]* | *[Điền]* |
| **Dung lượng lưu trữ trên đĩa** | `1,309 MB` | *[Ước tính ~180 MB]* | **Giảm ~86%** |
