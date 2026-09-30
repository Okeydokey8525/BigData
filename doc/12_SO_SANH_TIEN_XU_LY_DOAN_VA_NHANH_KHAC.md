# BÁO CÁO ĐỐI SÁNH CHI TIẾT TIỀN XỬ LÝ DỮ LIỆU: FOLDER `DoAn` VÀ FOLDER `nhanh_khac`

> **Tài liệu tham chiếu:**
> - Quy chuẩn tối ưu: [`AI_WORK_OPTIMIZATION_RULE.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/AI_WORK_OPTIMIZATION_RULE.md)
> - Chuẩn mực làm việc & minh bạch dữ liệu: [`nguyen-tac-lam-viec-dai.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/nguyen-tac-lam-viec-dai.md)
> - Ngày lập: 30/09/2026
> - Trạng thái kiểm chứng: **100% các số liệu và nhận định đều được đối chiếu trực tiếp từ mã nguồn thực tế (CONFIRMED)**.

---

## 1. TỔNG QUAN PHẠM VI KHẢO SÁT

Báo cáo này tiến hành rà soát, mổ xẻ và đối chiếu toàn diện các bước tiền xử lý (Preprocessing), làm sạch (Data Cleaning), chọn lọc đặc trưng (Feature Selection), biến đổi đặc trưng (Feature Engineering) và phân chia dữ liệu (Data Splitting) giữa 2 thư mục dự án:

1. **Thư mục `DoAn`** (`c:\LeDucLuong\HK VII\NhapMonBigData\DoAn`):
   - Mã nguồn ETL/Tiền xử lý: [`src/etl/clean_data.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/clean_data.py), [`src/etl/to_parquet.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/to_parquet.py), [`src/etl/export_metadata.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/etl/export_metadata.py).
   - Mã nguồn huấn luyện & Pipeline: [`src/spark_ml/pipeline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/spark_ml/pipeline.py), [`src/baseline/run_all_baseline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/baseline/run_all_baseline.py), [`src/baseline/train_all_7m.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/src/baseline/train_all_7m.py).
   - Tài liệu kỹ thuật: [`doc/01_DATASET_SPECIFICATION.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/01_DATASET_SPECIFICATION.md), [`README.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/README.md).
2. **Thư mục `nhanh_khac`** (`c:\LeDucLuong\HK VII\NhapMonBigData\nhanh_khac`):
   - Mã nguồn tiền xử lý (Jupyter Notebooks):
     - [`notebooks/01_data_understanding.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/nhanh_khac/notebooks/01_data_understanding.ipynb)
     - [`notebooks/02_data_cleaning.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/nhanh_khac/notebooks/02_data_cleaning.ipynb)
     - [`notebooks/03_correlation_analysis.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/nhanh_khac/notebooks/03_correlation_analysis.ipynb)
     - [`notebooks/04_feature_selection.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/nhanh_khac/notebooks/04_feature_selection.ipynb)
     - [`notebooks/05_data_split.ipynb`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/nhanh_khac/notebooks/05_data_split.ipynb)
   - Mã nguồn mô hình hóa liên quan: `06_random_forest_corrected.ipynb`, `07_decision_tree.ipynb`, `08_catboost.ipynb`, `09_xgboost.ipynb`.

---

## 2. BẢNG MA TRẬN ĐỐI SÁNH TỔNG QUAN

| Tiêu chí | Nhánh `DoAn` | Nhánh `nhanh_khac` | Trạng thái xác minh |
| :--- | :--- | :--- | :---: |
| **1. Định nghĩa bài toán ML** | **Multi-class Classification (6 lớp)**<br>• Lớp 0: `OnTime_or_MinorDelay`<br>• Lớp 1-5: `Carrier`, `Weather`, `NAS`, `Security`, `LateAircraft` | **Multi-label Classification (5 bài toán Binary)**<br>• 5 biến mục tiêu 0/1 riêng biệt: `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay` | `CONFIRMED` |
| **2. Điều kiện xác định trễ** | Tuân thủ FAA/BTS: Chỉ chuyến bay $\text{arr\_delay} \ge 15$ mới xét nguyên nhân trễ chính (`argmax` thời gian trễ). $\text{arr\_delay} < 15$ đưa về lớp 0. | Không lọc theo $\text{arr\_delay} \ge 15$. Bất kỳ chuyến bay nào có giá trị delay cột $> 0$ đều gắn nhãn 1 cho cột đó. | `CONFIRMED` |
| **3. Lọc chuyến bay bất thường** | Lọc **CẢ** chuyến bay hủy (`cancelled == 0`) **VÀ** chuyển hướng (`diverted == 0`). | Chỉ lọc chuyến bay hủy (`cancelled == 0`). **KHÔNG lọc** `diverted`. | `CONFIRMED` |
| **4. Xử lý giá trị Missing** | • Cột delay: điền `0`.<br>• Cột `dep_delay`: điền `0`.<br>• Cột `arr_delay`: `dropna`.<br>• Giữ lại tối đa bản ghi vì không dùng các cột hậu bay làm feature. | • Xóa dòng missing trên 11 cột: `dep_time`, `dep_delay`, `taxi_out`, `wheels_off`, `wheels_on`, `arr_time`, `arr_delay`, `actual_elapsed_time`, `air_time`, `op_carrier_fl_num`, `crs_elapsed_time`. | `CONFIRMED` |
| **5. Số dòng sau làm sạch** | Dự kiến giữ lại **~6,965,267 dòng** (nếu trên file 7M gốc). | Chính xác **6,965,266 dòng** (Loại 96,315 dòng hủy + 17,500 dòng missing từ 7,079,081 dòng gốc). | `CONFIRMED` |
| **6. Phân tích tương quan & EDA** | Thực hiện trong notebook `01_eda_sample.ipynb` và các script thống kê. | Thực hiện rất bài bản trong `03_correlation_analysis.ipynb`: Pearson (Numeric), Pearson (Feature-Feature để phát hiện đa cộng tuyến), Cramér's V (Categorical - Target). | `CONFIRMED` |
| **7. Số lượng đặc trưng đầu vào** | **11 features**:<br>• 7 Numeric: `month`, `day_of_month`, `day_of_week`, `dep_hour`, `arr_hour`, `crs_elapsed_time`, `distance`.<br>• 4 Categorical: `op_unique_carrier`, `origin`, `dest`, `dep_time_of_day`. | **11 features** trong model (ban đầu chọn 12, sau đó bỏ `fl_date`):<br>• 8 Numeric: `month`, `day_of_month`, `day_of_week`, `op_carrier_fl_num`, `crs_dep_time`, `crs_arr_time`, `crs_elapsed_time`, `distance`.<br>• 3 Categorical: `op_unique_carrier`, `origin`, `dest`. | `CONFIRMED` |
| **8. Kỹ thuật sinh đặc trưng mới (Feature Engineering)** | • Trích xuất giờ: `dep_hour`, `arr_hour`.<br>• Phân nhóm khung giờ bay (`dep_time_of_day`: Morning, Afternoon, Evening, Night). | • Chuyển đổi giờ HHMM sang tổng số phút trong ngày: `hours * 60 + minutes` (ví dụ `1018` -> `618` phút). | `CONFIRMED` |
| **9. Xử lý cột `op_carrier_fl_num`** | **Loại bỏ hoàn toàn** do số hiệu chuyến bay mang tính định danh phân tán cao, dễ gây overfitting và nhiễu mô hình. | **Giữ lại làm biến số** (Numeric Feature) cho mô hình học máy. | `CONFIRMED` |
| **10. Mã hóa biến phân loại (Encoding)** | • **LabelEncoder** (đối với Scikit-learn/Boosted Trees), giữ nguyên 11 chiều.<br>• **StringIndexer + VectorAssembler** (trên Spark MLlib). Không làm bùng nổ số chiều. | • **OneHotEncoder (sparse)** qua `ColumnTransformer` cho `op_unique_carrier`, `origin`, `dest`.<br>• **Bùng nổ số chiều lên 719 features** (do có >350 sân bay). | `CONFIRMED` |
| **11. Chiến lược chia dữ liệu (Data Splitting)** | • **Train 80% / Test 20%**.<br>• Phân tầng nhãn đơn (`Stratified` theo `delay_cause_code` trên Scikit-Learn hoặc `randomSplit` trên Spark). | • **Train 70% / Validation 15% / Test 15%**.<br>• Kỹ thuật: **`MultilabelStratifiedShuffleSplit`** (thư viện `iterstrat`), phân tầng đồng đều cả 5 nhãn nhị phân. | `CONFIRMED` |
| **12. Định dạng & Lưu trữ trung gian** | **Apache Parquet (Snappy)** phân vùng theo tháng (`partitionBy="month"`). Kèm cơ chế **Streaming Chunking** (500k dòng/mẩu). | **Tệp CSV thô** (`flight_data_7m_cleaned.csv`, `flight_data_7m_selected.csv`, `train.csv`, `validation.csv`, `test.csv`). | `CONFIRMED` |
| **13. Tối ưu hóa RAM (Downcasting)** | Có bảng quy chuẩn Downcasting rõ ràng: `int8`, `int16`, `float32`, `category`. Giảm RAM nạp 7M từ ~7.5GB xuống ~2.2GB. | Sử dụng kiểu dữ liệu mặc định của Pandas (`int64`, `float64`, `object`). | `CONFIRMED` |
| **14. Kiến trúc triển khai** | Script module hóa (`src/etl/`), CLI có tham số, sẵn sàng tích hợp cụm phân tán **Apache Spark MLlib & HDFS**. | Chuỗi tuần tự trên **Jupyter Notebooks** (.ipynb), vận hành đơn máy (Single-node). | `CONFIRMED` |

---

## 3. PHÂN TÍCH CHI TIẾT TỪNG KHÍA CẠNH KỸ THUẬT

### 3.1. Sự khác biệt cốt lõi về bản chất bài toán (Problem Formulation)

#### Nhánh `DoAn` (Multi-class Single-Label):
- **Cơ sở nghiệp vụ:** Bám sát quy định của Cục Thống kê Giao thông Vận tải Hoa Kỳ (BTS) và Cục Hàng không Liên bang (FAA): Một chuyến bay chỉ tính là chậm trễ nếu thời gian trễ hạ cánh $\text{arr\_delay} \ge 15$ phút. Khi trễ, chuyến bay sẽ được gắn nhãn nguyên nhân chủ đạo (Primary Dominant Cause) chiếm số phút trễ lớn nhất:
  $$\text{delay\_cause} = \arg\max(\text{carrier\_delay}, \text{weather\_delay}, \text{nas\_delay}, \text{security\_delay}, \text{late\_aircraft\_delay})$$
- **Hệ thống nhãn mục tiêu:** Gồm 6 lớp duy nhất:
  - `0`: Đúng giờ hoặc trễ không đáng kể (`OnTime_or_MinorDelay` - chiếm ~78-80%).
  - `1`: Trễ do Hãng hàng không (`Carrier`).
  - `2`: Trễ do Thời tiết (`Weather`).
  - `3`: Trễ do Không lưu quốc gia (`NAS`).
  - `4`: Trễ do An ninh sân bay (`Security`).
  - `5`: Trễ do Tàu bay đến muộn dây chuyền (`LateAircraft`).
- **Ưu điểm:** Phù hợp với bài toán ra quyết định vận hành sân bay/hãng bay (cần biết nguyên nhân chính yếu nhất để quy trách nhiệm/xử lý bồi thường); tương thích tự nhiên với thuật toán `RandomForestClassifier` phân tán trên Spark MLlib.
- **Hạn chế:** Bỏ qua trường hợp chuyến bay bị trễ do nhiều nguyên nhân kết hợp (ví dụ vừa do thời tiết 30 phút, vừa do bảo trì hãng 20 phút).

#### Nhánh `nhanh_khac` (Multi-label Classification):
- **Cơ sở nghiệp vụ:** Nhận định rằng một chuyến bay trễ có thể xuất phát từ nhiều nguyên nhân đồng thời. Thống kê từ `data_understanding_summary.txt` cho thấy: 78.81% chuyến có 0 nguyên nhân, 10.33% có 1 nguyên nhân, 8.88% có 2 nguyên nhân, 1.97% có 3 nguyên nhân và 0.01% có 4 nguyên nhân.
- **Biến mục tiêu:** 5 biến nhị phân độc lập ($Y \in \{0, 1\}^5$):
  $$Y_{\text{cause}} = \mathbb{I}(\text{cause\_delay} > 0)$$
- **Cách huấn luyện:** Tách thành 5 bài toán phân loại nhị phân (Binary Classification) độc lập cho từng nguyên nhân:
  ```python
  for target in TARGET_COLUMNS:
      model.fit(X_train, Y_train[target])
  ```
- **Ưu điểm:** Nắm bắt được tính chất đa nguyên nhân cùng xuất hiện trên một chuyến bay.
- **Hạn chế:**
  - Không phân biệt chuyến bay trễ nghiêm trọng ($\ge 15$ phút) với chuyến bay chỉ bị ghi nhận vài phút delay nhỏ lẻ.
  - Phải duy trì và huấn luyện 5 mô hình riêng biệt (hoặc mô hình đa mục tiêu), làm tăng gấp 5 lần chi phí huấn luyện trên tập dữ liệu 7 triệu dòng.

---

### 3.2. Quy trình làm sạch dữ liệu & Xử lý Missing Values

#### Nhánh `nhanh_khac` (`02_data_cleaning.ipynb`):
1. **Lọc trùng:** `df.drop_duplicates()` (loại 0 dòng trên dữ liệu gốc).
2. **Lọc chuyến bay hủy:** `df[df["cancelled"] == 0]` -> Loại bỏ chính xác **96,315 dòng**.
3. **Lọc Missing trên 11 cột:**
   ```python
   MISSING_COLUMNS = [
       "dep_time", "dep_delay", "taxi_out", "wheels_off", "wheels_on",
       "arr_time", "arr_delay", "actual_elapsed_time", "air_time",
       "op_carrier_fl_num", "crs_elapsed_time"
   ]
   df = df.dropna(subset=MISSING_COLUMNS).reset_index(drop=True)
   ```
   Loại bỏ thêm **17,500 dòng**. Tổng dữ liệu còn lại là **6,965,266 dòng**.
4. **Vấn đề tồn tại:**
   - **Chưa lọc chuyến bay chuyển hướng (`diverted == 1`):** Các chuyến bay chuyển hướng không hạ cánh tại sân bay đích dự kiến, nhưng không bị loại ở bước này.
   - **Lọc missing trên các cột hậu bay:** Các cột như `taxi_out`, `wheels_off`, `taxi_in`, `air_time` là những cột bị cấm sử dụng trong mô hình dự báo trước giờ bay (để chống Data Leakage). Việc ép xóa các dòng thiếu giá trị ở các cột này là không cần thiết, làm mất oan một số bản ghi hợp lệ.

#### Nhánh `DoAn` (`src/etl/clean_data.py`):
1. **Điền giá trị 0 cho các cột Delay:** `col.fillna(0)` cho 5 cột nguyên nhân trễ.
2. **Lọc triệt để chuyến bay bất thường:**
   ```python
   valid_mask = (df['cancelled'] == 0) & (df['diverted'] == 0)
   df = df[valid_mask].copy()
   ```
   Loại bỏ cả chuyến bay bị hủy VÀ chuyển hướng, đảm bảo dữ liệu chỉ chứa các hành trình bay hoàn tất từ Origin đến Dest.
3. **Xử lý Missing tinh gọn:**
   - Chỉ loại bỏ các dòng thiếu `arr_delay` (vì đây là cơ sở tính biến mục tiêu).
   - `dep_delay` điền 0 nếu thiếu.
   - Không can thiệp `dropna` vào các cột hậu bay vì các cột đó bị loại bỏ hoàn toàn khỏi Feature Set ngay sau đó.
4. **Tối ưu tốc độ Vectorized:**
   - Sử dụng NumPy vectorization (`vectorized_determine_delay_cause`) xử lý gán nhãn 7 triệu dòng trong < 1 giây, thay vì dùng vòng lặp hoặc `apply` tốn hàng chục phút.

---

### 3.3. Kỹ thuật đặc trưng (Feature Selection & Engineering)

#### 1. Lựa chọn đặc trưng (Feature Selection) & Phòng chống Data Leakage:
- **Điểm chung:** Cả hai nhánh đều nhận thức rất tốt về **Data Leakage (Rò rỉ dữ liệu)**. Cả hai đều loại bỏ các thuộc tính chỉ xuất hiện sau khi cất cánh hoặc sau khi hạ cánh (`dep_time`, `dep_delay`, `taxi_out`, `wheels_off`, `wheels_on`, `taxi_in`, `arr_time`, `arr_delay`, `actual_elapsed_time`, `air_time`).
- **Điểm khác biệt ở tập Features cuối cùng:**

| Thuộc tính | Nhánh `DoAn` | Nhánh `nhanh_khac` | Đánh giá so sánh |
| :--- | :---: | :---: | :--- |
| `month`, `day_of_month`, `day_of_week` | Có | Có | Đồng nhất (chu kỳ lịch) |
| `crs_elapsed_time`, `distance` | Có | Có | Đồng nhất (khoảng cách & hành trình) |
| `op_unique_carrier` | Có | Có | Đồng nhất (hãng hàng không) |
| `origin`, `dest` | Có | Có | Đồng nhất (sân bay đi và đến) |
| `op_carrier_fl_num` | **Loại bỏ** | **Giữ lại** | `DoAn` hợp lý hơn: Số hiệu chuyến bay là định danh (Identifier) mang tính ngẫu nhiên/quá chi tiết, giữ lại dạng numeric dễ khiến mô hình cây học vẹt (overfit). |
| `crs_dep_time`, `crs_arr_time` | Biến đổi thành `dep_hour`, `arr_hour` | Chuyển đổi thành phút trong ngày (`hour*60 + min`) | Cả 2 đều chuyển đổi định dạng HHMM thành biến liên tục hợp lệ. |
| `dep_time_of_day` | **Có** (Morning, Afternoon, Evening, Night) | **Không** | `DoAn` bổ sung đặc trưng miền có ý nghĩa thực tế cao (khung giờ bay cao điểm). |

#### 2. Kỹ thuật mã hóa biến phân loại (Categorical Encoding) - Điểm rẽ nhánh quan trọng nhất:
- **Nhánh `nhanh_khac` dùng One-Hot Encoding:**
  - Áp dụng `OneHotEncoder(handle_unknown="ignore", sparse_output=True)` cho `op_unique_carrier`, `origin`, `dest`.
  - **Hậu quả kỹ thuật:** Do `origin` và `dest` có hơn 350 mã sân bay IATA khác nhau, One-Hot Encoding làm bùng nổ số lượng cột từ 11 lên **719 cột** (theo log thực tế ở `06_random_forest_corrected.ipynb`: `(4875686, 719)`).
  - Ma trận thưa 719 chiều trên gần 5 triệu dòng huấn luyện gây áp lực khổng lồ lên RAM. Thuật toán Random Forest của Scikit-Learn rất khó xử lý sâu trên ma trận thưa này mà không bị suy giảm tốc độ nghiêm trọng hoặc cạn kiệt RAM. Hơn nữa, việc bùng nổ chiều này hoàn toàn không thể chuyển giao trực tiếp sang Apache Spark MLlib nếu không cấu hình lại.
- **Nhánh `DoAn` dùng Label Encoding / StringIndexer:**
  - Hướng đơn máy: Áp dụng `LabelEncoder` cho các biến phân loại. Ma trận giữ nguyên **11 cột**.
  - Hướng phân tán Spark: Áp dụng `StringIndexer` biến đổi chuỗi sang số nguyên, kết hợp `VectorAssembler` gom thành vector đặc trưng cô đọng.
  - **Lợi ích:** Kích thước ma trận cực kỳ nhẹ, huấn luyện nhanh gấp hàng chục lần, không gây tràn bộ nhớ, tương thích chuẩn 100% với hệ thống phân tán Apache Spark.

---

### 3.4. Chiến lược phân chia dữ liệu (Data Splitting Strategy)

#### Nhánh `nhanh_khac` (`05_data_split.ipynb`):
- **Tỷ lệ chia:** 70% Train (4,875,686 dòng) / 15% Validation (1,044,790 dòng) / 15% Test (1,044,790 dòng).
- **Thuật toán:** Sử dụng `MultilabelStratifiedShuffleSplit` từ thư viện `iterstrat`.
- **Đánh giá:** Đây là điểm sáng kỹ thuật rất lớn của `nhanh_khac`. Vì bài toán đặt ra là Multi-label, việc sử dụng `MultilabelStratifiedShuffleSplit` giúp tỷ lệ dương tính của cả 5 nhãn trễ trên 3 tập Train, Val, Test được bảo toàn tuyệt đối đến từng chữ số thập phân:
  - `carrier_delay`: 11.33%
  - `weather_delay`: 1.28%
  - `nas_delay`: 10.42%
  - `security_delay`: 0.11%
  - `late_aircraft_delay`: 10.67%
- **Lưu trữ:** Ghi đĩa 3 file CSV lớn riêng biệt (`train.csv`, `validation.csv`, `test.csv`).

#### Nhánh `DoAn`:
- **Tỷ lệ chia:** 80% Train / 20% Test (hoặc 80/20 chia phân tán qua `randomSplit([0.8, 0.2], seed=42)` trên Spark).
- **Thuật toán:** `Stratified train_test_split` theo biến mục tiêu đa lớp `delay_cause_code` (0 đến 5).
- **Đánh giá:** Đảm bảo tỷ lệ của lớp đa số (OnTime ~80%) và các lớp thiểu số (Security ~0.1%, Weather ~1.3%) phân bố đồng đều giữa tập huấn luyện và kiểm thử.
- **Lưu trữ:** Tách và xử lý trực tiếp trong bộ nhớ/DataFrame hoặc chia ngẫu nhiên qua seed của Spark, không cần nhân bản tạo ra hàng GB file CSV phân mảnh trên ổ đĩa.

---

### 3.5. Tối ưu hóa Big Data, Định dạng lưu trữ & Quản lý RAM

| Tiêu chí kỹ thuật | Nhánh `DoAn` | Nhánh `nhanh_khac` |
| :--- | :--- | :--- |
| **Định dạng file trung gian** | **Apache Parquet (Snappy)**: Tối ưu cột (Columnar format), nén cao. | **CSV**: Tệp văn bản thuần, dung lượng lớn, đọc/ghi chậm. |
| **Phân vùng dữ liệu (Partitioning)** | Có: `partitionBy=["month"]` (Hive partition style). Cho phép Spark chỉ đọc đúng tháng cần truy vấn (Partition Pruning). | Không phân vùng: Lưu trữ các file CSV phẳng lớn (>1.5 GB). |
| **Kỹ thuật nạp dữ liệu lớn** | **Streaming Chunking** (`chunksize=500_000`): Đọc và chuyển đổi từng khối, peak RAM chỉ vài trăm MB. | Đọc toàn bộ file CSV vào RAM một lần bằng `pd.read_csv()`, dễ bị OOM trên máy yếu. |
| **Kỹ thuật Downcasting** | Có quy chuẩn rõ ràng: `int8` (month, day, dow), `int16` (dep_time, arr_time, distance), `float32` (delays), `category` (carrier, airport). | Không áp dụng: Để Pandas tự động nhận kiểu mặc định 64-bit (`int64`, `float64`, `object`). |
| **Mức tiêu thụ RAM ước tính khi nạp 7M** | **~2.2 GB - 2.8 GB** | **~7.5 GB - 8.5 GB** |

---

## 4. TỔNG KẾT ƯU ĐIỂM VÀ HẠN CHẾ CỦA TỪNG BÊN

### 4.1. Nhánh `DoAn`
* **Ưu điểm vượt trội:**
  1. **Tư duy Big Data chuẩn mực:** Sử dụng Apache Parquet có phân vùng theo tháng và Streaming Chunking giúp xử lý trọn vẹn 7 triệu dòng trên máy cá nhân không lo tràn RAM.
  2. **Tối ưu hóa bộ nhớ chuyên sâu (Downcasting):** Giảm dung lượng RAM cần thiết xuống gần 70%.
  3. **Tương thích hoàn hảo với Apache Spark MLlib & HDFS:** Pipeline mã hóa gọn gàng (LabelEncoder / StringIndexer), không làm bùng nổ số chiều.
  4. **Cấu trúc mã nguồn chuyên nghiệp:** Tách thành các module Python (`src/etl/`, `src/spark_ml/`, `src/baseline/`) có CLI arguments rõ ràng, sẵn sàng đóng gói và triển khai.
  5. **Bám sát nghiệp vụ FAA/BTS:** Phân biệt rõ ngưỡng trễ 15 phút và phân loại nguyên nhân chính.
* **Hạn chế:**
  1. Bài toán Multi-class đơn nhãn chưa mô tả được trường hợp trễ do nhiều nguyên nhân đồng thời xảy ra trên cùng một chuyến bay.

### 4.2. Nhánh `nhanh_khac`
* **Ưu điểm vượt trội:**
  1. **Quy trình phân tích khám phá (EDA) và tương quan rất bài bản:** Việc tính toán tỉ mỉ Pearson Correlation (Numeric-Numeric, Numeric-Target) và Cramér's V (Categorical-Target) ở notebook `03` là một điểm cộng học thuật rất lớn.
  2. **Chiến lược chia dữ liệu Multi-label xuất sắc:** Sử dụng `MultilabelStratifiedShuffleSplit` chia 3 tập Train/Val/Test (70/15/15) giữ nguyên tỉ lệ từng nhãn đến từng phần trăm.
  3. **Mô hình hóa linh hoạt theo đa nhãn:** Cho phép dự đoán từng nguyên nhân độc lập.
* **Hạn chế:**
  1. **Bùng nổ số chiều (Curse of Dimensionality):** Áp dụng One-Hot Encoding cho Origin/Dest đẩy số chiều lên 719 cột, làm tê liệt khả năng mở rộng trên Big Data và ngốn RAM cực lớn.
  2. **Thiếu tối ưu hóa lưu trữ:** Toàn bộ lưu dạng CSV thô, gây nghẽn I/O đĩa nghiêm trọng khi đọc/ghi 7 triệu dòng.
  3. **Chưa lọc chuyến bay chuyển hướng (`diverted`):** Bỏ sót việc làm sạch thuộc tính này.
  4. **Giữ lại cột `op_carrier_fl_num`:** Dễ gây overfitting.
  5. **Dạng thức Notebook thuần túy:** Khó tự động hóa, chưa có tích hợp Spark/Hadoop phân tán.

---

## 5. ĐỀ XUẤT TÍCH HỢP ĐỂ HOÀN THIỆN ĐỒ ÁN (RECOMMENDATIONS)

Để đồ án đạt chất lượng học thuật và thực tiễn cao nhất, nhóm nên **kết hợp tinh hoa của cả hai nhánh**:

1. **Về Lưu trữ & Pipeline nền tảng (Áp dụng theo `DoAn`):**
   - Giữ nguyên toàn bộ cơ chế **Apache Parquet Snappy phân vùng theo tháng** và **Downcasting bộ nhớ** của `DoAn`. Tuyệt đối không dùng CSV trung gian và One-Hot Encoding 719 cột của `nhanh_khac`.
2. **Về EDA & Phân tích tương quan (Kế thừa từ `nhanh_khac`):**
   - Đưa toàn bộ các bảng tính toán và biểu đồ tương quan Pearson và Cramér's V từ `nhanh_khac/notebooks/03_correlation_analysis.ipynb` vào báo cáo chính thức và notebook EDA của `DoAn` để tăng tính thuyết phục học thuật.
3. **Về Làm sạch dữ liệu (Hài hòa cả hai):**
   - Duy trì việc lọc cả `cancelled == 0` và `diverted == 0` của `DoAn`.
   - Giữ lại biến đổi giờ HHMM sang số phút trong ngày của `nhanh_khac` kết hợp với khung giờ bay `dep_time_of_day` của `DoAn`.
4. **Về Hướng mở rộng bài toán:**
   - Hướng trọng tâm chính của đề tài (bảo vệ trước hội đồng Big Data): Duy trì bài toán **Multi-class trên Apache Spark MLlib Random Forest phân tán** (nhánh `DoAn`).
   - Hướng mở rộng tham khảo trong báo cáo: Trích dẫn thêm kết quả so sánh đối chứng từ hướng tiếp cận **Multi-label 5 nguyên nhân độc lập** của `nhanh_khac` để làm phong phú nội dung thảo luận.
