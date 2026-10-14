# ỨNG DỤNG THUẬT TOÁN RANDOM FOREST PHÂN TÁN DỰ ĐOÁN NGUYÊN NHÂN TRỄ CHUYẾN BAY THƯƠNG MẠI: TRƯỜNG HỢP TẠI HOA KỲ NĂM 2024

[![Học phần](https://img.shields.io/badge/H%E1%BB%8Dc%20Ph%E1%BA%A7n-Nh%E1%BA%ADp%20M%C3%B4n%20Big%20Data-blue?style=for-the-badge)](https://huit.edu.vn)
[![Đơn vị](https://img.shields.io/badge/%C4%90%C6%A1n%20V%E1%BB%8B-HUIT-orange?style=for-the-badge)](https://huit.edu.vn)
[![Apache Spark](https://img.shields.io/badge/Distributed%20Engine-Apache%20Spark%203.5-red?style=for-the-badge&logo=apache-spark)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-brightgreen?style=for-the-badge&logo=python)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/Web%20Dashboard-FastAPI-teal?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)

---

## 📌 THÔNG TIN HỌC PHẦN & NHÓM THỰC HIỆN

* **Cơ sở đào tạo:** Trường Đại học Công Thương Thành phố Hồ Chí Minh (HUIT)
* **Khoa:** Công nghệ Thông tin
* **Học phần:** Nhập môn Big Data (Mã lớp học phần: HK VII)
* **Giảng viên hướng dẫn:** **TS. Phan Hồ Viết Trường**
* **Nhóm sinh viên thực hiện:** **Nhóm 6** (Lịch học: Thứ 4, Ca 10 - 12)

| STT | Họ và Tên | Mã số sinh viên (MSSV) | Vai trò / Nhiệm vụ chính |
| :---: | :--- | :---: | :--- |
| 1 | **Cù Văn Vĩ An** | `2001230005` | Triển khai cụm phân tán Apache Spark, tối ưu hóa RDD/Parquet, Benchmark |
| 2 | **Lê Đức Lương** | `2001230490` | Xây dựng Pipeline MLlib Random Forest, huấn luyện Baseline 7M, Dashboard Web_Air |
| 3 | **Trần Huỳnh Tuấn Anh** | `2001230023` | Xử lý dữ liệu thô BTS, trực quan hóa biểu đồ khoa học IEEE, biên soạn báo cáo |

---

## 🎯 TỔNG QUAN ĐỀ TÀI & BÀI TOÁN KỸ THUẬT

### 1. Bối cảnh & Thách thức Big Data
Ngành hàng không Hoa Kỳ vận hành hàng chục nghìn chuyến bay mỗi ngày. Năm 2024, tổng số chuyến bay ghi nhận đạt hơn **7.07 triệu bản ghi**. Việc chuyến bay bị hoãn gây thiệt hại kinh tế hàng tỷ USD và làm gián đoạn kế hoạch của hàng triệu hành khách.
Tuy nhiên, việc phân tích dữ liệu quy mô lớn này gặp phải 3 thách thức kỹ thuật lớn:
1. **Nghẽn I/O & Lưu trữ:** Tệp CSV thô kích thước lớn (~1.25 GB) tốn nhiều thời gian đọc tuần tự trên đĩa từ.
2. **Nghẽn bộ nhớ (RAM/OOM):** Xử lý toàn bộ dữ liệu trên một máy tính cá nhân bằng Pandas/Scikit-learn dễ gây tràn bộ nhớ (Out-Of-Memory - OOM), đặc biệt nếu mã hóa One-Hot Encoding cho hàng trăm sân bay sẽ làm bùng nổ số chiều (>800 cột).
3. **Thời gian huấn luyện:** Thuật toán Machine Learning truyền thống trên CPU đơn luồng mất hàng giờ đồng hồ để hội tụ.

### 2. Mục tiêu nghiên cứu
Đề tài xây dựng một đường ống xử lý dữ liệu lớn (Big Data Pipeline) hoàn chỉnh nhằm:
* **Tiền xử lý phân tán (Distributed ETL):** Dùng Apache Spark xử lý sạch **7,079,081 dòng**, nén và phân vùng thành định dạng **Apache Parquet (Snappy)** theo 12 tháng, giảm **88.7%** dung lượng lưu trữ.
* **Dự đoán nguyên nhân trễ (Multiclass Classification):** Phân loại chính xác chuyến bay vào **6 nhóm nguyên nhân** theo quy chuẩn của Cục Thống kê Giao thông Vận tải Hoa Kỳ (US BTS).
* **Đối chứng thực nghiệm song song 2 hướng:**
  * **Hướng Thuần (Single-node Baseline):** Sử dụng CPU/GPU đơn máy huấn luyện 6 mô hình (Logistic Regression, Decision Tree, Random Forest, LightGBM, CatBoost, XGBoost) để đo đạc giới hạn tài nguyên.
  * **Hướng Phân tán (Distributed Spark MLlib):** Triển khai mô hình **Random Forest phân tán** trên cụm máy tính (Cluster), chứng minh tính vượt trội về tốc độ huấn luyện, khả năng mở rộng (Scalability) và tối ưu hóa bộ nhớ.
* **Ứng dụng thực tế (Web_Air Dashboard):** Cung cấp giao diện trực quan cho phép tra cứu, dự đoán chuyến bay thời gian thực và so sánh đồng thời kết quả của các mô hình.

---

## 📊 BỘ DỮ LIỆU & QUY CHUẨN ĐẶC TRƯNG

### 1. Thông tin tập dữ liệu
* **Tên bộ dữ liệu:** [Flight Delay Dataset — 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) (Thu thập từ Cục Thống kê Vận tải Hoa Kỳ - US Bureau of Transportation Statistics - BTS DOT).
* **Quy mô gốc:** **7,079,081 dòng**, **35 cột thuộc tính**, dung lượng CSV: **~1.25 GB**.
* **Dữ liệu sau làm sạch (ETL):** **6,965,267 dòng** (đã loại bỏ 113,814 chuyến bay bị hủy `Cancelled=1` hoặc chuyển hướng `Diverted=1`), lưu dưới dạng Parquet chỉ còn **140.57 MB**.

### 2. Sáu (06) nhóm nguyên nhân trễ mục tiêu (Target Classes)
Mỗi chuyến bay được gán vào 1 trong 6 nhãn theo nguyên nhân chiếm ưu thế lớn nhất:
* `Nhãn 0 - OnTime`: Chuyến bay đúng giờ hoặc trễ không đáng kể (thời gian trễ đến `< 15 phút`).
* `Nhãn 1 - Carrier`: Trễ do hãng hàng không (bảo trì kỹ thuật, phi hành đoàn, dọn dẹp, tra nạp nhiên liệu).
* `Nhãn 2 - Weather`: Trễ do điều kiện thời tiết cực đoan (bão tuyết, mưa đá, sương mù dày đặc).
* `Nhãn 3 - NAS`: Trễ do Hệ thống Không phận Quốc gia (National Airspace System - lưu lượng không lưu đông đúc, điều phối cất hạ cánh tại sân bay).
* `Nhãn 4 - Security`: Trễ do an ninh (sự cố an ninh nhà ga, kiểm tra an ninh lại hành khách).
* `Nhãn 5 - LateAircraft`: Trễ dây chuyền do máy bay bị trễ ở chặng trước đến muộn.

### 3. Mười ba (13) đặc trưng đầu vào sau tiền xử lý
Để đảm bảo an toàn bộ nhớ và ngăn chặn rò rỉ dữ liệu (Data Leakage):
* **9 đặc trưng số (Numerical):** `month` (tháng), `day_of_month` (ngày trong tháng), `day_of_week` (thứ trong tuần), `dep_hour` (giờ khởi hành 0-23), `dep_minute` (phút khởi hành 0-59), `distance` (khoảng cách bay theo dặm), `crs_elapsed_time` (thời gian bay dự kiến), `dep_delay` (thời gian trễ cất cánh thực tế), `taxi_out` (thời gian lăn bánh ra đường băng).
* **4 đặc trưng phân loại (Categorical):** `carrier_code_idx` (mã hãng bay), `origin_idx` (sân bay xuất phát), `dest_idx` (sân bay hạ cánh), `delay_group_idx` (nhóm khoảng thời gian trễ theo chuẩn BTS).
* *Quy chuẩn mã hóa:* Sử dụng `StringIndexer` (Spark) và `LabelEncoder` (CPU) thay vì One-Hot Encoding để giữ cố định 13 cột, tránh bùng nổ số chiều gây OOM.

---

## 🗂️ CẤU TRÚC THƯ MỤC SOURCE CODE

Thư mục nộp bài được cấu trúc khoa học theo chuẩn kỹ nghệ phần mềm và Big Data:

```text
Nhom6_DoAn_BigData_SourceCode/
├── README.md                           # Cẩm nang giới thiệu và hướng dẫn toàn diện (File này)
├── requirements.txt                    # Danh mục thư viện Python phụ thuộc
├── run_web_air.bat                     # [1-Click] Khởi chạy Web Dashboard thời gian thực
├── run_spark_pipeline.bat              # [1-Click] Chạy thực nghiệm Apache Spark phân tán
├── run_single_machine_cpu.bat          # [1-Click] Chạy thực nghiệm Baseline 6 mô hình CPU
├── Nhom6_T4_C10-12_BaoCao.docx         # Đề cương đồ án học phần đã được duyệt
├── Bao_Cao/                            # Báo cáo học phần chính thức định dạng IEEE (.docx)
├── Flight Delay Dataset — 2024/        # Thư mục dữ liệu
│   ├── flight_data_2024_data_dictionary.csv  # Từ điển giải nghĩa 35 cột dữ liệu (kèm tiếng Việt)
│   └── flight_data_2024_sample.csv           # 10,000 dòng dữ liệu mẫu chạy kiểm thử nhanh
├── doc/                                # Bộ tài liệu kỹ thuật, kiến trúc và phân tích thực nghiệm
├── models/                             # Trọng số các mô hình đã huấn luyện trên 7 triệu dòng
│   ├── baseline/                       # 6 mô hình CPU/GPU (.joblib), scaler, encoders, metadata
│   └── spark/                          # Mô hình Random Forest phân tán Spark MLlib
├── results/                            # Kết quả thực nghiệm và biểu đồ khoa học
│   ├── metrics/                        # Báo cáo số liệu thực nghiệm định dạng JSON và CSV
│   └── figures/                        # 24 biểu đồ khoa học chất lượng cao chuẩn IEEE (PNG)
├── src/                                # Toàn bộ mã nguồn chương trình Python của đề tài
│   ├── etl/                            # Tiền xử lý dữ liệu và chuyển đổi định dạng
│   ├── baseline/                       # Huấn luyện mô hình nhánh đơn máy (Single-node)
│   ├── spark_ml/                       # Huấn luyện mô hình nhánh phân tán (Apache Spark MLlib)
│   ├── serving/                        # REST API prototype phục vụ suy luận
│   └── utils/                          # Công cụ đo đạc tài nguyên và vẽ biểu đồ tự động
└── Web_Air/                            # Ứng dụng Web Dashboard giao diện người dùng
    ├── app.py                          # Backend FastAPI xử lý các API dự đoán và số liệu
    ├── config.py                       # Cấu hình đường dẫn, danh sách đặc trưng, metadata
    ├── run.bat                         # Kịch bản khởi chạy nhanh server Web
    ├── core/                           # Bộ nạp mô hình vào RAM và động cơ suy luận
    ├── templates/                      # Giao diện HTML5 (3 Tab chức năng)
    └── static/                         # CSS Glassmorphic và JavaScript AJAX
```

---

## 📖 BẢNG TRA CỨU CHỨC NĂNG CÁC TỆP PYTHON (`src/`)

Dưới đây là bảng giải nghĩa chi tiết chức năng của từng tệp mã nguồn Python để người đọc dễ dàng nắm bắt:

### 1. Phân hệ Tiền xử lý Dữ liệu (`src/etl/`)

| Tên tệp Python | Chức năng chính (Giải nghĩa tiếng Việt) | Chi tiết kỹ thuật |
| :--- | :--- | :--- |
| [`spark_etl.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/etl/spark_etl.py) | **Tiền xử lý phân tán Apache Spark** | Sử dụng PySpark DataFrame & Spark SQL đọc CSV 7M dòng, loại bỏ bản ghi bị hủy/chuyển hướng, tính toán nhãn trễ, tối ưu bộ nhớ và ghi ra định dạng Snappy Parquet phân vùng theo 12 tháng (`partitionBy('month')`). |
| [`clean_data.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/etl/clean_data.py) | **Làm sạch dữ liệu nhánh đơn máy** | Sử dụng Pandas/Polars để lọc dữ liệu, xử lý missing values, chuẩn hóa các trường số và sinh tập dữ liệu sạch phục vụ nhánh thực nghiệm CPU/GPU. |
| [`to_parquet.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/etl/to_parquet.py) | **Chuyển đổi CSV sang Parquet** | Kịch bản chuyển đổi định dạng lưu trữ từ CSV thô sang Parquet có nén Snappy để tăng tốc độ I/O và tiết kiệm dung lượng đĩa. |
| [`benchmark_end_to_end_7m.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/etl/benchmark_end_to_end_7m.py) | **Đo đạc hiệu năng toàn trình ETL** | So sánh đối đầu thời gian nạp và tiền xử lý dữ liệu giữa Pandas trên đơn máy và Apache Spark trên cụm phân tán. |
| [`generate_14m_enriched.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/etl/generate_14m_enriched.py) | **Sinh dữ liệu mở rộng 14 triệu dòng** | Làm giàu và nhân bản dữ liệu có kiểm soát nhằm mục đích thử nghiệm tải cực hạn (Stress Testing) trên cụm phân tán Spark. |
| [`export_metadata.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/etl/export_metadata.py) | **Trích xuất thông tin đặc tả (Metadata)** | Quét tập dữ liệu sau xử lý, trích xuất cấu trúc cột, phân bố nhãn trễ và lưu thành tệp JSON phục vụ Web Dashboard. |

### 2. Phân hệ Huấn luyện Nhánh Đơn Máy (`src/baseline/`)

| Tên tệp Python | Chức năng chính (Giải nghĩa tiếng Việt) | Chi tiết kỹ thuật |
| :--- | :--- | :--- |
| [`train_all_7m.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/baseline/train_all_7m.py) | **Huấn luyện toàn diện 6 mô hình trên 7M dòng** | Kịch bản thực nghiệm trung tâm của nhánh CPU: nạp 6.96M dòng dữ liệu, chia tập Train/Test (80/20), huấn luyện tuần tự 6 thuật toán (Logistic Regression, Decision Tree, Random Forest CPU, LightGBM, CatBoost, XGBoost), đo lường thời gian huấn luyện, tốc độ suy luận, RAM đỉnh và lưu trọng số vào `models/baseline/`. |
| [`run_all_baseline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/baseline/run_all_baseline.py) | **Điều phối chạy tự động Baseline** | Tự động kiểm tra tính khả dụng của dữ liệu, nạp môi trường ảo và kích hoạt chuỗi huấn luyện mô hình cơ sở. |
| [`train_baseline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/baseline/train_baseline.py) | **Huấn luyện thử nghiệm mô hình cơ bản** | Chạy kiểm thử nhanh các mô hình cổ điển (Logistic Regression, Decision Tree) trên tập mẫu (sample) để kiểm tra logic code. |
| [`train_sota.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/baseline/train_sota.py) | **Huấn luyện thử nghiệm mô hình Boosting** | Chạy kiểm thử các mô hình tăng cường độ dốc tiên tiến (LightGBM, CatBoost, XGBoost) trên tập dữ liệu mẫu. |
| [`week_task_dt_catboost.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/baseline/week_task_dt_catboost.py) | **Thực nghiệm đối sánh Cây đơn vs Boosting** | Đối sánh chuyên sâu giữa Decision Tree (mô hình cây đơn phân cấp) và CatBoost (thuật toán boosting hiện đại) trên quy mô dữ liệu lớn. |

### 3. Phân hệ Tính toán Phân tán Apache Spark (`src/spark_ml/`)

| Tên tệp Python | Chức năng chính (Giải nghĩa tiếng Việt) | Chi tiết kỹ thuật |
| :--- | :--- | :--- |
| [`pipeline.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/spark_ml/pipeline.py) | **Xây dựng đường ống Spark MLlib Pipeline** | Đóng gói chuỗi tiền xử lý phân tán: `StringIndexer` cho các cột chuỗi + `VectorAssembler` gộp 13 đặc trưng vào vector + Cấu hình mô hình `RandomForestClassifier` phân tán với siêu tham số (`numTrees=100`, `maxDepth=10`, `maxBins=64`). |
| [`run_end_to_end_spark.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/spark_ml/run_end_to_end_spark.py) | **Kịch bản chạy phân tán toàn trình trên Cụm** | Kết nối `SparkSession` tới Spark Master, đọc Parquet phân vùng, phân chia dữ liệu trên các Worker, huấn luyện mô hình phân tán, đánh giá Ma trận nhầm lẫn (Confusion Matrix), đo mức tiêu thụ RAM trên từng node và lưu mô hình Spark MLlib vào `models/spark/`. |

### 4. Phân hệ Đo đạc & Trực quan hóa (`src/utils/`)

| Tên tệp Python | Chức năng chính (Giải nghĩa tiếng Việt) | Chi tiết kỹ thuật |
| :--- | :--- | :--- |
| [`visualize_results.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/utils/visualize_results.py) | **Sinh tự động 24 biểu đồ khoa học chuẩn IEEE** | Đọc các tệp kết quả JSON/CSV trong `results/metrics/`, sử dụng Matplotlib/Seaborn xuất bản 24 đồ thị trực quan: so sánh thời gian huấn luyện, tốc độ suy luận, mức tiêu thụ RAM, đường cong ROC đa lớp và ma trận nhầm lẫn. |
| [`metrics.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/src/utils/metrics.py) | **Bộ tính toán chỉ số đánh giá phân loại** | Cung cấp các hàm tính toán: Accuracy, Macro F1, Weighted F1, Macro Precision, Macro Recall cho bài toán phân loại 6 lớp mất cân bằng dữ liệu. |

### 5. Phân hệ Ứng dụng Web Dashboard (`Web_Air/`)

| Tên tệp Python | Chức năng chính (Giải nghĩa tiếng Việt) | Chi tiết kỹ thuật |
| :--- | :--- | :--- |
| [`Web_Air/app.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/Web_Air/app.py) | **Máy chủ FastAPI phục vụ giao diện người dùng** | Cung cấp 9 REST API endpoints: dự đoán đơn lẻ, dự đoán đối đầu cùng lúc cả 6 mô hình, trả về bảng đối sánh 9 chỉ số hiệu năng và phục vụ thư viện 24 biểu đồ khoa học. |
| [`Web_Air/config.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/Web_Air/config.py) | **Cấu hình tham số và đường dẫn ứng dụng** | Định nghĩa 13 đặc trưng đầu vào, 6 nhãn nguyên nhân trễ kèm mã màu/icon, đường dẫn nạp mô hình trong `models/` và số liệu trong `results/`. |
| `Web_Air/core/model_loader.py` | **Bộ nạp mô hình Singleton vào bộ nhớ RAM** | Nạp trước toàn bộ các mô hình `.joblib` và scaler vào RAM khi khởi động server, giúp giảm độ trễ phản hồi xuống dưới 15ms. |
| `Web_Air/core/predictor.py` | **Động cơ thực thi suy luận thời gian thực** | Nhận tham số chuyến bay từ form, chuẩn hóa dữ liệu qua scaler, gọi mô hình dự đoán xác suất và sinh lời khuyên cho hành khách. |

---

## 📈 KẾT QUẢ THỰC NGHIỆM ĐỐI CHỨNG NỔI BẬT

Đồ án đã tiến hành đo đạc thực nghiệm khắt khe trên toàn bộ **6,965,267 dòng** dữ liệu sạch năm 2024. Dưới đây là các kết quả nổi bật chứng minh tính hiệu quả của hệ thống Big Data phân tán:

### 1. Đối sánh Đơn máy (Single-node CPU) vs Cụm phân tán Apache Spark (3-Node)

| Chỉ số đo đạc | Hướng Thuần (CPU Single-node) | Hướng Spark (3-Node Cluster) | Mức độ cải thiện / Tăng tốc |
| :--- | :---: | :---: | :---: |
| **Thời gian huấn luyện Random Forest** | **85.18 giây** | **13.03 giây** | 🚀 **Spark nhanh hơn 6.54 lần** |
| **Độ trễ suy luận (Inference Latency)** | **16.32 ms / 1k mẫu** | **1.86 ms / 1k mẫu** | ⚡ **Spark nhanh hơn 8.77 lần** |
| **Mức tiêu thụ bộ nhớ RAM** | **1,754.2 MB** (tập trung 1 máy) | **392.9 MB** / worker | 📉 **Tiết kiệm 77.6% RAM/worker** |
| **Thời gian toàn trình (ETL + 6 Models)** | **615.25 giây** (~10.25 phút) | **148.11 giây** (~2.47 phút) | 🏎️ **Nhanh hơn 4.15 lần** (thuần tính toán) |
| **Dung lượng lưu trữ dữ liệu** | **1.25 GB** (CSV thô) | **140.57 MB** (Parquet Snappy) | 💾 **Giảm 88.7% dung lượng đĩa** |

> **Nhận xét chuyên môn:** Kết quả thực nghiệm khẳng định rằng với dữ liệu quy mô lớn (>7 triệu dòng), hệ thống phân tán Apache Spark không chỉ giúp triệt tiêu hiện tượng nghẽn bộ nhớ (OOM) mà còn đem lại tốc độ xử lý vượt bậc nhờ cơ chế tính toán trong bộ nhớ (In-memory computing) và phân tán công việc trên nhiều máy.

### 2. Bảng vàng đối sánh 6 mô hình học máy trên 7 triệu dòng (Single-node Baseline)

| Xếp hạng | Tên mô hình | Thời gian huấn luyện (s) | Suy luận (ms/1k) | RAM Đỉnh (MB) | Accuracy | Macro F1 | Weighted F1 |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 🥇 **1** | **CatBoost** (Chất lượng cao nhất) | 73.08 s | 1.83 ms | 1,939.8 MB | **72.16%** | **70.83%** | **71.85%** |
| 🥈 **2** | **LightGBM** (Cân bằng tốt nhất) | **11.23 s** | **0.86 ms** | 1,720.0 MB | 71.95% | 70.36% | 71.59% |
| 🥉 **3** | **XGBoost** (Tốc độ cao) | 26.65 s | 3.52 ms | 1,842.1 MB | 72.11% | 70.62% | 71.77% |
| 4 | **Random Forest (CPU)** | 85.18 s | 16.32 ms | 1,754.2 MB | 71.80% | 70.18% | 71.43% |
| 5 | **Decision Tree** | 13.98 s | 0.44 ms | 1,732.1 MB | 71.18% | 69.41% | 70.78% |
| 6 | **Logistic Regression** | 20.35 s | 0.31 ms | **1,509.3 MB** | 62.45% | 47.99% | 58.74% |

---

## 🚀 HƯỚNG DẪN CÀI ĐẶT & CHẠY 1-CLICK (QUICK START)

Dự án được thiết kế theo tiêu chí **"1-Click Run"** để giảng viên và người đánh giá có thể kiểm thử dễ dàng nhất.

### 1. Yêu cầu môi trường tiên quyết
* **Hệ điều hành:** Windows 10/11, Linux Ubuntu hoặc macOS.
* **Python:** Phiên bản `3.10`, `3.11` hoặc `3.12` (Khuyến nghị 3.11).
* **Java:** OpenJDK 11 hoặc OpenJDK 17 (Cần thiết cho Apache Spark).
* **RAM:** Tối thiểu 8 GB RAM khả dụng.

### 2. Cài đặt các thư viện phụ thuộc
Mở Command Prompt (cmd) hoặc PowerShell tại thư mục này và chạy:
```bash
# 1. Tạo môi trường ảo Python (Khuyến nghị)
python -m venv venv

# 2. Kích hoạt môi trường ảo
# Trên Windows:
.\venv\Scripts\activate
# Trên Linux/macOS:
source venv/bin/activate

# 3. Cài đặt các gói phụ thuộc
pip install -r requirements.txt
```

### 3. Ba (03) kịch bản chạy 1-Click bằng tệp `.bat` (Windows)

#### 👉 Kịch bản 1: Khởi chạy Web Dashboard tương tác (Khuyến nghị trải nghiệm đầu tiên)
* **Cách thực hiện:** Nhấp đúp chuột vào tệp [`run_web_air.bat`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/run_web_air.bat) (hoặc chạy lệnh: `python -m uvicorn Web_Air.app:app --host 127.0.0.1 --port 8000`).
* **Trình duyệt tự động mở tại:** `http://127.0.0.1:8000`
* **Trải nghiệm:**
  * **Tab 1 (Dự đoán chuyến bay):** Chọn các chuyến bay mẫu hoặc nhập thông số thực tế để dự đoán nguyên nhân trễ và xem đối đầu 6 mô hình.
  * **Tab 2 (Bảng đối sánh):** Xem bảng số liệu 9 chỉ số hiệu năng trên 7 triệu dòng.
  * **Tab 3 (Thư viện biểu đồ):** Phóng to duyệt toàn bộ 24 biểu đồ chuẩn IEEE xuất bản từ thực nghiệm.

#### 👉 Kịch bản 2: Chạy thực nghiệm phân tán Apache Spark
* **Cách thực hiện:** Nhấp đúp chuột vào tệp [`run_spark_pipeline.bat`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/run_spark_pipeline.bat).
* **Mô tả:** Tự động kích hoạt PySpark, nạp dữ liệu mẫu/chính, khởi tạo `Pipeline` phân tán (StringIndexer, VectorAssembler) và huấn luyện `RandomForestClassifier` phân tán.

#### 👉 Kịch bản 3: Chạy thực nghiệm Baseline trên đơn máy CPU
* **Cách thực hiện:** Nhấp đúp chuột vào tệp [`run_single_machine_cpu.bat`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/run_single_machine_cpu.bat).
* **Mô tả:** Tự động nạp dữ liệu và huấn luyện 6 mô hình Machine Learning trên CPU, sau đó tự động kích hoạt `src/utils/visualize_results.py` để sinh đồ thị.

---

## 📚 TÀI LIỆU HỌC THUẬT & BÁO CÁO KÈM THEO

Trong gói mã nguồn này, nhóm đã đính kèm đầy đủ hệ thống văn bản phục vụ chấm thi:
1. **Báo cáo toàn văn chính thức:** Nằm trong thư mục [`Bao_Cao/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/Bao_Cao) (định dạng chuẩn IEEE hai cột, đầy đủ phương pháp luận, kiến trúc toán học và phân tích đối chứng thực nghiệm).
2. **Đề cương đồ án được duyệt:** Tệp [`Nhom6_T4_C10-12_BaoCao.docx`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/Nhom6_T4_C10-12_BaoCao.docx).
3. **Bộ tài liệu kỹ thuật chi tiết:** Nằm trong thư mục [`doc/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_DoAn_BigData_SourceCode/doc):
   * `01_DATASET_SPECIFICATION.md`: Đặc tả chi tiết 35 cột dữ liệu và logic gán nhãn 6 nhóm trễ.
   * `02_ARCHITECTURE_AND_TECHSTACK.md`: Thiết kế kiến trúc phân tán HDFS, Spark Cluster, FastAPI.
   * `07_EXPERIMENT_AND_EVALUATION_METRICS.md`: Tiêu chuẩn đánh giá khoa học các mô hình.
   * `14_HUONG_DAN_TRIEN_KHAI_SPARK_CLUSTER_3_LAPTOP_TAILSCALE.md`: Cẩm nang thiết lập cụm Spark 3 laptop qua mạng ảo.

---

## ⚖️ GIẤY PHÉP & BẢN QUYỀN (LICENSE)

Dự án được thực hiện phục vụ mục đích nghiên cứu và học tập trong khuôn khổ học phần **Nhập môn Big Data** tại **Trường Đại học Công Thương Thành phố Hồ Chí Minh (HUIT)**. Dữ liệu gốc thuộc bản quyền công khai của Cục Thống kê Vận tải Hoa Kỳ (US BTS).
Mọi thắc mắc kỹ thuật vui lòng liên hệ nhóm sinh viên thực hiện (Nhóm 6).
