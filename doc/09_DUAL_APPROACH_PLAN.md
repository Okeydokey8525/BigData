# KẾ HOẠCH TRIỂN KHAI SONG SONG 2 HƯỚNG: HƯỚNG THUẦN VS HƯỚNG SPARK

> **Tài liệu giải trình phương pháp luận & Kế hoạch thực nghiệm theo yêu cầu của GVHD (TS. Phan Hồ Viết Trường).**

---

## 1. TẠI SAO GIẢNG VIÊN YÊU CẦU LÀM "1 HƯỚNG THUẦN" VÀ "1 HƯỚNG SPARK"?

Trong môn học **Nhập môn Big Data**, câu hỏi phản biện lớn nhất của hội đồng chấm đồ án là:
> *"Tại sao bài toán này phải dùng Apache Spark và công nghệ Big Data phức tạp, trong khi tôi có thể viết một đoạn mã Python thông thường với Pandas và Scikit-learn để giải quyết?"*

Để trả lời thuyết phục câu hỏi này và đạt điểm tối đa, sinh viên **bắt buộc phải xây dựng bài toán đối chứng thực nghiệm (Empirical Benchmark)**:
* Chạy bằng phương pháp **Truyền thống ("Thuần")** $\to$ Chỉ ra giới hạn (Nghẽn bộ nhớ RAM, thời gian đọc đĩa chậm, không thể mở rộng khi dữ liệu vượt ngưỡng đơn máy).
* Chạy bằng phương pháp **Dữ liệu lớn ("Spark")** $\to$ Chứng minh ưu thế vượt trội (Xử lý phân tán trên bộ nhớ, tối ưu hóa định dạng nén Parquet, song song hóa việc huấn luyện mô hình trên các Partition).

---

## 2. ĐỊNH NGHĨA KỸ THUẬT: "CHẠY THUẦN" LÀ CHẠY KIỂU GÌ? (CPU HAY GPU?)

### 2.1. Hướng Thuần 1 (Kinh điển - Bắt buộc): Đơn máy trên CPU (Single-Node CPU)
* **Thư viện sử dụng:** `Pandas` (đọc & xử lý dữ liệu) + `Scikit-learn` (`RandomForestClassifier(n_jobs=-1)`).
* **Bản chất phần cứng:** Chạy hoàn toàn trên **CPU** của 1 máy trạm duy nhất (không có cơ chế Master-Worker phân tán).
* **Thực tế thực nghiệm với 7.07 triệu dòng:**
  * Thao tác `pd.read_csv("flight_data_2024.csv")` nạp vào RAM chiếm ~4.5 GB - 5.5 GB.
  * Trên máy cục bộ (chỉ còn trống ~4.2 GB RAM), việc này sẽ gây **Out Of Memory (OOM)** hoặc đóng băng hệ điều hành.
  * Trên Kaggle (30 GB RAM), code có thể đọc được dữ liệu, nhưng hàm huấn luyện `RandomForestClassifier` của Scikit-learn xây dựng 100 cây trên 7 triệu dòng với CPU đơn máy (4 vCPUs) sẽ chạy rất lâu (dự kiến mất nhiều tiếng đồng hồ).
  * $\to$ **Đây chính là bằng chứng thực nghiệm rõ ràng nhất để bảo vệ luận điểm: "Dữ liệu lớn vượt quá khả năng xử lý hiệu quả của công cụ đơn máy truyền thống".**

### 2.2. Hướng Thuần 2 (Mở rộng nâng cao): Các Mô hình Cải tiến Tiên tiến (SOTA Boosted Trees)
Để chứng minh sự am hiểu công nghệ mới, nhóm triển khai 3 thuật toán Gradient Boosting hiện đại nhất hiện nay:

1. **LightGBM (`lightgbm.LGBMClassifier`):**
   * Cơ chế: GOSS (Gradient-based One-Side Sampling) và EFB (Exclusive Feature Bundling).
   * Tốc độ: Huấn luyện nhanh nhất trong các mô hình dạng cây, tiêu tốn ít RAM nhất.
2. **CatBoost (`catboost.CatBoostClassifier`):**
   * Cơ chế: Ordered Boosting và Target Statistics tích hợp sẵn.
   * Điểm mạnh độc nhất: Xử lý trực tiếp các biến phân loại danh mục cao (`op_unique_carrier`, `origin`, `dest`) mà không cần One-Hot Encoding làm phình to ma trận dữ liệu.
3. **XGBoost (`xgboost.XGBClassifier`):**
   * Cơ chế: eXtreme Gradient Boosting với điều chuẩn L1/L2 regularization và cơ chế Histogram-based split finding.
   * Hỗ trợ tăng tốc phần cứng: Có thể kích hoạt `tree_method='hist'`, `device='cuda'` để tận dụng GPU.

* **Ý nghĩa học thuật đối chứng:** So sánh hiệu năng toàn diện giữa 3 trường phái:
  $$\text{Đơn máy CPU (Scikit-Learn)} \quad \longleftrightarrow \quad \text{Mô hình Cải tiến (LightGBM/CatBoost/XGBoost)} \quad \longleftrightarrow \quad \text{Phân tán (Apache Spark MLlib)}$$


## 3. ĐỊNH NGHĨA KỸ THUẬT: "CHẠY SPARK" LÀ CHẠY KIỂU GÌ?

* **Thư viện sử dụng:** **Apache Spark (PySpark DataFrame, Spark SQL, Spark MLlib)**.
* **Cơ chế hoạt động:**
  * **Tầng lưu trữ & I/O:** Chuyển đổi dữ liệu từ CSV sang **Apache Parquet** có nén **Snappy** và phân vùng theo `month`. Tốc độ đọc đĩa nhanh hơn 8 - 10 lần và dung lượng lưu trữ giảm từ 1.3 GB xuống ~180 MB.
  * **Tầng tính toán:** Khởi tạo `SparkSession` với kiến trúc Driver - Executors. Dữ liệu được chia thành nhiều **Partitions**. Các tác vụ tiền xử lý, lọc, trích xuất đặc trưng (`StringIndexer`, `VectorAssembler`) được thực thi lười (**Lazy Evaluation**) và tối ưu hóa qua trình tối ưu Catalyst Optimizer.
  * **Tầng Machine Learning:** Sử dụng `pyspark.ml.classification.RandomForestClassifier`. Cây quyết định được phân bổ xây dựng song song trên các phân vùng dữ liệu và các Executor.

---

## 4. MA TRẬN THỰC NGHIỆM ĐỐI CHỨNG (BENCHMARK MATRIX CHO CHƯƠNG 5 BÁO CÁO)

Khi làm thực nghiệm, nhóm sẽ thu thập các con số thực tế để điền vào bảng so sánh sau:

| Tiêu chí đánh giá | HƯỚNG THUẦN (CPU)<br>*(Pandas + Scikit-learn)* | HƯỚNG THUẦN (GPU)<br>*(RAPIDS / XGBoost GPU)* | HƯỚNG SPARK (Phân tán)<br>*(PySpark + Spark MLlib)* |
| :--- | :---: | :---: | :---: |
| **Môi trường thực thi** | Local / Kaggle (CPU) | Kaggle (2x Tesla T4) | Local (Spark Local) / Kaggle |
| **Thời gian nạp dữ liệu (Data Ingestion)** | *[Đo thực tế (giây)]* | *[Đo thực tế (giây)]* | *[Đo thực tế (giây)]* |
| **Mức tiêu thụ RAM đỉnh điểm (Peak RAM)** | Rất cao (~5 - 6 GB) | Thấp trên RAM (chuyển sang VRAM) | Được kiểm soát theo Partition |
| **Thời gian Feature Engineering** | *[Đo thực tế (giây)]* | *[Đo thực tế (giây)]* | *[Đo thực tế (giây)]* |
| **Thời gian huấn luyện Random Forest** | *[Rất lâu hoặc OOM]* | *[Nhanh]* | *[Tối ưu hóa phân tán]* |
| **Độ chính xác (Accuracy / F1-Score)** | *[Đo thực tế (%)]* | *[Đo thực tế (%)]* | *[Đo thực tế (%)]* |
| **Khả năng mở rộng (Scalability)** | Kém khi dữ liệu tăng | Bị giới hạn bởi VRAM GPU | **Tốt nhất (Thêm Worker là mở rộng)** |

---

## 5. LỘ TRÌNH THỰC HIỆN CỤ THỂ CHO DỰ ÁN

```mermaid
graph TD
    A["Tập dữ liệu gốc 2024 (7.07M dòng)"] --> B["BƯỚC 1: Xây dựng Module Tiền xử lý chung (ETL)"]
    
    B --> C["KỊCH BẢN 1: HƯỚNG THUẦN"]
    C --> C1["Baseline: Scikit-learn Random Forest (CPU)"]
    C --> C2["Mở rộng: XGBoost / LightGBM GPU"]
    
    B --> D["KỊCH BẢN 2: HƯỚNG SPARK (TRỌNG TÂM ĐỒ ÁN)"]
    D --> D1["Tối ưu hóa: Chuyển đổi CSV sang Parquet"]
    D --> D2["Spark ML Pipeline: StringIndexer -> VectorAssembler"]
    D --> D3["Huấn luyện: Spark MLlib RandomForestClassifier"]
    
    C1 --> E["BƯỚC 3: TỔNG HỢP KẾT QUẢ SO SÁNH & NGHIỆM THU"]
    C2 --> E
    D3 --> E
    
    E --> F1["Báo cáo Word Nhom6_T4_C10-12_BaoCao.docx (Chương 4 & 5)"]
    E --> F2["MongoDB -> FastAPI -> Dashboard React (Chương 6)"]
```

---

## 6. PHẢN BIỆN & CHIẾN LƯỢC CỘNG TÁC CHO NHÓM 3 NGƯỜI (KAGGLE VS LOCAL)

### 6.1. Phản biện thẳng thắn: "Có nên làm việc hoàn toàn trên Kaggle không?"
**Câu trả lời dứt khoát: KHÔNG NÊN LÀM 100% TRÊN KAGGLE, VÀ CŨNG KHÔNG NÊN LÀM 100% TRÊN LOCAL.**

Lý do phản biện:
1. **Bẫy lớn nhất của Kaggle đối với đồ án môn "Nhập môn Big Data":**
   * Đồ án Big Data chấm điểm cao nhất ở **Kiến trúc hệ thống phân tán** (Cụm Hadoop NameNode, DataNode, Spark Master, Spark Worker, MongoDB, Dashboard tương tác).
   * **Kaggle là một Sandbox Container đơn lẻ:** Bạn **không thể** cài đặt Docker Desktop, không thể dựng cụm HDFS đa node, không thể mở port ra Internet để chạy Web Dashboard React hay FastAPI cho thầy và nhóm xem. Nếu chỉ nộp 1 file Jupyter Notebook trên Kaggle, đồ án sẽ bị coi là đồ án môn *Machine Learning / Data Mining thông thường*, đánh mất hoàn toàn yếu tố *Hệ thống Dữ liệu lớn (Big Data Systems)*.
2. **Bất cập khi 3 người cùng làm việc trên Kaggle Notebook:**
   * Kaggle **không có tính năng đồng chỉnh sửa thời gian thực (Real-time collaborative editing)**. Nếu 2 hoặc 3 bạn cùng mở 1 notebook để code, người bấm "Save" sau sẽ **ghi đè và xóa sạch code** của người bấm trước (Version Conflict).
   * File `.ipynb` trên Kaggle dài hàng nghìn dòng rất khó quản lý chất lượng code, không thể chia module theo chuẩn công nghệ phần mềm (`src/etl/`, `src/models/`, `backend/`, `frontend/`).
   * Giới hạn 30 giờ GPU/tuần cho mỗi account: Nếu dùng chung 1 account sẽ hết quota rất nhanh.

3. **Bất cập nếu 3 người chỉ làm việc trên máy tính cá nhân (Local):**
   * RAM máy Lương khả dụng chỉ còn ~4.2 GB. Máy 2 bạn An và Tuấn Anh nếu cấu hình yếu hơn thì sẽ bị văng (crash OOM) ngay lập tức khi cố đọc 7 triệu dòng.

---

### 6.2. Giải pháp Tối ưu: Mô hình Lai "Git Quản lý - Kaggle Huấn luyện - Local Demo"

Nhóm 3 người nên kết hợp sức mạnh của cả hai nền tảng như sau:

| Trụ cột | Nền tảng thực thi | Cách thức phối hợp 3 người |
| :--- | :--- | :--- |
| **Quản lý mã nguồn** | **GitHub Repository** | Tạo repo Git cho đồ án. Mỗi bạn clone về máy local, code theo từng nhánh (branch) và từng module riêng biệt, không bao giờ bị đè code lên nhau. |
| **Huấn luyện nặng (Train 7M dòng)** | **Kaggle Notebook** | **Chia nhỏ bài toán:** Cả 3 bạn đều dùng account Kaggle riêng ($3 \times 30h = 90h$ GPU/tuần miễn phí):<br>• Bạn An chạy: Hướng Thuần (Scikit-learn vs LightGBM).<br>• Bạn Lương chạy: Hướng Spark (PySpark ETL & Parquet & Random Forest).<br>• Bạn Tuấn Anh chạy: Hướng Cải tiến (CatBoost & XGBoost).<br>$\to$ Sau khi train xong, mỗi người xuất file kết quả (`metrics.json`, biểu đồ, model) đưa về Git. |
| **Trình diễn hệ thống & Báo cáo** | **Máy Local (Docker + React)** | Máy của Lương (Ryzen 7, RTX 5050) cấu hình rất mạnh, dùng để dựng cụm Docker (Hadoop/Spark/MongoDB), chạy Backend FastAPI và bật React Dashboard để quay video demo và chụp ảnh kiến trúc nộp cho thầy. |

---

### 6.3. Bảng Phân công Trách nhiệm Nhóm 6 (RACI Matrix)

| Thành viên | Trách nhiệm chính (Lead) | Nhiệm vụ cụ thể | Môi trường làm việc chính |
| :--- | :--- | :--- | :--- |
| **Lê Đức Lương** | **Kiến trúc Big Data & Hướng Spark** | • Xây dựng cụm Docker (Hadoop HDFS + Spark + MongoDB).<br>• Pipeline PySpark ETL, nén Parquet phân vùng.<br>• Huấn luyện Spark MLlib `RandomForestClassifier`. | Local (Docker & Spark) + Kaggle Spark |
| **Cù Văn Vĩ An** | **Hướng Thuần & Mô hình Cải tiến** | • Baseline Scikit-learn Random Forest (CPU).<br>• Huấn luyện LightGBM và CatBoost trên Kaggle GPU.<br>• Thu thập số liệu đối chứng thời gian & RAM. | Kaggle Notebooks (GPU) |
| **Trần Huỳnh Tuấn Anh** | **Mô hình XGBoost, Serving & Web Dashboard** | • Huấn luyện XGBoost GPU trên Kaggle.<br>• Xây dựng Backend FastAPI đọc kết quả từ MongoDB.<br>• Thiết kế Dashboard React trực quan hóa biểu đồ. | Kaggle (ML) + Local (FastAPI & React) |

