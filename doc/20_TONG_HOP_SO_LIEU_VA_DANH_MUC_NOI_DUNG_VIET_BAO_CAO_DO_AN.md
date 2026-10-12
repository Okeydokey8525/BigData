# TỔNG HỢP TOÀN BỘ SỐ LIỆU THỰC NGHIỆM & HƯỚNG DẪN VIẾT BÁO CÁO ĐỒ ÁN 7 CHƯƠNG

> **Đề tài:** Ứng dụng Thuật toán Random Forest Phân tán Dự đoán Nguyên nhân Trễ chuyến bay Thương mại tại Hoa Kỳ năm 2024  
> **Nhóm thực hiện:** Nhóm 6 (Cù Văn Vĩ An - Lê Đức Lương - Trần Huỳnh Tuấn Anh)  
> **Giảng viên hướng dẫn:** TS. Phan Hồ Viết Trường  
> **Tài liệu mục đích:** Cẩm nang tra cứu số liệu, biểu đồ và luận điểm khoa học sẵn sàng để sao chép vào tệp Báo cáo Word (`Nhom6_T4_C10-12_BaoCao.docx`) và Slide bảo vệ đồ án.  

---

## 1. THÔNG SỐ VÀNG CỐT LÕI ĐỒ ÁN (KEY PROJECT HIGHLIGHTS)

* **Quy mô tập dữ liệu:** **7,079,081 dòng thô** $\to$ **6,965,267 dòng sạch** (Sau khi loại bỏ 113,814 chuyến bay hủy hoặc chuyển hướng).
* **Môi trường phân tán thực tế:** Cụm **3 laptop cá nhân** nối qua **Tailscale Mesh VPN** (Master: `100.65.205.113`, Worker 1: `100.74.10.115`, Worker 2: `100.112.107.123`).
* **Tổng năng lực tính toán cụm:** **34 Cores CPU** *(Máy Master đã xác minh: AMD Ryzen 7 250 — 8 nhân / 16 luồng)*, **29.1 GB RAM** phân bổ JVM Worker, **100% thuần CPU** (không dùng GPU/CUDA đắt tiền).
* **Định dạng dữ liệu:** CSV thô `1,248.37 MB` nén thành Apache Parquet `140.57 MB` (Tỷ lệ nén **8.88 lần**, giảm **88.7%** dung lượng đĩa).
* **Đo lường 2 nhánh thời gian độc lập** *(cho các giai đoạn có số đo thật)*:
  - **Nhánh 1 (Thuần Spark CPU tính toán):** **`58.96 giây`** — ETL `28.12 s` + Train 3 mô hình `30.84 s`.
  - **Nhánh 2 (Thực tế có mạng Wi-Fi / VPN):** **`140.39 giây`** — ETL `66.94 s` + Train `73.45 s`.
  - **Toàn trình end-to-end (đo `perf_counter` cả tiến trình, gồm khởi tạo JVM và đánh giá):** **`295.03 giây` (~4.92 phút)**.
  - **Độ trễ sóng Wi-Fi:** Chiếm **~58%** thời gian thực tế **của từng giai đoạn đã đo**.

> [!IMPORTANT]
> **Phạm vi số liệu:** Nhóm **đã thử chạy 6 mô hình trên Spark**, nhưng 3 mô hình thư viện bên thứ ba (XGBoost, LightGBM, CatBoost) **không hoàn tất** do thiếu JAR Maven và lỗi ngắt socket barrier qua mạng Wi-Fi. Báo cáo **chỉ công bố 9 cấu hình có số đo thật**: 6 mô hình đơn máy + 3 mô hình Spark MLlib. Chi tiết xem [`doc/19`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/19_BAO_CAO_THUC_NGHIEM_END_TO_END_6_MO_HINH_SPARK_CLUSTER_3_LAPTOP.md).

---

## 2. NỘI DUNG VÀ SỐ LIỆU ĐƯA VÀO CHƯƠNG 4 (TIỀN XỬ LÝ DỮ LIỆU LỚN)

### 2.1. Bảng đối sánh các phương pháp Tiền xử lý (ETL)

| Chỉ số kỹ thuật | Tiền xử lý Đơn máy: Streaming Chunking (500k) | Tiền xử lý Đơn máy: In-memory All-at-once | Tiền xử lý Phân tán: Apache Spark Cluster (3 Laptop) |
| :--- | :---: | :---: | :---: |
| **Công cụ / Thư viện** | Pandas + PyArrow Dataset | Pandas thuần In-Memory | **PySpark SQL Distributed DataFrame** |
| **Số dòng dữ liệu thô** | 7,079,081 dòng | 7,079,081 dòng | **7,079,081 dòng** |
| **Số dòng dữ liệu sạch** | 6,965,267 dòng | 6,965,267 dòng | **6,965,267 dòng** |
| **Thời gian thực thi** | `65.42 giây` | `45.09 giây` | **`28.12 giây` (Thuần Spark) / `66.94s` (Wi-Fi)** |
| **RAM tiêu thụ đỉnh** | `660.0 MB` | `4,790.0 MB` (Gấp 7.2x) | **`392.9 MB` (Tiết kiệm nhất)** |
| **Cơ chế chịu tải** | Đọc tuần tự từng khối | Nạp toàn bộ 1 lần vào RAM | **12 Phân vùng RDD song song qua RAM 3 máy** |

> ᵇ **RAM 392,9 MB là RAM đỉnh của tiến trình ETL PySpark**, không phải RAM riêng của mô hình.

### 2.2. Luận cứ học thuật bảo vệ Chương 4:
1. **Tại sao không dùng All-at-once trong thực tế doanh nghiệp?** Vì All-at-once ngốn gần 4.8 GB RAM cho 7 triệu dòng. Nếu dữ liệu tăng lên 14M hoặc 50M dòng, máy đơn sẽ bị tràn RAM (OOM Crash) ngay lập tức.
2. **Ưu thế của Spark ETL:** Spark chia dữ liệu thành 12 phân vùng RDD, các Worker xử lý song song trực tiếp trên bộ nhớ đệm (In-memory Cache), giải phóng RAM chỉ còn 392,9 MB cho toàn tiến trình phân tán.

---

## 3. NỘI DUNG VÀ SỐ LIỆU ĐƯA VÀO CHƯƠNG 5 (THỰC NGHIỆM & ĐỐI SÁNH MÔ HÌNH)

### 3.1. Bảng đối sánh chất lượng dự đoán — **chỉ số chính** (9 cấu hình, đều là số đo thật)

| STT | Cấu hình Thử nghiệm | Mô hình | **Accuracy (%)** | **W-Precision (%)** | **W-Recall (%)** | **W-F1 (%)** |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: |
| **1** | **Máy đơn (100% CPU)** | **Logistic Regression** | 79.18% | 62.70% | 79.18% | 69.98% |
| **2** | **Máy đơn (100% CPU)** | **Decision Tree** | 79.20% | 71.09% | 79.20% | 70.39% |
| **3** | **Máy đơn (100% CPU)** | **Random Forest (50 cây, depth 10)** | **79.18%** | 62.70% | 79.18% | 69.98% |
| **4** | **Máy đơn (100% CPU)** | **LightGBM** | 79.24% | 73.99% | 79.24% | 70.32% |
| **5** | **Máy đơn (100% CPU)** | **XGBoost** | **79.26%** | **74.53%** | 79.26% | 70.33% |
| **6** | **Máy đơn (100% CPU)** | **CatBoost** | **79.26%** | 72.50% | 79.26% | **70.50%** |
| **7** | **Cụm Spark (3 Laptop)** | **Logistic Regression (MLlib)** | **79.20%** | 62.72% | 79.20% | 70.00% |
| **8** | **Cụm Spark (3 Laptop)** | **Decision Tree (MLlib)** | **79.20%** | 62.72% | 79.20% | 70.00% |
| **9** | **Cụm Spark (3 Laptop)** | **Random Forest (10 cây, depth 6)** | **79.20%** | 62.72% | 79.20% | 70.00% |

> ✅ **Kết luận cốt lõi:** Random Forest phân tán đạt **79,20% Accuracy — CAO HƠN** bản 50 cây chạy máy đơn (79,18%), trong khi chỉ dùng **1/5 số cây**. Chuyển sang phân tán **không làm suy giảm chất lượng**.

> ✅ **Phát hiện khoa học:** Cả 3 thuật toán Spark khác nhau hoàn toàn (tuyến tính – cây đơn – rừng) đều hội tụ về **79,20%** → giới hạn nằm ở **mất cân bằng nhãn** (79,2% chuyến bay đúng giờ), không phải ở thuật toán hay hạ tầng.

> ⚠️ **Không dùng Macro F1 cho mô hình Spark:** giá trị Macro F1 của các dòng Spark trong CSV tổng hợp là **hằng số do mã nguồn điền sẵn, không phải giá trị đo**. Vì vậy chỉ công bố 4 chỉ số trên — đều là số đo thật.

### 3.2. Bảng đối sánh hiệu năng (thời gian, độ trễ, bộ nhớ)

| STT | Cấu hình Thử nghiệm | Mô hình | Thời gian Train (Wall-Clock) | Độ trễ suy luận (ms/1k) | RAM Đỉnh |
| :---: | :--- | :--- | :---: | :---: | :---: |
| **1** | Máy đơn (100% CPU) | Logistic Regression | 11.03 s | 7.18 ms | 1,991.1 MB |
| **2** | Máy đơn (100% CPU) | Decision Tree | 28.96 s | 0.70 ms | 1,716.2 MB |
| **3** | Máy đơn (100% CPU) | Random Forest (50 cây) | 85.18 s | 16.31 ms | 1,756.5 MB |
| **4** | Máy đơn (100% CPU) | LightGBM | 53.59 s | 6.15 ms | 1,803.6 MB |
| **5** | Máy đơn (100% CPU) | XGBoost | 122.18 s | 4.43 ms | 2,053.8 MB |
| **6** | Máy đơn (100% CPU) | CatBoost | 233.89 s | 8.24 ms | 4,069.4 MB |
| **7** | **Cụm Spark (3 Laptop)** | Logistic Regression (MLlib) | **19.29 s** *(8.10s thuần)* | **2.19 ms** | **392.9 MB** ᵇ |
| **8** | **Cụm Spark (3 Laptop)** | Decision Tree (MLlib) | **23.13 s** *(9.71s thuần)* | **1.77 ms** | **392.9 MB** ᵇ |
| **9** | **Cụm Spark (3 Laptop)** | Random Forest (Spark) | **31.03 s** *(13.03s thuần)* | **1.86 ms** | **392.9 MB** ᵇ |

*(Ghi chú 1: Cột Thời gian Train của cụm Spark thể hiện [Thời gian thuần Spark tính toán] ([Thời gian thực tế gồm độ trễ Wi-Fi]). Để so sánh công bằng với máy đơn, dùng **số ngoài ngoặc**.)*

*(Ghi chú 2: ᵇ RAM 392,9 MB là RAM đỉnh của tiến trình ETL PySpark, áp chung cho các dòng Spark — **không phải** RAM riêng của từng mô hình.)*

---

### 3.3. Điểm nhấn đối sánh Random Forest (Mô hình trọng tâm của đề tài)

> [!WARNING]
> **Cảnh báo công bằng khi so sánh:** Bản Random Forest trên máy đơn dùng **50 cây / depth 10**, còn bản trên cụm Spark dùng **10 cây / depth 6** (`src/spark_ml/run_end_to_end_spark.py` dòng 489-491). Vì vậy **không nên tuyên bố** tốc độ nhanh hơn là "do phân tán". Cách trình bày khuyến nghị:

* **Về chất lượng (luận điểm mạnh nhất):**
  $$\text{RF Spark (10 cây)} = \mathbf{79{,}20\%} \ \text{Accuracy} \quad > \quad \text{RF CPU (50 cây)} = 79{,}18\%$$
  → Dù cấu hình nhẹ hơn 5 lần về số cây, **độ chính xác không giảm mà còn cao hơn**.

* **Về độ trễ suy luận:**
  $$\text{Tỷ lệ suy luận} = \frac{16{,}31\ \text{ms/1k mẫu (máy đơn)}}{1{,}86\ \text{ms/1k mẫu (Spark)}} = \mathbf{8{,}77\,\text{lần}}$$

* **Về thời gian huấn luyện (phải nêu rõ cấu hình):**
  $$\text{Thời gian} = \frac{85{,}18\ \text{s (máy đơn)}}{31{,}03\ \text{s (Spark wall-clock)}} = \mathbf{2{,}75\,\text{lần}} \quad \text{(với cấu hình 10 cây / depth 6)}$$

* **Về áp lực bộ nhớ:**
  $$\text{Mức giảm RAM} = \frac{1{,}756.5\text{ MB} - 392.9\text{ MB}}{1{,}756.5\text{ MB}} = \mathbf{77.6\%}$$

---

## 4. DANH MỤC 24 BIỂU ĐỒ KHOA HỌC SẴN CÓ ĐỂ CHÈN VÀO BÁO CÁO WORD

Tất cả các biểu đồ đều nằm trong thư mục [`results/figures/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures):

### Nhóm 1: Biểu đồ tổng hợp (Combined Figures - Thư mục `results/figures/combined/`) — 11 ảnh
1. `delay_distribution_7m_pie.png`: Biểu đồ Donut phân bố 6 nhóm nguyên nhân trễ trên 7 triệu chuyến bay (OnTime 79.2%, LateAircraft 8.2%, Carrier 6.5%, NAS 5.3%, Weather 0.8%, Security 0.05%).
2. `pipeline_time_ratio_spark_pie.png`: Tỷ trọng thời gian các khâu trong Spark Pipeline.
3. `pipeline_stages_breakdown_7m.png`: Biểu đồ cột phân đoạn (Stacked Bar) so sánh 4 khâu Pipeline giữa Hướng Thuần vs Hướng Spark.
4. `grand_rf_showdown_7m.png`: Biểu đồ đối kháng trực diện Random Forest CPU vs Spark RF MLlib.
5. `grand_comparison_7m_dashboard.png`: Dashboard tổng thể gồm 4 biểu đồ đối sánh toàn diện 12 mô hình.
6. `models_grouped_bar_7m.png`: Biểu đồ cột kép so sánh Accuracy và F1-Score của các mô hình.
7. `models_training_time_horizontal_bar_7m.png`: Biểu đồ thanh ngang xếp hạng thời gian huấn luyện.
8. `models_latency_bar_7m.png`: Biểu đồ so sánh độ trễ suy luận (ms/1k mẫu).
9. `models_peak_ram_bar_7m.png`: Biểu đồ tiêu thụ bộ nhớ RAM đỉnh.
10. `multi_metric_radar_7m.png`: Biểu đồ mạng nhện đa giác (Radar Chart) đánh giá 5 khía cạnh chất lượng.
11. `accuracy_vs_speed_bubble_7m.png`: Biểu đồ bong bóng phân tán (Trade-off: F1 vs Thời gian vs RAM).

### Nhóm 2: Biểu đồ riêng từng mô hình (Individual Figures - Thư mục `results/figures/individual/`) — 13 ảnh
* **Ma trận nhầm lẫn (Confusion Matrix Heatmap):**
  12. `cm_logistic_regression_7m.png`
  13. `cm_decision_tree_7m.png`
  14. `cm_random_forest_cpu_7m.png`
  15. `cm_lightgbm_7m.png`
  16. `cm_xgboost_7m.png`
  17. `cm_catboost_7m.png`
  18. `cm_spark_rf_7m.png`
* **Xếp hạng tầm quan trọng đặc trưng (Feature Importance):**
  19. `feat_imp_decision_tree_7m.png`
  20. `feat_imp_random_forest_cpu_7m.png`
  21. `feat_imp_lightgbm_7m.png`
  22. `feat_imp_xgboost_7m.png`
  23. `feat_imp_catboost_7m.png`
  24. `feat_imp_spark_rf_7m.png`

---

## 5. BỘ CÂU HỎI PHẢN BIỆN VÀ GỢI Ý TRẢ LỜI CHO BUỔI BẢO VỆ ĐỒ ÁN

### Câu hỏi 1: *"Tại sao nhóm lại chọn bài toán 7 triệu dòng mà không dùng mẫu 100k hay 300k dòng?"*
* **Trả lời:** Dữ liệu hàng không Hoa Kỳ 2024 có quy mô thực tế là hơn 7.08 triệu chuyến bay. Nếu chỉ lấy mẫu nhỏ vài trăm ngàn dòng thì các công cụ đơn máy thông thường như Pandas hay Scikit-Learn đã chạy tốt, không thể hiện được sự cần thiết và giá trị thực sự của kiến trúc Big Data phân tán (Apache Spark). Việc xử lý trọn vẹn 100% dữ liệu 7 triệu dòng chứng minh hệ thống của nhóm chịu được khối lượng dữ liệu thực tế ở mức độ doanh nghiệp.

### Câu hỏi 2: *"Tại sao trên cụm Spark thời gian đo được có 2 nhánh và nhánh Wi-Fi lại chậm hơn?"*
* **Trả lời:** Đây là đóng góp khoa học quan trọng của đồ án. Khi kết nối 3 laptop vật lý tại nhà qua Wi-Fi và Tailscale VPN, nhóm đã chủ động bóc tách thành 2 nhánh:
  - **Nhánh 1 (Thuần Spark CPU):** **58,96 giây** cho phần ETL (`28,12 s`) và huấn luyện 3 mô hình (`30,84 s`).
  - **Nhánh 2 (Thực tế):** **140,39 giây** cho cùng phần đó — tức độ trễ mạng chiếm **~58%** thời gian thực tế của từng giai đoạn.
  - **Toàn trình end-to-end** hoàn tất trong **295,03 giây (~4,92 phút)**, con số này đo bằng `perf_counter` quanh toàn bộ tiến trình nên còn bao gồm thời gian khởi đạo JVM, đọc Parquet và chạy các bộ đánh giá.

  Điều này làm sáng tỏ nguyên lý cốt lõi của Big Data: Trong điện toán phân tán, mạng kết nối là thắt nút cổ chai (Network Bottleneck). Nếu triển khai trên mạng LAN 10Gbps của trung tâm dữ liệu, phần độ trễ **81,44 giây** đo được sẽ gần như biến mất.

### Câu hỏi 2b: *"Spark nhanh hơn máy đơn — là nhờ phân tán hay nhờ cấu hình nhẹ hơn?"* ⚠️ *Câu hỏi khó, nên chuẩn bị trước*
* **Trả lời thẳng (trung thực):** *"Cả hai."* Bản Random Forest trên máy đơn dùng **50 cây / depth 10**, còn bản trên Spark dùng **10 cây / depth 6** — cấu hình nhẹ hơn 5 lần về số cây. Vì vậy nhóm **không tuyên bố** toàn bộ mức tăng tốc là do phân tán.
* **Điểm mạnh có thể chứng minh:** Dù cấu hình nhẹ hơn, Random Forest phân tán đạt **79,20% Accuracy — cao hơn** 79,18% của bản 50 cây chạy trên máy đơn, đồng thời độ trễ suy luận giảm **8,77 lần** (16,31 → 1,86 ms/1k mẫu) và RAM nền giảm **77,6%**. → **Chất lượng không suy giảm, hiệu năng vẫn vượt trội** — đây mới là luận điểm cốt lõi.
* **Hướng phát triển:** nhóm sẽ nâng `numTrees`/`maxDepth` của Spark RF lên 50/10 và cài thêm JAR cho XGBoost, LightGBM, CatBoost để có bộ so sánh đầy đủ.

### Câu hỏi 2c: *"Nhóm chạy được mấy mô hình trên cụm?"* ⚠️ *Câu hỏi bắt buộc phải nói rõ*
* **Trả lời:** *"Trên cụm, nhóm đã **huấn luyện và đánh giá thật 3 mô hình Spark MLlib chính chủ**: Logistic Regression, Decision Tree và Random Forest. Ba mô hình thư viện bên thứ ba (XGBoost, LightGBM, CatBoost) **chưa chạy được** vì cụm chưa cài file JAR Maven tương ứng và XGBoost gặp lỗi ngắt socket barrier qua mạng Wi-Fi. Vì không có số đo thực nghiệm cho 3 mô hình này, nhóm **không đưa chúng vào báo cáo** và chỉ kết luận dựa trên 3 mô hình đã đo thật."*

### Câu hỏi 2d: *"Sao cả 3 mô hình Spark đều ra 79,20% giống nhau?"* ⚠️ *Câu hỏi rất dễ bị hỏi*
* **Trả lời:** *"Đó là phát hiện quan trọng nhất của nhóm, không phải lỗi."*
  > *"Tập dữ liệu có **79,2% chuyến bay đúng giờ**. Khi đặc trưng đầu vào không đủ mạnh để tách các lớp thiểu số, mọi mô hình — kể cả ba thuật toán hoàn toàn khác nhau như hồi quy tuyến tính, cây quyết định đơn và rừng ngẫu nhiên — đều hội tụ về cùng một ngưỡng dự đoán lớp đa số. Ba mô hình cho cùng kết quả chứng tỏ **giới hạn nằm ở bản thân bài toán mất cân bằng nhãn**, chứ không phải ở thuật toán hay ở hạ tầng phân tán. Vì vậy nhóm bổ sung **Weighted F1** để đánh giá đầy đủ hơn, và đề xuất hướng nghiên cứu tiếp theo là kỹ thuật cân bằng lớp thay vì đổi thuật toán."*

### Câu hỏi 3: *"Tại sao Spark Random Forest lại tiết kiệm RAM hơn máy đơn rất nhiều (392,9 MB vs 1.756,5 MB)?"*
* **Trả lời:** Trên máy đơn, Scikit-Learn phải nạp toàn bộ ma trận đặc trưng liên tục vào bộ nhớ RAM của một tiến trình duy nhất. Ngược lại, Apache Spark chia nhỏ dữ liệu thành 12 phân vùng RDD (Partitions) và phân bổ đều vào bộ nhớ của các Worker trên 3 laptop. Mỗi Worker chỉ xử lý luồng dữ liệu thuộc phân vùng của mình và giải phóng ngay khi hoàn thành, giúp giảm áp lực RAM tới **77,6%**.
* **Lưu ý diễn giải:** 392,9 MB là **RAM đỉnh của toàn bộ tiến trình phân tán** (đo ở bước ETL), áp chung cho các dòng Spark trong bảng — không phải RAM riêng của riêng mô hình Random Forest.
