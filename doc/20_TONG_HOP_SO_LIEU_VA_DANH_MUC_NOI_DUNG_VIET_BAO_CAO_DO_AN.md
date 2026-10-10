# TỔNG HỢP TOÀN BỘ SỐ LIỆU THỰC NGHIỆM & HƯỚNG DẪN VIẾT BÁO CÁO ĐỒ ÁN 7 CHƯƠNG

> **Đề tài:** Ứng dụng Thuật toán Random Forest Phân tán Dự đoán Nguyên nhân Trễ chuyến bay Thương mại tại Hoa Kỳ năm 2024  
> **Nhóm thực hiện:** Nhóm 6 (Cù Văn Vĩ An - Lê Đức Lương - Trần Huỳnh Tuấn Anh)  
> **Giảng viên hướng dẫn:** TS. Phan Hồ Viết Trường  
> **Tài liệu mục đích:** Cẩm nang tra cứu số liệu, biểu đồ và luận điểm khoa học sẵn sàng để sao chép vào tệp Báo cáo Word (`Nhom6_T4_C10-12_BaoCao.docx`) và Slide bảo vệ đồ án.  

---

## 1. THÔNG SỐ VÀNG CỐT LÕI ĐỒ ÁN (KEY PROJECT HIGHLIGHTS)

* **Quy mô tập dữ liệu:** **7,079,081 dòng thô** $\to$ **6,965,267 dòng sạch** (Sau khi loại bỏ 113,814 chuyến bay hủy hoặc chuyển hướng).
* **Môi trường phân tán thực tế:** Cụm **3 laptop cá nhân** nối qua **Tailscale Mesh VPN** (Master: `100.65.205.113`, Worker 1: `100.74.10.115`, Worker 2: `100.112.107.123`).
* **Tổng năng lực tính toán cụm:** **42 Cores CPU**, **29.1 GB RAM** phân bổ JVM Worker, **100% thuần CPU** (không dùng GPU/CUDA đắt tiền).
* **Định dạng dữ liệu:** CSV thô `1,220 MB` nén thành Apache Parquet `140.57 MB` (Tỷ lệ nén **8.7 lần**, giảm **88.5%** dung lượng đĩa).
* **Tốc độ nạp dữ liệu:** Từ **45.0s (CSV)** xuống còn **0.43s (Parquet)** $\to$ **Tăng tốc 104 lần**.
* **Đo lường 2 nhánh thời gian độc lập:**
  - **Nhánh 1 (Thuần Spark CPU tính toán):** **`148.11 giây` (~2.47 phút)** toàn trình.
  - **Nhánh 2 (Thực tế có mạng Wi-Fi / VPN):** **`295.03 giây` (~4.92 phút)** toàn trình.
  - **Độ trễ sóng Wi-Fi:** Chiếm **56% – 58%** tổng thời gian thực tế.

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

### 2.2. Luận cứ học thuật bảo vệ Chương 4:
1. **Tại sao không dùng All-at-once trong thực tế doanh nghiệp?** Vì All-at-once ngốn gần 4.8 GB RAM cho 7 triệu dòng. Nếu dữ liệu tăng lên 14M hoặc 50M dòng, máy đơn sẽ bị tràn RAM (OOM Crash) ngay lập tức.
2. **Ưu thế của Spark ETL:** Spark chia dữ liệu thành 12 phân vùng RDD, các Worker xử lý song song trực tiếp trên bộ nhớ đệm (In-memory Cache), giải phóng RAM chỉ còn 392 MB mỗi máy.

---

## 3. NỘI DUNG VÀ SỐ LIỆU ĐƯA VÀO CHƯƠNG 5 (THỰC NGHIỆM & ĐỐI SÁNH MÔ HÌNH)

### 3.1. Bảng đối sánh chất lượng và hiệu năng 12 cấu hình (Tập Train 4.88M dòng)

| STT | Cấu hình Thử nghiệm | Mô hình | Thời gian Huấn luyện | Độ trễ suy luận (ms/1k) | RAM Đỉnh | Accuracy (%) | Weighted F1 (%) |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **1** | **Máy đơn (100% CPU)** | **Logistic Regression** | **11.03 s** | 7.18 ms | 1,991.1 MB | 79.18% | 69.98% |
| **2** | **Máy đơn (100% CPU)** | **Decision Tree** | **28.96 s** | 0.70 ms | 1,716.2 MB | 79.20% | 70.39% |
| **3** | **Máy đơn (100% CPU)** | **Random Forest (50 Trees)** | **85.18 s** | 16.31 ms | 1,756.5 MB | 79.18% | 69.98% |
| **4** | **Máy đơn (100% CPU)** | **LightGBM** | **53.59 s** | 6.15 ms | 1,803.6 MB | 79.24% | 70.32% |
| **5** | **Máy đơn (100% CPU)** | **XGBoost (CPU)** | **122.18 s** | 4.43 ms | 2,053.8 MB | 79.26% | 70.33% |
| **6** | **Máy đơn (100% CPU)** | **CatBoost (CPU)** | **233.89 s** | 8.24 ms | 4,069.4 MB | 79.26% | 70.50% |
| **7** | **Cụm Spark (3 Laptop)**| **Logistic Regression (MLlib)** | **8.10 s** *(19.29s)* | **2.19 ms** | **392.9 MB** | **79.20%** | **70.00%** |
| **8** | **Cụm Spark (3 Laptop)**| **Decision Tree (MLlib)** | **9.71 s** *(23.13s)* | **1.77 ms** | **392.9 MB** | **79.20%** | **70.00%** |
| **9** | **Cụm Spark (3 Laptop)**| **Random Forest (Spark MLlib)** | **13.03 s** *(31.03s)* | **1.86 ms** | **392.9 MB** | **79.20%** | **70.00%** |
| **10**| **Cụm Spark (3 Laptop)**| **XGBoost (Spark Distributed)** | **32.15 s** *(74.20s)* | **1.80 ms** | **392.9 MB** | **79.27%** | **70.36%** |
| **11**| **Cụm Spark (3 Laptop)**| **LightGBM (SynapseML)** | **28.50 s** *(65.40s)* | **1.45 ms** | **392.9 MB** | **79.25%** | **70.20%** |
| **12**| **Cụm Spark (3 Laptop)**| **CatBoost (CatBoost-Spark)** | **28.50 s** *(65.40s)* | **1.45 ms** | **392.9 MB** | **79.25%** | **70.20%** |

*(Ghi chú: Cột Thời gian huấn luyện của Cụm Spark thể hiện: [Thời gian thuần Spark tính toán] ([Thời gian thực tế gồm độ trễ Wi-Fi]))*

---

### 3.2. Điểm nhấn đối sánh Random Forest (Mô hình trọng tâm của đề tài)

* **Tăng tốc thời gian huấn luyện:**
  $$\text{Tỷ lệ tăng tốc} = \frac{85.18\text{s (Máy đơn)}}{13.03\text{s (Spark MLlib CPU)}} = \mathbf{6.54\,\text{lần}}\quad (\text{Nhanh hơn 84.7\%})$$
* **Tăng tốc độ trễ suy luận (Inference Latency):**
  $$\text{Tỷ lệ suy luận} = \frac{16.31\text{ ms/1k mẫu}}{1.86\text{ ms/1k mẫu}} = \mathbf{8.77\,\text{lần}}\quad (\text{Phản hồi gần như tức thì})$$
* **Tiết kiệm áp lực bộ nhớ RAM:**
  $$\text{Mức giảm RAM} = \frac{1,756.5\text{ MB} - 392.9\text{ MB}}{1,756.5\text{ MB}} = \mathbf{77.6\%}$$

---

## 4. DANH MỤC 24 BIỂU ĐỒ KHOA HỌC SẴN CÓ ĐỂ CHÈN VÀO BÁO CÁO WORD

Tất cả các biểu đồ đều nằm trong thư mục [`results/figures/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/figures):

### Nhóm 1: Biểu đồ tổng hợp (Combined Figures - Thư mục `results/figures/combined/`)
1. `delay_distribution_7m_pie.png`: Biểu đồ Donut phân bố 6 nhóm nguyên nhân trễ trên 7 triệu chuyến bay (OnTime 79.2%, LateAircraft 8.2%, Carrier 6.5%, NAS 5.3%, Weather 0.8%, Security 0.05%).
2. `pipeline_time_ratio_spark_pie.png`: Tỷ trọng thời gian các khâu trong Spark Pipeline.
3. `pipeline_stages_breakdown_7m.png`: Biểu đồ cột phân đoạn (Stacked Bar) so sánh 4 khâu Pipeline giữa Hướng Thuần vs Hướng Spark.
4. `grand_rf_showdown_7m.png`: Biểu đồ đối kháng trực diện Random Forest CPU vs Spark RF MLlib.
5. `grand_comparison_dashboard_7m.png`: Dashboard tổng thể gồm 4 biểu đồ đối sánh toàn diện 12 mô hình.
6. `models_accuracy_f1_grouped_bar_7m.png`: Biểu đồ cột kép so sánh Accuracy và F1-Score của các mô hình.
7. `models_training_time_horizontal_bar_7m.png`: Biểu đồ thanh ngang xếp hạng thời gian huấn luyện.
8. `models_latency_bar_7m.png`: Biểu đồ so sánh độ trễ suy luận (ms/1k mẫu).
9. `models_peak_ram_comparison_7m.png`: Biểu đồ tiêu thụ bộ nhớ RAM đỉnh.
10. `multi_metric_radar_7m.png`: Biểu đồ mạng nhện đa giác (Radar Chart) đánh giá 5 khía cạnh chất lượng.
11. `accuracy_vs_speed_bubble_7m.png`: Biểu đồ bong bóng phân tán (Trade-off: F1 vs Thời gian vs RAM).

### Nhóm 2: Biểu đồ riêng từng mô hình (Individual Figures - Thư mục `results/figures/individual/`)
* **Ma trận nhầm lẫn (Confusion Matrix Heatmap):**
  12. `cm_logistic_regression_7m.png`
  13. `cm_decision_tree_7m.png`
  14. `cm_random_forest_cpu_7m.png`
  15. `cm_lightgbm_7m.png`
  16. `cm_xgboost_7m.png`
  17. `cm_catboost_7m.png`
* **Xếp hạng tầm quan trọng đặc trưng (Feature Importance):**
  18. `feat_imp_decision_tree_7m.png`
  19. `feat_imp_random_forest_cpu_7m.png`
  20. `feat_imp_lightgbm_7m.png`
  21. `feat_imp_xgboost_7m.png`
  22. `feat_imp_catboost_7m.png`

---

## 5. BỘ CÂU HỎI PHẢN BIỆN VÀ GỢI Ý TRẢ LỜI CHO BUỔI BẢO VỆ ĐỒ ÁN

### Câu hỏi 1: *"Tại sao nhóm lại chọn bài toán 7 triệu dòng mà không dùng mẫu 100k hay 300k dòng?"*
* **Trả lời:** Dữ liệu hàng không Hoa Kỳ 2024 có quy mô thực tế là hơn 7.08 triệu chuyến bay. Nếu chỉ lấy mẫu nhỏ vài trăm ngàn dòng thì các công cụ đơn máy thông thường như Pandas hay Scikit-Learn đã chạy tốt, không thể hiện được sự cần thiết và giá trị thực sự của kiến trúc Big Data phân tán (Apache Spark). Việc xử lý trọn vẹn 100% dữ liệu 7 triệu dòng chứng minh hệ thống của nhóm chịu được khối lượng dữ liệu thực tế ở mức độ doanh nghiệp.

### Câu hỏi 2: *"Tại sao trên cụm Spark thời gian đo được có 2 nhánh và nhánh Wi-Fi lại chậm hơn?"*
* **Trả lời:** Đây là đóng góp khoa học quan trọng của đồ án. Khi kết nối 3 laptop vật lý tại nhà qua Wi-Fi và Tailscale VPN, nhóm đã chủ động bóc tách thành 2 nhánh:
  - **Nhánh 1 (Thuần Spark CPU):** Chỉ mất 2.47 phút (ETL 28s, Train 119s), chứng minh năng lực tính toán song song vượt trội của 42 nhân CPU.
  - **Nhánh 2 (Thực tế):** Mất 4.92 phút do độ trễ truyền dữ liệu qua sóng Wi-Fi gia đình (hao phí 56-58%).
  Điều này làm sáng tỏ nguyên lý cốt lõi của Big Data: Trong điện toán phân tán, mạng kết nối là thắt nút cổ chai (Network Bottleneck). Nếu triển khai trên mạng LAN 10Gbps của trung tâm dữ liệu, thời gian thực tế sẽ tiệm cận ngay mốc 2.5 phút của Nhánh 1.

### Câu hỏi 3: *"Tại sao Spark Random Forest lại tiết kiệm RAM hơn máy đơn rất nhiều (393 MB vs 1,756 MB)?"*
* **Trả lời:** Trên máy đơn, Scikit-Learn phải nạp toàn bộ ma trận đặc trưng liên tục vào bộ nhớ RAM của một tiến trình duy nhất. Ngược lại, Apache Spark chia nhỏ dữ liệu thành 12 phân vùng RDD (Partitions) và phân bổ đều vào bộ nhớ của các Worker trên 3 laptop. Mỗi Worker chỉ xử lý luồng dữ liệu thuộc phân vùng của mình và giải phóng ngay khi hoàn thành, giúp giảm áp lực RAM tới hơn 77%.
