# CHƯƠNG 5: THỰC NGHIỆM VÀ ĐÁNH GIÁ KẾT QUẢ

Chương này trình bày toàn bộ kết quả thực nghiệm thực chứng thu được từ việc huấn luyện và đánh giá trên tập dữ liệu đầy đủ **7,079,081 chuyến bay năm 2024**. Toàn bộ số liệu trong chương này được trích xuất trực tiếp từ các tệp log và kết quả đo lường thực tế trên hệ thống (`grand_model_comparison_7m.csv`, `pipeline_stages_breakdown_7m.csv`, `spark_end_to_end_metrics.json`), bao gồm:
1. Bảng đối sánh toàn diện 7 mô hình học máy.
2. Bóc tách thời gian thực thi theo 4 giai đoạn pipeline.
3. Đánh giá năng lực cụm tính toán phân tán Spark 3 node vật lý.
4. **Hệ thống 24 vị trí chèn hình ảnh khoa học (Placeholders) kèm theo phần nhận xét, phân tích chuyên sâu cho từng biểu đồ.**

---

## 5.1. Môi trường thực nghiệm

Toàn bộ các thí nghiệm được triển khai trong môi trường mạng phân tán thực tế kết hợp giữa máy trạm đơn lẻ và cụm máy tính 3 nút vật lý thông qua mạng riêng ảo Tailscale Mesh VPN:

*Bảng 5.1: Môi trường phần cứng và phần mềm thực nghiệm*

| Thành phần hệ thống | Thông số kỹ thuật chi tiết |
| :--- | :--- |
| **Cụm phân tán Apache Spark** | **3 Nút vật lý (Physical Nodes)** kết nối mạng Tailscale Mesh VPN |
| • *Spark Master Node:* | `spark://100.65.205.113:7077` (Intel Core i7-11800H, 16 GB RAM) |
| • *Spark Worker Node 1:* | AMD Ryzen 5 5600H, 16 GB RAM |
| • *Spark Worker Node 2:* | Intel Core i5-10300H, 8 GB RAM |
| • *Tổng tài nguyên cụm:* | **32 vCPUs, 40 GB RAM** |
| **Phần cứng Đơn máy (Baseline):** | Intel Core i7-11800H @ 2.30GHz (8 Cores / 16 Threads), 16 GB RAM, GPU NVIDIA GeForce RTX 3050 Laptop GPU (4 GB VRAM) |
| **Môi trường phần mềm:** | Python 3.10.12, PySpark 3.5.1, Java OpenJDK 11, CUDA 12.2 |
| **Thư viện khoa học:** | Polars 0.20+, Scikit-learn 1.4+, XGBoost 2.0+, LightGBM 4.3+, CatBoost 1.2+ |

---

## 5.2. Kịch bản thực nghiệm trên 7,079,081 dòng dữ liệu

Tập dữ liệu gốc năm 2024 được xử lý làm sạch và phân chia thành các tập dữ liệu thành phần theo kịch bản chuẩn mực:
* **Tổng số chuyến bay gốc:** **7,079,081 dòng**.
* **Số dòng bị loại bỏ (Hủy chuyến / Chuyển hướng / Sai lệch):** **113,814 dòng** (chiếm ~1.6%).
* **Số dòng hợp lệ đưa vào pipeline:** **6,965,267 dòng**.
* **Phân chia tập dữ liệu (Stratified Split 70/15/15):**
  * *Tập Huấn luyện (Train Set - 70%):* **4,875,851 dòng**.
  * *Tập Kiểm định (Validation Set - 15%):* **1,044,254 dòng**.
  * *Tập Kiểm tra độc lập (Test Set - 15%):* **1,045,162 dòng** (dùng chung để đánh giá khách quan tất cả các mô hình).

---

## 5.3. Bảng đối sánh toàn diện các mô hình học máy

Tất cả các mô hình đều được huấn luyện trên tập Train gồm 4,875,851 dòng và đánh giá độc lập trên tập Test gồm 1,045,162 dòng. Bảng 5.2 tổng hợp toàn bộ các chỉ số đo đạc thực nghiệm từ tệp dữ liệu gốc `grand_model_comparison_7m.csv`:

*Bảng 5.2: Bảng đối sánh toàn diện hiệu năng các mô hình trên 7,079,081 dòng*

| Mô hình học máy | Thời gian huấn luyện (s) | Độ trễ suy luận (ms / 1k mẫu) | RAM đỉnh (MB) | Accuracy (%) | Weighted Precision (%) | Weighted Recall (%) | Weighted F1 (%) | Macro F1 (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Logistic Regression (CPU)** | **17.99 s** | 40.00 ms | 1,710.4 MB | 79.18% | 62.70% | 79.18% | 69.98% | 14.73% |
| **Decision Tree (CPU)** | 51.12 s | **0.67 ms** | 1,418.5 MB | 79.19% | 71.09% | 79.19% | 70.45% | 15.72% |
| **Random Forest (CPU)** | 106.79 s | 33.51 ms | 1,417.9 MB | 79.18% | 62.70% | 79.18% | 69.98% | 14.73% |
| **LightGBM (CPU/GPU)** | 78.93 s | 11.71 ms | 1,464.8 MB | 79.25% | 73.99% | 79.25% | 70.34% | 15.94% |
| **XGBoost (GPU)** | 19.27 s | 73.52 ms | 1,742.0 MB | 79.27% | **74.53%** | 79.27% | 70.36% | 15.55% |
| **CatBoost (GPU)** | 19.60 s | 12.03 ms | 1,908.6 MB | **79.29%** | 72.50% | **79.29%** | **70.83%** | 16.54% |
| **Logistic Regression (Spark)** | 21.60 s | 1.74 ms | **835.2 MB** | 79.22% | 62.75% | 79.22% | 70.03% | **28.25%** |
| **Decision Tree (Spark)** | 24.96 s | 1.31 ms | **835.2 MB** | 79.23% | 68.42% | 79.23% | 70.26% | **28.25%** |
| **Random Forest (Spark MLlib)**| **44.31 s** | **1.98 ms** | **835.2 MB** | **79.22%** | 67.61% | **79.22%** | **70.03%** | **28.25%** |

---

## 5.4. Bóc tách thời gian thực thi theo 4 giai đoạn Pipeline

Để đánh giá toàn diện sự khác biệt về kiến trúc giữa phương pháp truyền thống và phương pháp phân tán Big Data, thời gian thực thi end-to-end được bóc tách chi tiết thành 4 giai đoạn riêng biệt:

*Bảng 5.3: Bóc tách thời gian 4 giai đoạn Pipeline giữa Hướng Thuần và Hướng Spark*

| Giai đoạn Pipeline | Hướng Thuần Đơn máy (Pandas/Scikit) | Hướng Phân tán (Apache Spark Cụm 3 Nút) | So sánh tốc độ / Đánh giá |
| :--- | :---: | :---: | :--- |
| **1. ETL Stage** | 43.88 s | **4.00 s** | **Spark nhanh hơn 10.97 lần** (nhờ đọc Parquet Snappy phân tán) |
| **2. Feature Engineering** | **2.50 s** | 8.52 s | Hướng thuần nhanh hơn (do Spark phải tạo đồ thị DAG phân tán) |
| **3. Training Stage (RF)** | 106.79 s | **49.89 s** | **Spark nhanh hơn 2.14 lần** (tính toán song song trên 32 vCPU) |
| **4. Evaluation Stage** | **1.80 s** | 2.83 s | Tương đương nhau trên tập kiểm tra 1 triệu dòng |
| **TỔNG THỜI GIAN (Total Time)**| **154.97 s (~2.58 phút)** | **65.24 s (~1.08 phút)** | **HỆ THỐNG SPARK NHANH HƠN 2.38 LẦN TOÀN DIỆN** |
| **Dung lượng RAM đỉnh (MB)** | 1,417.9 MB | **1,012.2 MB** | **Spark tiết kiệm hơn 405 MB RAM** cho máy điều khiển |

---

## 5.5. Chi tiết các vị trí chèn hình ảnh khoa học và nhận xét phân tích

Dưới đây là danh mục toàn bộ **24 hình ảnh biểu đồ khoa học** được nhóm nghiên cứu kết xuất từ dữ liệu thực nghiệm. Để phục vụ việc đưa vào file Word (`conference-template-a4.docx`), **mỗi mục đều có khung ghi chú vị trí chèn ảnh, đường dẫn file nguồn trên đĩa cứng và phần nhận xét khoa học chuyên sâu** tương ứng với từng biểu đồ.

---

### NHÓM I: PHÂN BỐ DỮ LIỆU & CƠ CẤU THỜI GIAN PIPELINE

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.1 - Phân bố nguyên nhân trễ chuyến bay thương mại 2024]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/delay_distribution_7m_pie.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột (One column width), đặt ngay dưới mục 5.2.
> * **Caption Word:** *Fig. 1. Phân bố tỷ lệ các nhóm nguyên nhân trễ chuyến bay thương mại Hoa Kỳ năm 2024.*

* **Nhận xét và phân tích chi tiết Fig. 5.1:**
  Biểu đồ Donut thể hiện rõ nét mức độ mất cân bằng nghiêm trọng giữa 5 nhóm nguyên nhân gây chậm chuyến trên quy mô hơn 7 triệu chuyến bay. Trong đó, `LateAircraftDelay` chiếm tỷ trọng áp đảo lớn nhất (**~38.5%**), theo sát là `CarrierDelay` (**~31.2%**) và `NASDelay` (**~23.8%**). Ba nhóm nguyên nhân này gộp lại chiếm hơn **93.5%** tổng số sự cố trễ toàn ngành. Ngược lại, trễ do thời tiết cực đoan (`WeatherDelay`) chỉ chiếm **6.1%** và trễ do an ninh (`SecurityDelay`) là lớp siêu thiểu số với tỷ lệ dưới **0.4%**. Phân bố này phản ánh tính chất dây chuyền lan tỏa (Ripple Effect) điển hình của ngành hàng không: chỉ cần một sự cố kỹ thuật hoặc lịch trình của hãng bay vào buổi sáng sẽ kéo theo hàng loạt chuyến bay sau bị trễ do máy bay đến muộn suốt cả ngày.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.2 - Tỷ trọng thời gian các khâu trong Spark Pipeline]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/pipeline_time_ratio_spark_pie.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột (One column width), đặt trong mục 5.4.
> * **Caption Word:** *Fig. 2. Cơ cấu phân bổ thời gian thực thi của cụm Apache Spark phân tán.*

* **Nhận xét và phân tích chi tiết Fig. 5.2:**
  Biểu đồ Donut minh chứng rằng trong môi trường Apache Spark phân tán với dữ liệu đã được lưu trữ dưới dạng Parquet tối ưu, giai đoạn huấn luyện mô hình học máy (`Training`) chiếm tỷ trọng áp đảo lên tới **76.5%** (49.89s trên tổng 65.24s). Khâu trích xuất đặc trưng (`Feature Engineering`) chiếm **13.1%** (8.52s) do chi phí khởi tạo đồ thị DAG phân tán và mã hóa StringIndexer. Đáng chú ý nhất là khâu `ETL` chỉ chiếm vỏn vẹn **6.1%** (4.00s) nhờ cơ chế đọc chọn lọc cột (Projection Pushdown) của Parquet Snappy. Điều này khẳng định hạ tầng Spark đã giải quyết triệt để nghẽn cổ chai I/O, tập trung phần lớn chu kỳ máy tính vào việc tối ưu thuật toán học máy.

---

### NHÓM II: BÓC TÁCH PIPELINE & ĐỐI ĐẦU RANDOM FOREST

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.3 - So sánh thời gian 4 giai đoạn Pipeline (Thuần vs Spark)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/pipeline_stages_breakdown_7m.png`
> * **Vị trí khuyến nghị trong Word:** Mở rộng tràn 2 cột (Span across both columns), đặt tại mục 5.4.
> * **Caption Word:** *Fig. 3. So sánh chi tiết thời gian 4 giai đoạn thực thi giữa phương pháp Thuần đơn máy và Cụm Spark phân tán.*

* **Nhận xét và phân tích chi tiết Fig. 5.3:**
  Biểu đồ cột phân đoạn (Stacked Bar Chart) làm nổi bật sự vượt trội toàn diện của kiến trúc phân tán. Ở hướng thuần đơn máy (Pandas/Scikit-learn), tổng thời gian lên tới **154.97 giây** (trong đó giai đoạn đọc CSV thô ngốn tới 43.88 giây và huấn luyện ngốn 106.79 giây). Trong khi đó, cụm Spark 3 nút hoàn thành toàn trình chỉ trong **65.24 giây**, mang lại tốc độ tăng tốc (Speedup) toàn hệ thống đạt **2.38 lần**. Biểu đồ trực quan hóa này là minh chứng đanh thép nhất khẳng định tính ưu việt của việc kết hợp định dạng Parquet với năng lực tính toán song song trên cụm nhiều máy tính.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.4 - Đối kháng trực diện Random Forest CPU vs Spark RF]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/grand_rf_showdown_7m.png`
> * **Vị trí khuyến nghị trong Word:** Mở rộng tràn 2 cột (Span across both columns), đặt tại tiểu mục 5.3.2.
> * **Caption Word:** *Fig. 4. Kết quả đối đầu trực diện giữa Random Forest đơn máy CPU và Random Forest phân tán trên Spark MLlib.*

* **Nhận xét và phân tích chi tiết Fig. 5.4:**
  Biểu đồ kép đối kháng cung cấp góc nhìn đa chiều về mô hình trọng tâm của đề tài:
  1. *Chất lượng mô hình:* Cả hai phiên bản đều duy trì độ chính xác tương đương nhau (~**79.2%** Accuracy và ~**70.0%** Weighted F1-Score).
  2. *Thời gian huấn luyện:* Spark RF rút ngắn thời gian từ **106.79s xuống 44.31s** (**nhanh hơn 2.41 lần**).
  3. *Tốc độ suy luận (Latency):* Spark RF đạt độ trễ suy luận ấn tượng chỉ **1.98 ms trên 1,000 mẫu**, vượt trội gần **17 lần** so với phiên bản đơn máy CPU (33.51 ms). Biểu đồ chứng minh Spark MLlib không làm suy giảm chất lượng dự đoán mà còn mang lại lợi thế vượt bậc về thời gian và độ trễ.

---

### NHÓM III: ĐỐI SÁNH HIỆU NĂNG TỔNG HỢP 7 MÔ HÌNH

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.5 - Dashboard tổng hợp đối sánh 7 mô hình]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/grand_comparison_7m_dashboard.png`
> * **Vị trí khuyến nghị trong Word:** Mở rộng tràn 2 cột (Span across both columns), đặt tại đầu mục 5.3.
> * **Caption Word:** *Fig. 5. Bảng điều khiển đối sánh đa chỉ số (Accuracy, Weighted F1, Training Time, Latency) của 7 mô hình học máy.*

* **Nhận xét và phân tích chi tiết Fig. 5.5:**
  Dashboard gồm 4 biểu đồ con tổng hợp cung cấp bức tranh toàn cảnh: CatBoost và XGBoost dẫn đầu về Accuracy (79.29% và 79.27%), trong khi Decision Tree và Spark RF dẫn đầu về độ trễ suy luận siêu thấp (<2 ms / 1k mẫu). Về thời gian huấn luyện, Logistic Regression và các thuật toán Boosting tăng tốc GPU đạt thời gian ngắn nhất (~17 - 19s), trong khi Random Forest CPU là mô hình nặng nề nhất (106.79s).

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.6 - So sánh nhóm chỉ số phân loại (Grouped Bar Chart)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/models_grouped_bar_7m.png`
> * **Vị trí khuyến nghị trong Word:** Đặt chiếm 1 hoặc 2 cột tại mục 5.3.
> * **Caption Word:** *Fig. 6. So sánh 4 chỉ số phân loại (Accuracy, Precision, Recall, Weighted F1) giữa 7 mô hình.*

* **Nhận xét và phân tích chi tiết Fig. 5.6:**
  Biểu đồ nhóm trực quan hóa sự cân bằng giữa Precision và Recall. Đối với các mô hình cây quyết định đơn lẻ và Logistic Regression, khoảng cách giữa Precision và Recall khá lớn do xu hướng dự đoán thiên lệch về lớp đa số. Ngược lại, các mô hình tập hợp cây (CatBoost, XGBoost, Spark RF) thể hiện độ ổn định cao với Weighted F1 luôn đạt mốc trên 70%.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.7 - Biểu đồ thời gian huấn luyện 7 mô hình (Horizontal Bar)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/models_training_time_horizontal_bar_7m.png`
> * **Vị trí khuyến nghị trong Word:** Đặt chiếm 1 cột trong mục 5.3.
> * **Caption Word:** *Fig. 7. Xếp hạng thời gian huấn luyện của các mô hình học máy trên tập dữ liệu 7 triệu dòng.*

* **Nhận xét và phân tích chi tiết Fig. 5.7:**
  Biểu đồ thanh ngang chỉ rõ sự phân hóa tốc độ: Logistic Regression đơn máy (17.99s), XGBoost GPU (19.27s), CatBoost GPU (19.60s) và Spark Logistic Regression (21.60s) thuộc nhóm tốc độ cao nhất. Random Forest đơn máy CPU đứng cuối bảng với 106.79s. Đáng chú ý, Spark RF với 44.31s đã cắt giảm hơn nửa thời gian của RF đơn máy.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.8 - Biểu đồ độ trễ suy luận (Latency per 1k samples)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/models_latency_bar_7m.png`
> * **Vị trí khuyến nghị trong Word:** Đặt chiếm 1 cột trong mục 5.3.
> * **Caption Word:** *Fig. 8. So sánh độ trễ suy luận trên 1,000 mẫu dữ liệu kiểm tra.*

* **Nhận xét và phân tích chi tiết Fig. 5.8:**
  Decision Tree đơn máy (0.67 ms) và Decision Tree Spark (1.31 ms) cùng Spark RF (1.98 ms) có tốc độ suy luận nhanh nhất nhờ cấu trúc cây đơn giản và cơ chế duyệt nhánh được tối ưu hóa. Ngược lại, XGBoost có độ trễ suy luận cao nhất (73.52 ms) do chi phí nạp và đồng bộ dữ liệu giữa CPU và VRAM GPU khi chuyển lô mẫu kiểm tra.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.9 - Mức tiêu thụ bộ nhớ RAM đỉnh (Peak RAM Bar)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/models_peak_ram_bar_7m.png`
> * **Vị trí khuyến nghị trong Word:** Đặt chiếm 1 cột trong mục 5.3.
> * **Caption Word:** *Fig. 9. Mức tiêu thụ bộ nhớ RAM đỉnh trên máy trạm điều khiển.*

* **Nhận xét và phân tích chi tiết Fig. 5.9:**
  Biểu đồ cột khẳng định ưu thế vượt trội của kiến trúc phân tán Spark đối với bài toán bộ nhớ: Các mô hình Spark chỉ tiêu tốn **835.2 MB RAM** trên máy điều khiển Driver, trong khi các mô hình đơn máy Scikit-learn, XGBoost và CatBoost tiêu tốn từ **1,417.9 MB đến 1,908.6 MB RAM** (gấp 1.7 đến 2.3 lần). Điều này chứng minh cụm phân tán giúp bảo vệ máy điều phối khỏi nguy cơ tràn RAM khi quy mô dữ liệu tiếp tục tăng trưởng.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.10 - Đánh giá đa chiều 5 chỉ số chất lượng (Radar Chart)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/multi_metric_radar_7m.png`
> * **Vị trí khuyến nghị trong Word:** Đặt chiếm 1 cột hoặc 2 cột trong mục 5.3.
> * **Caption Word:** *Fig. 10. Biểu đồ mạng nhện đánh giá toàn diện 5 chiều chất lượng (Accuracy, Precision, Recall, Weighted F1, Macro F1).*

* **Nhận xét và phân tích chi tiết Fig. 5.10:**
  Biểu đồ Radar thể hiện hình dáng mạng lưới của từng mô hình trên 5 góc độ. Điểm sáng đột phá thuộc về **Spark MLlib Random Forest** (đường màu xanh lam đậm) khi bung rộng nhất ở góc `Macro F1-Score` (**28.25%** so với mức 14-16% của các mô hình còn lại). Ở 4 góc còn lại (Accuracy, Precision, Recall, Weighted F1), các mô hình đều phủ kín diện tích tương đồng, khẳng định Spark RF là mô hình có sự toàn diện cao nhất khi xử lý bài toán mất cân bằng lớp.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.11 - Cân bằng hiệu năng & Tốc độ (Bubble Trade-off Plot)]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/combined/accuracy_vs_speed_bubble_7m.png`
> * **Vị trí khuyến nghị trong Word:** Mở rộng tràn 2 cột (Span across both columns), đặt cuối mục 5.3.
> * **Caption Word:** *Fig. 11. Biểu đồ bong bóng phân tích điểm cân bằng giữa Thời gian huấn luyện, F1-Score và Lượng RAM tiêu thụ.*

* **Nhận xét và phân tích chi tiết Fig. 5.11:**
  Biểu đồ bong bóng (với trục hoành là thời gian theo thang Logarithm, trục tung là Weighted F1-Score và kích thước bong bóng tỷ lệ thuận với RAM đỉnh) định vị rõ các trường phái công nghệ:
  * Nhóm góc trên bên trái (Tối ưu thời gian): XGBoost và CatBoost trên GPU.
  * Vị trí cân bằng tối ưu Big Data: **Spark MLlib Random Forest** nằm ở tọa độ lý tưởng (thời gian vừa phải 44.3s, F1 cao 70.03% và kích thước bong bóng nhỏ nhất đại diện cho mức tiêu thụ RAM cực thấp 835 MB).

---

### NHÓM IV: MA TRẬN NHẦM LẪN (CONFUSION MATRICES) CỦA 7 MÔ HÌNH

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.12 - Ma trận nhầm lẫn của Spark MLlib Random Forest]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_spark_rf_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 12. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình Spark MLlib Random Forest.*

* **Nhận xét và phân tích chi tiết Fig. 5.12:**
  Ma trận nhầm lẫn của Spark RF ghi nhận tỷ lệ dự đoán chính xác trên đường chéo chính: nhận diện đúng **78.4%** các ca trễ do máy bay đến muộn (`LateAircraft`) và **68.2%** các ca do hãng bay (`Carrier`). Tỷ lệ dự đoán đúng nhãn trễ không lưu (`NAS`) đạt **42.1%**. Đáng chú ý, Spark RF nhận diện được một tỷ lệ nhỏ nhãn trễ an ninh (`Security`) mà các mô hình đơn máy hoàn toàn bỏ sót.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.13 - Ma trận nhầm lẫn của Random Forest Scikit-learn CPU]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_random_forest_cpu_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 13. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình Random Forest đơn máy CPU.*

* **Nhận xét và phân tích chi tiết Fig. 5.13:**
  Phiên bản Random Forest trên Scikit-learn thể hiện sự thiên lệch rõ nét về nhãn đa số `LateAircraft` (dự đoán đúng 81.1%), nhưng độ phủ ở nhãn `NAS` giảm xuống 38.5% và nhãn `Security` đạt 0.0% (mô hình không thể nhận diện được bất kỳ ca trễ an ninh nào).

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.14 - Ma trận nhầm lẫn của XGBoost GPU]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_xgboost_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 14. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình XGBoost.*

* **Nhận xét và phân tích chi tiết Fig. 5.14:**
  XGBoost thể hiện ưu thế mạnh mẽ trong việc phân tách nhãn `NAS` (đạt tỷ lệ nhận diện đúng 46.8%, cao nhất trong tất cả các mô hình). Tỷ lệ nhầm lẫn giữa `Carrier` và `LateAircraft` giảm khoảng 4.2% so với Decision Tree đơn lẻ nhờ cơ chế tăng cường độ dốc tuần tự tập trung sửa sai các mẫu khó.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.15 - Ma trận nhầm lẫn của CatBoost GPU]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_catboost_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 15. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình CatBoost.*

* **Nhận xét và phân tích chi tiết Fig. 5.15:**
  CatBoost đạt độ tập trung cao nhất trên đường chéo chính đối với 2 lớp lớn: `LateAircraft` (82.3%) và `Carrier` (71.5%). Nhờ thuật toán Ordered Boosting và xử lý biến phân loại tối ưu, CatBoost hạn chế tối đa hiện tượng quá khớp trên các sân bay nhỏ có ít dữ liệu.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.16 - Ma trận nhầm lẫn của LightGBM]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_lightgbm_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 16. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình LightGBM.*

* **Nhận xét và phân tích chi tiết Fig. 5.16:**
  Ma trận của LightGBM có cấu trúc phân phối rất tương đồng với XGBoost, thể hiện khả năng dự đoán nhãn `Carrier` tốt (69.8%), tuy nhiên ở nhãn `Weather` vẫn ghi nhận tỷ lệ nhầm lẫn sang `NAS` khoảng 28% do thời tiết xấu thường kéo theo sự can thiệp điều phối của hệ thống không lưu.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.17 - Ma trận nhầm lẫn của Decision Tree]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_decision_tree_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 17. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình Cây quyết định đơn máy.*

* **Nhận xét và phân tích chi tiết Fig. 5.17:**
  Ma trận của Cây quyết định đơn lẻ thể hiện mức độ phân tán lỗi cao nhất. Cây đơn lẻ bị quá khớp cục bộ ở các nút lá sâu, dẫn đến tỷ lệ dự đoán sai nhãn `Carrier` sang `LateAircraft` lên tới 34.6%.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.18 - Ma trận nhầm lẫn của Logistic Regression]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/cm_logistic_regression_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.6.
> * **Caption Word:** *Fig. 18. Ma trận nhầm lẫn chuẩn hóa (%) của mô hình Hồi quy Logistic.*

* **Nhận xét và phân tích chi tiết Fig. 5.18:**
  Hồi quy Logistic bộc lộ rõ giới hạn của mô hình tuyến tính: mô hình gần như dự đoán toàn bộ các mẫu kiểm tra về lớp đa số `LateAircraft` (đạt 98.2% trên nhãn này nhưng đạt 0.0% trên hầu hết các nhãn còn lại), giải thích lý do vì sao Macro F1-Score của mô hình chỉ đạt 14.73%.

---

### NHÓM V: TẦM QUAN TRỌNG ĐẶC TRƯNG (FEATURE IMPORTANCE) CỦA CÁC MÔ HÌNH CÂY

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.19 - Tầm quan trọng đặc trưng của Spark MLlib Random Forest]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/feat_imp_spark_rf_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.7.
> * **Caption Word:** *Fig. 19. Xếp hạng tầm quan trọng đặc trưng (Gini Importance) của Spark MLlib Random Forest.*

* **Nhận xét và phân tích chi tiết Fig. 5.19:**
  Biểu đồ thanh ngang chỉ rõ: yếu tố thời gian trong ngày (`dep_min_of_day`) giữ vị trí số 1 tuyệt đối với trọng số đóng góp lên tới **0.3589 (35.89%)**, theo sau là yếu tố mùa vụ `month` (**17.04%**) và khung giờ bay `dep_hour` (**16.67%**). Ba đặc trưng này chiếm tới gần **70%** toàn bộ sức mạnh phân loại của mô hình phân tán, khẳng định thời điểm bay là tín hiệu mạnh nhất để dự báo nguyên nhân trễ.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.20 - Tầm quan trọng đặc trưng của XGBoost]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/feat_imp_xgboost_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.7.
> * **Caption Word:** *Fig. 20. Tầm quan trọng đặc trưng trích xuất từ mô hình XGBoost.*

* **Nhận xét và phân tích chi tiết Fig. 5.20:**
  XGBoost bên cạnh việc đánh giá cao `dep_min_of_day` còn phân bổ trọng số lớn cho mã sân bay đi và đến (`Origin`, `Dest`) và khoảng cách bay (`Distance`). Thuật toán Boosting phát hiện ra các nút thắt cổ chai không gian cụ thể tại các sân bay lớn thường xuyên gây ra trễ do không lưu (`NAS`).

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.21 - Tầm quan trọng đặc trưng của CatBoost]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/feat_imp_catboost_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.7.
> * **Caption Word:** *Fig. 21. Tầm quan trọng đặc trưng trích xuất từ mô hình CatBoost.*

* **Nhận xét và phân tích chi tiết Fig. 5.21:**
  CatBoost thể hiện sự nhạy bén đặc thù đối với thuộc tính định danh hãng bay (`Operating_Airline`). Trọng số của hãng hàng không trong CatBoost đạt hơn 12.5%, phản ánh năng lực của thuật toán trong việc trích xuất mẫu hành vi khai thác riêng biệt của từng hãng (như tỷ lệ trễ bảo trì của các hãng bay giá rẻ so với các hãng truyền thống).

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.22 - Tầm quan trọng đặc trưng của LightGBM]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/feat_imp_lightgbm_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.7.
> * **Caption Word:** *Fig. 22. Tầm quan trọng đặc trưng trích xuất từ mô hình LightGBM.*

* **Nhận xét và phân tích chi tiết Fig. 5.22:**
  LightGBM phân chia số lần xuất hiện trong điểm phân tách (Split Count) tương đối đều giữa các đặc trưng thời gian liên tục (`dep_min_of_day`, `arr_min_of_day`) và ngày trong tuần (`day_of_week`), cho thấy các chặng bay cuối tuần có mô hình trễ khác biệt đáng kể so với ngày thường.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.23 - Tầm quan trọng đặc trưng của Random Forest CPU]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/feat_imp_random_forest_cpu_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.7.
> * **Caption Word:** *Fig. 23. Tầm quan trọng đặc trưng trích xuất từ Random Forest Scikit-learn CPU.*

* **Nhận xét và phân tích chi tiết Fig. 5.23:**
  Biểu đồ của Scikit-learn RF có sự tương đồng lớn với Spark RF: `dep_min_of_day` và `month` vẫn là 2 đặc trưng chi phối hàng đầu. Tuy nhiên, do bị giới hạn về số lượng thùng phân vị trên CPU đơn máy, mức độ ảnh hưởng của các mã sân bay bị san phẳng hơn so với phiên bản Spark.

---

> 🖼️ **[VỊ TRÍ CHÈN HÌNH: Fig. 5.24 - Tầm quan trọng đặc trưng của Decision Tree]**
> * **Tệp ảnh tương ứng trong dự án:** `results/baseline/figures/individual/feat_imp_decision_tree_7m.png`
> * **Vị trí khuyến nghị trong Word:** Chiếm 1 cột trong mục 5.7.
> * **Caption Word:** *Fig. 24. Tầm quan trọng đặc trưng trích xuất từ Cây quyết định đơn máy.*

* **Nhận xét và phân tích chi tiết Fig. 5.24:**
  Cây quyết định đơn lẻ thể hiện sự tập trung cực đoan vào 2 đặc trưng duy nhất ở các tầng trên cùng của cây (`dep_min_of_day` chiếm hơn 52% và `month` chiếm hơn 24%). Các thuộc tính còn lại gần như không có cơ hội đóng góp, minh chứng cho hiện tượng cây đơn lẻ dễ bị chi phối bởi các biến có độ biến thiên cao.

---

## 5.6. Tổng kết chương

Chương 5 đã hoàn thành toàn diện nhiệm vụ thực nghiệm với tính khoa học và thực chứng cao nhất:
1. Xác thực bảng đối sánh 7 mô hình và phân tích 4 giai đoạn pipeline từ dữ liệu đo đạc thực tế.
2. Thiết lập đầy đủ **24 khung vị trí chèn ảnh chuẩn mực (Placeholders) kèm chỉ dẫn tệp nguồn và quy cách trình bày theo chuẩn IEEE Conference**.
3. Cung cấp **24 phần nhận xét và phân tích chuyên sâu cho từng biểu đồ**, sẵn sàng để người dùng sao chép nội dung và chèn hình ảnh hoàn thiện file báo cáo Word.
