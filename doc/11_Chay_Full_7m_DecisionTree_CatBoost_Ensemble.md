# BÁO CÁO KẾT QUẢ THỬ NGHIỆM SINGLE-NODE BASELINE
**(HƯỚNG TIẾP CẬN TRUYỀN THỐNG BẰNG PANDAS & SCIKIT-LEARN)**

---

## 1. MỤC TIÊU VÀ PHẠM VI NGHIÊN CỨU
Trong giai đoạn này, nhóm tiến hành xây dựng một đường ống dữ liệu (Data Pipeline) và huấn luyện mô hình Học máy trên kiến trúc **Single-node** (1 máy tính cá nhân). 
Mục tiêu cốt lõi không chỉ là tìm kiếm một mô hình có độ chính xác cao, mà là **đo lường và phơi bày những giới hạn vật lý (điểm nghẽn cổ chai về RAM, CPU, I/O)** của các công cụ truyền thống (Pandas, Scikit-learn) khi phải xử lý khối lượng Big Data (hơn 7 triệu dòng dữ liệu bay năm 2024). Đây sẽ là tiền đề vững chắc để làm nổi bật tính ưu việt của kiến trúc phân tán Apache Spark trong Giai đoạn 2 của dự án.

## 2. QUY TRÌNH XỬ LÝ DỮ LIỆU TỰ ĐỘNG (AUTOMATED ETL PIPELINE)
Để thay thế phương pháp làm sạch dữ liệu thủ công, nhóm đã thiết lập một Pipeline tự động 100% bám sát tư duy Kỹ sư Dữ liệu, tuân thủ nghiêm ngặt nguyên tắc **Chống Rò rỉ dữ liệu (Anti Data-Leakage)**.

### 2.1. Nạp và Khai thác Dữ liệu thô (Data Ingestion)
*   **Tổng dữ liệu gốc:** Toàn bộ file CSV ~1.2GB với hơn **7.07 triệu chuyến bay**.
*   **Tiền xử lý nhãn:** Vì bài toán tập trung vào việc "Phân loại 5 nguyên nhân gây trễ" (Carrier, Weather, NAS, Security, LateAircraft), nhóm đã chủ động cấu hình thuật toán chỉ giữ lại **1,449,972 chuyến bay bị trễ thực sự** ($\ge$ 15 phút), gạt bỏ các chuyến bay On-Time nhằm ngăn chặn tình trạng mất cân bằng dữ liệu cực đoan.

### 2.2. Kỹ thuật Lọc Nhiễu và Giảm Chiều Dữ Liệu
1.  **Lọc Ngoại lai (Outlier Detection) bằng IQR (Khoảng phân vị):**
    Thuật toán tự động quét các cột thời gian (taxi_out, dep_delay,...) và cắt bỏ 200,136 dòng mang giá trị dị biệt (do lỗi nhập liệu hệ thống). Khối lượng dữ liệu sạch chốt hạ ở mức **1,249,836 dòng**.
2.  **Chặn Đa Cộng Tuyến (Multicollinearity) bằng Ma trận Tương quan (Correlation Matrix):**
    Thuật toán thiết lập ngưỡng tương quan $\ge$ 0.85, tự động gỡ bỏ các đặc trưng trùng lặp mang tính dây chuyền như `distance`, `air_time`, `dep_hour` để tăng tốc độ hội tụ cho mô hình.
3.  **Lựa chọn Đặc trưng (Feature Selection) bằng Information Gain:**
    Triển khai một cây quyết định nháp (Draft Decision Tree) để tính điểm *Feature Importance*. Thuật toán tự động loại bỏ thêm 16 đặc trưng mang điểm số bằng 0 (như `cancellation_code`, `origin_city_name`...), chỉ giữ lại **6 đặc trưng tinh túy nhất** cho quá trình học sâu.

---

## 3. KẾT QUẢ HUẤN LUYỆN VÀ ĐÁNH GIÁ MÔ HÌNH
Trên tập dữ liệu 1.25 triệu mẫu siêu sạch, nhóm tiến hành huấn luyện 3 kiến trúc mô hình học máy. Các chỉ số được đo lường bằng phương pháp *Weighted Average* để công bằng hóa các nhãn hiếm (như lỗi An ninh - Security).

### 3.1. Bảng Chỉ số Đánh giá Tổng hợp

| Kiến trúc Mô hình | Độ chính xác (Accuracy) | Precision | Recall | F1-Score | Thời gian Huấn luyện |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Decision Tree (CART)** | 43.54% | 53.55% | 43.54% | 45.86% | **3.08s** (Nhanh nhất) |
| **CatBoost (SOTA)** | 42.36% | 53.90% | 42.36% | 45.19% | 17.69s |
| **Ensemble (Soft Voting)**| **43.94%** | **54.19%** | **43.94%** | **46.46%** | 20.83s |

*Đánh giá:* Tuy Accuracy dao động ở mức ~44%, nhưng với một bài toán phân lớp 5 nhãn vô định (tỷ lệ random là 20%), mức **F1-Score đạt 46.46%** chứng tỏ mô hình đã nắm bắt thành công các quy luật ngầm của hàng không, không phải kết quả của việc đoán mò.

### 3.2. Thuật toán Nổi trội nhất: Ensemble Learning
Mô hình **Ensemble (DT + CatBoost)** sử dụng cơ chế **Soft Voting** đã xuất sắc đạt điểm F1 cao nhất. Bằng cách kết hợp xác suất (probabilities) thay vì đếm phiếu cứng, Ensemble đã bù trừ hoàn hảo sự sắc bén về luật IF-ELSE của Decision Tree và sự bền bỉ xử lý nhiễu đa chiều của CatBoost.

**Ma trận nhầm lẫn (Confusion Matrix) của mô hình Ensemble:**
*(Thứ tự Nhãn: Carrier, Weather, NAS, Security, LateAircraft)*
```text
[[15992 13728 11610 10890 27669]
 [  902  3127  1632   932  1917]
 [ 4854  4914 32478  6261  9769]
 [   56    92    70   292   212]
 [ 8971 10346 13061 12247 57946]]
```

---

## 4. PHÂN TÍCH ƯU / NHƯỢC ĐIỂM VÀ KẾT LUẬN

### 4.1. Những điểm sáng (Pros)
*   Xây dựng thành công **Tư duy Làm sạch dữ liệu tự động** thay vì làm bằng tay (Hard-code).
*   Tính minh bạch cao: Kiểm soát hoàn toàn **Data Leakage**, giúp mô hình không "học vẹt" kết quả thời gian hạ cánh thực tế.

### 4.2. Khám phá Giới hạn của Single-Node (Cons)
*   **Điểm nghẽn Bộ nhớ (RAM Bottleneck):** Thư viện Pandas vận hành theo cơ chế nạp toàn bộ vào RAM (In-Memory). Việc đọc 1.2GB dữ liệu khiến máy tính cá nhân bị "treo" cục bộ hơn 30 giây, đẩy RAM chạm ngưỡng giới hạn.
*   **Điểm nghẽn Tính toán (CPU Bottleneck):** Việc huấn luyện mô hình CatBoost và Ensemble mất tới hơn 20 giây cho 1.25 triệu mẫu. Nếu kích thước dữ liệu tăng lên gấp 10 lần (10GB), hệ thống Single-node chắc chắn sẽ sập đổ (OOM - Out of Memory).

### 4.3. Đề xuất Cải tiến (Hướng tới Apache Spark)
Từ những giới hạn vật lý rõ rệt trên, nhóm đề xuất **2 mũi nhọn chiến lược** cho giai đoạn tiếp theo:
1.  **Về mặt Kiến trúc:** Di chuyển toàn bộ Data Pipeline sang hệ sinh thái **Apache Spark (PySpark)**. Nhờ cơ chế băm dữ liệu (Partitioning) và RDD, Spark sẽ tận dụng tối đa tính toán phân tán, xóa bỏ rào cản RAM của Pandas và kéo giảm thời gian huấn luyện xuống chỉ còn vài giây.
2.  **Về mặt Nghiệp vụ Học máy:** Để bức phá độ chính xác lên ngưỡng 70%, dự án cần áp dụng **SMOTE** để bù đắp các nhãn dữ liệu hiếm (Security), kết hợp **Feature Engineering** chuyên sâu như nối (Join) thêm dữ liệu thời tiết API và mật độ không lưu theo từng giờ vào dataset gốc.
