# BỘ CÔNG THƯƠNG
## TRƯỜNG ĐẠI HỌC CÔNG THƯƠNG THÀNH PHỐ HỒ CHÍ MINH
### KHOA CÔNG NGHỆ THÔNG TIN

---

<br><br><br>

# BÁO CÁO ĐỒ ÁN HỌC PHẦN
## MÔN HỌC: NHẬP MÔN BIG DATA

<br>

### TÊN ĐỀ TÀI:
# ỨNG DỤNG THUẬT TOÁN RANDOM FOREST PHÂN TÁN DỰ ĐOÁN NGUYÊN NHÂN TRỄ CHUYẾN BAY THƯƠNG MẠI: TRƯỜNG HỢP TẠI HOA KỲ NĂM 2024

<br><br>

* **Giảng viên hướng dẫn:** TS. Phan Hồ Viết Trường
* **Lớp học phần:** Nhập môn Big Data (T4, C10-12)
* **Học viên thực hiện:** Nhóm 6
  1. **Cù Văn Vĩ An** - MSSV: 2001230005
  2. **Lê Đức Lương** - MSSV: 2001230490 (Nhóm trưởng)
  3. **Trần Huỳnh Tuấn Anh** - MSSV: 2001230023

<br><br><br>

**TP. HỒ CHÍ MINH, NĂM 2026**

---

<div style="page-break-after: always;"></div>

# LỜI CAM ĐOAN

Chúng em xin cam đoan đề án **"Ứng dụng thuật toán Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay thương mại: Trường hợp tại Hoa Kỳ năm 2024"** là công trình nghiên cứu và thực nghiệm độc lập do tập thể Nhóm 6 thực hiện dưới sự định hướng, hướng dẫn khoa học trực tiếp của **TS. Phan Hồ Viết Trường**.

Toàn bộ các số liệu khảo sát, kết quả thực nghiệm huấn luyện mô hình học máy, các chỉ số đối sánh hiệu năng hệ thống phân tán và ứng dụng phục vụ mô hình được trình bày trong báo cáo này là hoàn toàn trung thực, khách quan và được đo đạc từ hệ thống thực tế tại phòng lab của nhóm. Các tài liệu, công trình nghiên cứu và mã nguồn mở tham khảo đều được trích dẫn nguồn gốc rõ ràng, hợp lệ theo đúng quy chuẩn học thuật của Trường Đại học Công Thương Thành phố Hồ Chí Minh.

Chúng em xin chịu hoàn toàn trách nhiệm trước Hội đồng chấm đồ án và Nhà trường về tính trung thực của toàn bộ nội dung trong báo cáo này!

<br>

*TP. Hồ Chí Minh, ngày 05 tháng 10 năm 2026*

**Đại diện nhóm tác giả**  
*(Ký và ghi rõ họ tên)*

<br><br>

**Lê Đức Lương**

---

<div style="page-break-after: always;"></div>

# LỜI CẢM ƠN

Lời đầu tiên, nhóm tác giả xin được bày tỏ lòng biết ơn sâu sắc và chân thành nhất tới Ban Giám hiệu Trường Đại học Công Thương Thành phố Hồ Chí Minh (HUIT) cùng các Thầy, Cô giáo thuộc Khoa Công nghệ Thông tin đã tạo điều kiện học tập thuận lợi và truyền đạt cho chúng em những nền tảng tri thức vững chắc về khoa học máy tính và công nghệ dữ liệu trong suốt quá trình theo học tại trường.

Đặc biệt, nhóm chúng em xin gửi lời cảm ơn trân trọng và sâu sắc nhất đến **TS. Phan Hồ Viết Trường**, giảng viên trực tiếp giảng dạy và hướng dẫn học phần Nhập môn Big Data. Thầy đã dành nhiều thời gian quý báu để định hướng đề tài, tận tình chỉ dẫn phương pháp luận nghiên cứu, gợi mở các giải pháp kỹ thuật tối ưu hóa trong môi trường tính toán phân tán và luôn động viên, khích lệ nhóm vượt qua những thách thức kỹ thuật phát sinh trong suốt quá trình triển khai hệ thống thực tế trên cụm phần cứng vật lý.

Do thời gian thực hiện có hạn và kiến thức thực tế của nhóm còn đang trong quá trình tích lũy, báo cáo đồ án khó tránh khỏi những thiếu sót nhất định. Nhóm rất mong nhận được những ý kiến đóng góp, chỉ bảo quý báu từ Thầy và Hội đồng thẩm định để đề tài được hoàn thiện hơn nữa, phục vụ tốt cho các hướng nghiên cứu chuyên sâu tiếp theo.

Nhóm tác giả xin kính chúc Thầy luôn dồi dào sức khỏe, hạnh phúc và gặt hái thêm nhiều thành công rực rỡ trong sự nghiệp nghiên cứu và giảng dạy cao quý!

---

<div style="page-break-after: always;"></div>

# MỤC LỤC TỔNG QUÁT

* **LỜI CAM ĐOAN**
* **LỜI CẢM ƠN**
* **DANH MỤC TỪ VIẾT TẮT**
* **DANH MỤC BẢNG BIỂU**
* **DANH MỤC HÌNH ẢNH**
* **LỜI MỞ ĐẦU**
* **CHƯƠNG 1: TỔNG QUAN ĐỀ TÀI**
  * 1.1. Đặt vấn đề
  * 1.2. Tình hình nghiên cứu và định hướng thực hiện
  * 1.3. Mục tiêu nghiên cứu
    * 1.3.1. Mục tiêu tổng quát
    * 1.3.2. Mục tiêu cụ thể
  * 1.4. Đối tượng và phạm vi nghiên cứu
  * 1.5. Nội dung và phương pháp nghiên cứu
  * 1.6. Bố cục đồ án
* **CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ CÔNG NGHỆ LIÊN QUAN**
  * 2.1. Tổng quan về Big Data
  * 2.2. Hệ thống tệp phân tán Hadoop (HDFS)
  * 2.3. Hệ sinh thái tính toán phân tán Apache Spark
  * 2.4. Học máy phân tán với Spark MLlib và các thuật toán đối sánh
  * 2.5. Các phương pháp đánh giá mô hình phân loại đa lớp
  * 2.6. Định dạng lưu trữ tối ưu Apache Parquet
  * 2.7. Công nghệ Serving và Giao diện Dashboard (FastAPI)
  * 2.8. Tổng kết chương
* **CHƯƠNG 3: PHÂN TÍCH DỮ LIỆU VÀ TIỀN XỬ LÝ DỮ LIỆU LỚN**
  * 3.1. Giới thiệu bộ dữ liệu hàng không Hoa Kỳ 2024
  * 3.2. Khảo sát và phân tích khám phá dữ liệu (EDA)
  * 3.3. Tổ chức lưu trữ dữ liệu tối ưu với Apache Parquet
  * 3.4. Tiền xử lý dữ liệu quy mô lớn (PySpark & Polars)
  * 3.5. Xây dựng biến mục tiêu và phân loại 5 nhóm nguyên nhân trễ
  * 3.6. Trích xuất đặc trưng và kỹ thuật chống rò rỉ dữ liệu (Data Leakage)
  * 3.7. Chiến lược phân chia tập dữ liệu
  * 3.8. Tổng kết chương
* **CHƯƠNG 4: THIẾT KẾ HỆ THỐNG VÀ XÂY DỰNG MÔ HÌNH PHÂN TÁN**
  * 4.1. Kiến trúc tổng thể hệ thống hai nhánh (Dual-Branch Pipeline)
  * 4.2. Xây dựng Machine Learning Pipeline trên Apache Spark
  * 4.3. Thiết lập các mô hình học máy đối chứng
  * 4.4. Xây dựng mô hình Random Forest phân tán
  * 4.5. Thiết kế và triển khai Cụm vật lý 3 Node qua Tailscale Mesh VPN
  * 4.6. Cơ chế phân vùng dữ liệu và tối ưu hóa Spark Tuning
  * 4.7. Quản lý và đóng gói Model Artifacts
  * 4.8. Tổng kết chương
* **CHƯƠNG 5: THỰC NGHIỆM VÀ ĐÁNH GIÁ KẾT QUẢ**
  * 5.1. Môi trường thực nghiệm
  * 5.2. Kịch bản thực nghiệm trên 7,079,081 dòng dữ liệu
  * 5.3. Bảng đối sánh toàn diện 7 mô hình học máy
  * 5.4. Bóc tách thời gian thực thi theo 4 giai đoạn Pipeline
  * 5.5. Đánh giá hiệu năng và khả năng mở rộng của Cụm Spark phân tán
  * 5.6. Phân tích chi tiết Ma trận nhầm lẫn (Confusion Matrix)
  * 5.7. Phân tích Tầm quan trọng đặc trưng (Feature Importance)
  * 5.8. Tổng kết chương
* **CHƯƠNG 6: XÂY DỰNG HỆ THỐNG PHỤC VỤ MÔ HÌNH VÀ DEMO (WEB_AIR)**
  * 6.1. Kiến trúc hệ thống Serving & Dashboard
  * 6.2. Thiết kế RESTful API bằng FastAPI
  * 6.3. Xây dựng giao diện Web Dashboard trực quan hóa (3 Tab)
  * 6.4. Đánh giá độ trễ suy luận và kiểm thử tải hệ thống
  * 6.5. Tổng kết chương
* **CHƯƠNG 7: KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN**
  * 7.1. Các kết quả và đóng góp chính của đồ án
  * 7.2. Những khó khăn và hạn chế của đề tài
  * 7.3. Hướng phát triển trong tương lai
  * 7.4. Tổng kết toàn diện đồ án
* **TÀI LIỆU THAM KHẢO**

---

<div style="page-break-after: always;"></div>

# DANH MỤC TỪ VIẾT TẮT

| Viết tắt | Thuật ngữ tiếng Anh | Định nghĩa / Diễn giải tiếng Việt |
| :--- | :--- | :--- |
| **API** | Application Programming Interface | Giao diện lập trình ứng dụng |
| **BTS** | Bureau of Transportation Statistics | Cục Thống kê Vận tải Hoa Kỳ |
| **CPU** | Central Processing Unit | Bộ vi xử lý trung tâm |
| **CSV** | Comma-Separated Values | Định dạng tệp văn bản phân tách bằng dấu phẩy |
| **DAG** | Directed Acyclic Graph | Đồ thị có hướng không chu trình |
| **EDA** | Exploratory Data Analysis | Phân tích khám phá dữ liệu |
| **FAA** | Federal Aviation Administration | Cục Hàng không Liên bang Hoa Kỳ |
| **GPU** | Graphics Processing Unit | Bộ xử lý đồ họa (tăng tốc tính toán song song) |
| **HDFS** | Hadoop Distributed File System | Hệ thống tệp phân tán Hadoop |
| **IQR** | Interquartile Range | Khoảng tứ phân vị (phương pháp xác định ngoại lai) |
| **JSON** | JavaScript Object Notation | Định dạng trao đổi dữ liệu dạng văn bản nhẹ |
| **ML** | Machine Learning | Học máy |
| **MLlib** | Machine Learning Library | Thư viện học máy của Apache Spark |
| **NAS** | National Airspace System | Hệ thống không lưu quốc gia Hoa Kỳ |
| **OHE** | One-Hot Encoding | Kỹ thuật mã hóa biến định danh thành vector nhị phân |
| **RAM** | Random Access Memory | Bộ nhớ truy xuất ngẫu nhiên |
| **RDD** | Resilient Distributed Dataset | Tập dữ liệu phân tán có khả năng đàn hồi trong Spark |
| **REST** | Representational State Transfer | Kiểu kiến trúc dịch vụ web truyền trạng thái đại diện |
| **RF** | Random Forest | Thuật toán Rừng ngẫu nhiên |
| **ROC** | Receiver Operating Characteristic | Đường cong đặc trưng hoạt động máy thu |
| **AUC** | Area Under Curve | Diện tích dưới đường cong đánh giá phân loại |
| **UI** | User Interface | Giao diện người dùng |
| **VPN** | Virtual Private Network | Mạng riêng ảo kết nối bảo mật |
| **XGBoost** | Extreme Gradient Boosting | Thuật toán tăng cường độ dốc cực trị |
