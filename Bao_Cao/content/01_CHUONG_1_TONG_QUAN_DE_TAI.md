# LỜI MỞ ĐẦU

Giao thông hàng không giữ vai trò huyết mạch trong cấu trúc kinh tế - xã hội hiện đại, là cầu nối giao thương thương mại và luân chuyển hành khách với lưu lượng hàng triệu người mỗi ngày. Tuy nhiên, sự cố trễ chuyến bay (Flight Delays) luôn là một trong những thách thức nan giải và kinh niên nhất của ngành hàng không toàn cầu. Theo các thống kê của Cục Thống kê Vận tải Hoa Kỳ (Bureau of Transportation Statistics - BTS) và Cục Hàng không Liên bang (Federal Aviation Administration - FAA), thiệt hại kinh tế trực tiếp và gián tiếp do trễ chuyến bay tại Hoa Kỳ lên đến hàng chục tỷ USD mỗi năm, bao gồm chi phí nhiên liệu gia tăng, chi phí bồi thường và sắp xếp lưu trú cho hành khách, chi phí trả thêm giờ cho phi hành đoàn và suy giảm uy tín thương hiệu của các hãng hàng không.

Trước đây, phần lớn các nghiên cứu dự đoán trễ chuyến bay thường tiếp cận bài toán dưới dạng bài toán phân loại nhị phân đơn giản (Chuyến bay có trễ hay không trễ dựa trên ngưỡng trễ quy ước 15 phút). Tuy nhiên, trên góc độ vận hành thực tế tại các trung tâm điều hành bay (Flight Operations Center - FOC), việc chỉ biết một chuyến bay bị trễ mà không biết trước **nguyên nhân cốt lõi gây trễ** là gì (do hãng bay, do thời tiết cực đoan, do tắc nghẽn kiểm soát không lưu, do an ninh hay do máy bay chặng trước đến muộn) không mang lại nhiều giá trị thực tiễn cho việc ra quyết định điều phối chủ động.

Hơn thế nữa, dữ liệu vận hành hàng không mang đầy đủ các đặc trưng kinh điển của dữ liệu lớn (Big Data). Chỉ riêng trong năm 2024, mạng lưới hàng không thương mại nội địa Hoa Kỳ đã ghi nhận hơn 7 triệu chuyến bay, với hàng chục thuộc tính phức tạp về thời gian, không gian, lịch trình và yếu tố môi trường. Việc xử lý khối lượng dữ liệu khổng lồ này vượt quá giới hạn của các hệ thống cơ sở dữ liệu quan hệ truyền thống và các công cụ phân tích đơn máy (Single-node). Các phương pháp phân tích truyền thống nhanh chóng gặp hiện tượng tràn bộ nhớ (Out-Of-Memory - OOM) và thời gian xử lý kéo dài nhiều giờ hoặc nhiều ngày.

Xuất phát từ yêu cầu thực tiễn đó, đề tài đồ án **"Ứng dụng thuật toán Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay thương mại: Trường hợp tại Hoa Kỳ năm 2024"** được thực hiện nhằm xây dựng một quy trình công nghệ dữ liệu lớn hoàn chỉnh. Đề tài kết hợp sức mạnh tính toán phân tán của hệ sinh thái Apache Spark và thư viện học máy phân tán Spark MLlib, tối ưu hóa lưu trữ cột Apache Parquet, đồng thời đối sánh sâu sắc với các mô hình học máy hiện đại trên đơn máy (XGBoost, LightGBM, CatBoost) và triển khai hệ thống phục vụ mô hình thông qua nền tảng web hiện đại.

---

# CHƯƠNG 1: TỔNG QUAN ĐỀ TÀI

## 1.1. Đặt vấn đề

Trong kỷ nguyên chuyển đổi số và bùng nổ dữ liệu, ngành công nghiệp hàng không thương mại đã và đang tích lũy một khối lượng dữ liệu khổng lồ từ hệ thống đặt chỗ, cảm biến máy bay, thông tin radar thời tiết và nhật ký điều phối bay của FAA. Trong năm 2024, toàn mạng lưới hàng không nội địa Hoa Kỳ đã vận hành hơn 7 triệu chuyến bay. Sự gia tăng liên tục về mật độ bay tại các cảng hàng không cửa ngõ (Hubs) như Atlanta (ATL), Dallas/Fort Worth (DFW), Denver (DEN), Chicago O'Hare (ORD) đã khiến cho hạ tầng không lưu thường xuyên rơi vào tình trạng quá tải.

Trễ chuyến bay là một hiện tượng phát sinh từ nhiều yếu tố tương tác phức tạp và có tính chất lan truyền dây chuyền (Delay Propagation). Theo quy định tiêu chuẩn của BTS và FAA, nguyên nhân gây chậm chuyến bay thương mại được bóc tách và phân thành 5 nhóm nguyên nhân chính yếu:
1. **Trễ do hãng hàng không (Carrier Delay):** Bao gồm các sự cố bảo dưỡng kỹ thuật máy bay, vệ sinh khoang hành khách, tra nạp nhiên liệu chậm, phi hành đoàn hoặc tiếp viên đến muộn, trục trặc phần mềm đặt chỗ hoặc thủ tục hành lý.
2. **Trễ do thời tiết cực đoan (Weather Delay):** Do giông bão, tuyết rơi dày, sương mù làm tầm nhìn giảm dưới mức an toàn, hoặc gió giật mạnh vượt ngưỡng khai thác an toàn của tàu bay.
3. **Trễ do hệ thống không lưu quốc gia (National Airspace System - NAS Delay):** Phát sinh do mật độ không phận quá dày đặc, kiểm soát viên không lưu giãn khoảng cách hạ cánh, hoạt động tại sân bay bị tắc nghẽn đường lăn (taxiway), hoặc bảo trì đường băng.
4. **Trễ do an ninh (Security Delay):** Do sự cố sơ tán nhà ga, hành khách vi phạm quy trình kiểm tra an ninh, lỗi cổng soi chiếu hành lý, hoặc tái kiểm tra tàu bay.
5. **Trễ do máy bay đến muộn (Late Aircraft Delay):** Tàu bay thực hiện chuyến bay hiện tại bị trễ do chặng bay trước đó đến trễ, gây ra hiệu ứng gợn sóng lan tỏa (Ripple Effect) suốt cả ngày khai thác.

Về mặt công nghệ, tập dữ liệu toàn bộ chuyến bay nội địa năm 2024 của BTS bao gồm **7,079,081 dòng** với dung lượng tệp thô định dạng CSV lên tới hơn **2.3 GB**. Khi đưa toàn bộ dữ liệu này vào bộ nhớ RAM của một máy tính cá nhân để tiến hành tiền xử lý, tính toán ma trận đặc trưng một nóng (One-Hot Encoding) và huấn luyện các mô hình tập hợp cây (Ensemble Models), hệ thống thường xuyên đối mặt với các nút thắt cổ chai nghiêm trọng:
* **Hạn chế bộ nhớ vật lý:** Tràn RAM dẫn đến lỗi hệ thống hoặc phân trang đĩa nhớ (Disk Paging/Swapping) làm tốc độ tính toán suy giảm hàng trăm lần.
* **Thời gian huấn luyện kéo dài:** Các thuật toán xây dựng cây quyết định truyền thống có độ phức tạp tính toán cao theo số lượng mẫu và số lượng đặc trưng.
* **Khó khăn trong việc mở rộng quy mô (Scalability):** Mô hình đơn máy không thể tự động mở rộng khi dữ liệu tăng thêm theo từng năm hoặc khi tích hợp dữ liệu stream theo thời gian thực.

Do đó, việc nghiên cứu và áp dụng nền tảng tính toán phân tán **Apache Spark** kết hợp thuật toán **Random Forest phân tán (Spark MLlib)** là một đòi hỏi cấp thiết và mang tính khoa học cao nhằm giải quyết bài toán dự đoán nguyên nhân trễ chuyến bay trên quy mô Big Data.

---

## 1.2. Tình hình nghiên cứu và định hướng thực hiện

### 1.2.1. Tình hình nghiên cứu trong và ngoài nước
Trong những năm qua, bài toán phân tích và dự báo chuyến bay đã thu hút sự quan tâm của nhiều nhà khoa học dữ liệu trên thế giới:
* Nhiều nghiên cứu quốc tế đã sử dụng dữ liệu lịch sử của BTS [9] và FAA [10] để dự báo trễ chuyến bay bằng các mô hình học máy truyền thống như Hồi quy Logistic, Support Vector Machine (SVM) và Mạng nơ-ron nhân tạo. Phần lớn các công trình này chủ yếu tập trung vào phân loại nhị phân (trễ hoặc đúng giờ) hoặc dự báo thời lượng trễ (delay duration) trên các tập mẫu rút gọn [24].
* Rất ít nghiên cứu giải quyết bài toán phân loại đa lớp xác định nguyên nhân trễ chi phối (Dominant Delay Cause) giữa 5 nhóm nguyên nhân thực tế của BTS trên không gian dữ liệu lớn toàn vẹn hàng năm.
* Tại Việt Nam, việc ứng dụng hệ sinh thái Hadoop [4], [5], [13] và Spark [2], [11], [12] để giải quyết bài toán phân loại đa lớp trên dữ liệu vận tải thực tế quy mô hơn 7 triệu dòng của năm 2024 còn là một hướng đi mới mẻ và giàu tiềm năng thực tiễn.

### 1.2.2. Định hướng thực hiện và tính mới của đề tài
Khắc phục những khoảng trống nghiên cứu trên, đề tài này lựa chọn hướng tiếp cận thực chứng, toàn diện và đột phá về mặt công nghệ:
1. **Xử lý toàn vẹn dữ liệu quy mô lớn (Full-scale Big Data):** Không sử dụng kỹ thuật lấy mẫu thu nhỏ; đề tài tiếp nhận, làm sạch và xử lý trọn vẹn toàn bộ **7,079,081 chuyến bay** của năm 2024.
2. **Giải quyết bài toán phân loại đa lớp 5 nguyên nhân thực tế:** Xây dựng biến mục tiêu phân loại 5 lớp dựa trên cơ chế xác định nguyên nhân chi phối lớn nhất (Dominant Delay Cause), phản ánh chính xác bản chất vận hành bay.
3. **Tiếp cận Pipeline hai nhánh đối sánh song song (Dual-Branch Pipeline):**
   * *Nhánh Đơn máy tối ưu (Single-node Baseline):* Ứng dụng Polars streaming và thư viện Scikit-learn, kết hợp GPU để huấn luyện các thuật toán Boosting tiên tiến nhất hiện nay (XGBoost, LightGBM, CatBoost).
   * *Nhánh Phân tán (Distributed Spark MLlib):* Xây dựng pipeline dữ liệu phân tán trên Apache Spark, tối ưu hóa lưu trữ cột Parquet Snappy phân vùng theo tháng, và triển khai thuật toán Random Forest phân tán.
4. **Hiện thực hóa Cụm tính toán phân tán vật lý (Physical Multi-node Cluster):** Không chỉ dừng lại ở việc giả lập cụm trên một máy ảo (Pseudo-distributed mode), nhóm đã kết nối và cấu hình thành công một **cụm 3 máy tính vật lý** thực tế thông qua giải pháp mạng riêng ảo **Tailscale Mesh VPN**, phân phối các tác vụ tính toán Spark qua Driver và các Executor độc lập.
5. **Đóng gói giải pháp Serving thực tế (`Web_Air`):** Xây dựng ứng dụng web hiện đại (FastAPI + Modern Web Dashboard) cung cấp giao diện dự đoán thời gian thực đối đầu giữa các mô hình và hiển thị trực quan 24 biểu đồ phân tích khoa học.

---

## 1.3. Mục tiêu nghiên cứu

### 1.3.1. Mục tiêu tổng quát
Nghiên cứu, thiết kế và hiện thực hóa một hệ thống xử lý dữ liệu lớn toàn diện từ khâu thu thập, tiền xử lý, lưu trữ phân tán, huấn luyện mô hình học máy phân tán đến triển khai ứng dụng web phục vụ dự đoán nguyên nhân trễ chuyến bay thương mại nội địa Hoa Kỳ năm 2024, từ đó đánh giá sâu sắc hiệu năng giữa kiến trúc đơn máy và kiến trúc phân tán.

### 1.3.2. Mục tiêu cụ thể
1. **Làm chủ quy trình tiền xử lý và lưu trữ dữ liệu lớn:**
   * Làm sạch, xử lý dữ liệu khuyết thiếu, phát hiện và loại bỏ triệt để các thuộc tính gây rò rỉ dữ liệu (Data Leakage) trên hơn 7 triệu bản ghi.
   * Chuyển đổi và tối ưu hóa định dạng lưu trữ từ CSV truyền thống sang Apache Parquet nén Snappy và phân vùng theo tháng (`Month`), giảm thiểu đáng kể dung lượng đĩa và tăng tốc độ đọc I/O.
2. **Xây dựng bài toán gán nhãn đa lớp khoa học:**
   * Thiết lập logic xác định 5 nguyên nhân trễ chuyến bay chuẩn xác theo tiêu chuẩn thống kê của Cục Thống kê Vận tải Hoa Kỳ (BTS).
3. **Triển khai hạ tầng phân tán trên cụm máy tính vật lý:**
   * Thiết lập và cấu hình cụm tính toán Apache Spark trên 3 nút máy tính độc lập liên kết thông qua Tailscale Mesh VPN.
   * Tối ưu hóa các tham số cấp phát tài nguyên của Spark (Cores, Memory, Partitions, Kryo Serializer).
4. **Huấn luyện và đối sánh toàn diện 7 mô hình học máy:**
   * Huấn luyện mô hình Random Forest phân tán trên Spark MLlib.
   * Huấn luyện 6 mô hình đối chứng trên đơn máy (Logistic Regression, Decision Tree, Random Forest Scikit-learn, Extra Trees, XGBoost GPU, LightGBM, CatBoost GPU).
   * Đo lường chi tiết các chỉ số: Accuracy, Weighted/Macro F1-Score, thời gian huấn luyện, thông lượng suy luận (Inference Throughput) và mức độ tiêu thụ RAM.
5. **Xây dựng ứng dụng phục vụ mô hình (Serving Application) hoàn chỉnh:**
   * Phát triển ứng dụng Web Python `Web_Air` trên nền tảng FastAPI hỗ trợ suy luận thời gian thực với độ trễ thấp (<50ms).
   * Xây dựng Dashboard trực quan hóa gồm 3 tab chức năng tương tác cao.

---

## 1.4. Đối tượng và phạm vi nghiên cứu

### 1.4.1. Đối tượng nghiên cứu
* Đối tượng nghiên cứu cốt lõi: Các chuyến bay thương mại nội địa tại Hoa Kỳ ghi nhận hiện tượng trễ giờ khởi hành hoặc trễ giờ đến từ 15 phút trở lên trong năm 2024.
* Đối tượng công nghệ:
  * Khung tính toán phân tán Apache Spark và thư viện học máy Spark MLlib.
  * Thuật toán phân loại rừng ngẫu nhiên (Random Forest) và cơ chế phân tán cây quyết định.
  * Các thuật toán học máy đối chứng (Logistic Regression, Decision Tree, Extra Trees, XGBoost, LightGBM, CatBoost).
  * Định dạng lưu trữ tối ưu hóa Apache Parquet.

### 1.4.2. Phạm vi dữ liệu
* **Nguồn dữ liệu:** Dữ liệu mở chính thức từ Cục Thống kê Vận tải Hoa Kỳ (Bureau of Transportation Statistics - BTS) được tổng hợp trong bộ *Flight Delay Dataset 2024*.
* **Thời gian dữ liệu:** Toàn bộ 12 tháng của năm 2024 (từ tháng 01/2024 đến hết tháng 12/2024).
* **Quy mô dữ liệu:** Gồm chính xác **7,079,081 dòng** và **35 thuộc tính** đo lường chi tiết quá trình khai thác bay của tất cả các hãng hàng không thương mại nội địa Hoa Kỳ.

### 1.4.3. Phạm vi công nghệ
* **Hệ điều hành:** Môi trường hỗn hợp Windows 11 và Linux.
* **Ngôn ngữ lập trình:** Python 3.10+, PySpark 3.5.x.
* **Hạ tầng phân tán:** Cụm 3 máy tính xách tay kết nối qua giao thức Tailscale Mesh VPN.
* **Công nghệ ứng dụng:** Nền tảng FastAPI, chuẩn giao tiếp RESTful JSON, giao diện hiện đại HTML5/CSS3/JavaScript (không dùng thư viện ngoài nặng nề).

---

## 1.5. Nội dung và phương pháp nghiên cứu

### 1.5.1. Phương pháp nghiên cứu
Đồ án tuân thủ nghiêm ngặt quy trình chuẩn công nghiệp trong khoa học dữ liệu **CRISP-DM (Cross-Industry Standard Process for Data Mining)**, bao gồm 6 giai đoạn:
1. **Thấu hiểu nghiệp vụ (Business Understanding):** Nghiên cứu quy định khai thác bay của FAA/BTS, cấu trúc chi phí và ảnh hưởng vận hành của từng loại trễ chuyến bay.
2. **Thấu hiểu dữ liệu (Data Understanding):** Tiến hành phân tích khám phá dữ liệu (EDA) trên tập dữ liệu gốc 7 triệu bản ghi, khảo sát phân bố dữ liệu thiếu, phân tích tương quan và đánh giá tỷ lệ mất cân bằng nhãn.
3. **Chuẩn bị dữ liệu (Data Preparation):** Lọc dữ liệu hợp lệ, xây dựng nhãn mục tiêu 5 lớp, mã hóa chuỗi (String Indexing), chuẩn hóa véc-tơ thuộc tính và loại trừ các biến gây rò rỉ thông tin.
4. **Xây dựng mô hình (Modeling):** Thiết kế Pipeline trên Spark MLlib và Scikit-learn, cấu hình siêu tham số và huấn luyện 7 thuật toán học máy.
5. **Đánh giá mô hình (Evaluation):** Đánh giá khách quan trên tập kiểm tra độc lập (15% dữ liệu tương đương hơn 1 triệu dòng) qua các độ đo thống kê (Accuracy, Precision, Recall, Macro/Weighted F1-score, Confusion Matrix) và các chỉ số đo đạc hiệu năng tính toán (CPU, RAM, Thời gian thực thi).
6. **Triển khai ứng dụng (Deployment):** Đóng gói mô hình thành dịch vụ API, xây dựng dashboard web phục vụ người dùng cuối.

### 1.5.2. Nội dung nghiên cứu chính
* Khảo sát lý thuyết về Big Data, kiến trúc Hadoop HDFS và Apache Spark.
* Thu thập, kiểm tra tính toàn vẹn và phân tích EDA bộ dữ liệu BTS 2024.
* Chuyển đổi định dạng lưu trữ CSV sang Parquet Snappy phân vùng tháng.
* Cấu hình và thiết lập cụm Spark 3 node vật lý qua mạng Tailscale.
* Xây dựng Spark ML Pipeline và huấn luyện Random Forest phân tán.
* Thực nghiệm đối sánh hiệu năng giữa Spark MLlib và 6 mô hình đơn máy.
* Phát triển ứng dụng Web `Web_Air` với 3 phân hệ chức năng tương tác.
* Tổng kết, đánh giá ưu nhược điểm và vạch ra định hướng nghiên cứu mở rộng.

---

## 1.6. Bố cục đồ án

Báo cáo đồ án được kết cấu thành 7 chương nội dung chính cùng các phần bổ trợ, được tổ chức mạch lạc như sau:

* **LỜI MỞ ĐẦU**
* **CHƯƠNG 1: TỔNG QUAN ĐỀ TÀI** — Trình bày bối cảnh thực tế, tính cấp thiết, mục tiêu, đối tượng, phạm vi và phương pháp nghiên cứu của đề tài.
* **CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ CÔNG NGHỆ LIÊN QUAN** — Hệ thống hóa các kiến thức nền tảng về Big Data, hệ thống tệp phân tán HDFS, khung tính toán Apache Spark, lý thuyết thuật toán Random Forest phân tán, các mô hình Boosting và định dạng lưu trữ cột Parquet.
* **CHƯƠNG 3: PHÂN TÍCH DỮ LIỆU VÀ TIỀN XỬ LÝ DỮ LIỆU LỚN** — Khảo sát chi tiết tập dữ liệu BTS 2024, phân tích khám phá EDA, phương pháp gán nhãn 5 nguyên nhân trễ, quy trình tối ưu lưu trữ Parquet Snappy và kỹ thuật chống rò rỉ dữ liệu.
* **CHƯƠNG 4: THIẾT KẾ HỆ THỐNG VÀ XÂY DỰNG MÔ HÌNH PHÂN TÁN** — Mô tả kiến trúc hệ thống 2 nhánh, cách thức thiết lập cụm Spark 3 node vật lý qua Tailscale Mesh VPN, thiết kế Spark ML Pipeline và quy trình huấn luyện Random Forest phân tán.
* **CHƯƠNG 5: THỰC NGHIỆM VÀ ĐÁNH GIÁ KẾT QUẢ** — Trình bày môi trường thực nghiệm, phân tích bảng đối sánh toàn diện 7 mô hình trên 7 triệu dòng dữ liệu, bóc tách thời gian thực thi theo 4 giai đoạn pipeline, phân tích ma trận nhầm lẫn và tầm quan trọng đặc trưng qua 24 biểu đồ khoa học.
* **CHƯƠNG 6: XÂY DỰNG HỆ THỐNG PHỤC VỤ MÔ HÌNH VÀ DEMO (WEB_AIR)** — Trình bày kiến trúc phần mềm, thiết kế RESTful API bằng FastAPI và chi tiết 3 tab chức năng của ứng dụng Web Dashboard.
* **CHƯƠNG 7: KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN** — Tổng kết các đóng góp chính của đồ án, chỉ ra những hạn chế kỹ thuật hiện tại và vạch ra hướng phát triển mở rộng trong tương lai.
* **TÀI LIỆU THAM KHẢO**
