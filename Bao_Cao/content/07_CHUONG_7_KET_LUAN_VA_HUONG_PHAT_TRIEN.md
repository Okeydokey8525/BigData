# CHƯƠNG 7: KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN

Chương này tổng kết toàn bộ các kết quả và đóng góp cốt lõi mà đề tài đã đạt được, phân tích khách quan các khó khăn và hạn chế kỹ thuật còn tồn tại, đồng thời vạch ra các định hướng nghiên cứu mở rộng trong tương lai nhằm hoàn thiện hơn nữa hệ thống phân tích và dự báo trong ngành hàng không thương mại.

---

## 7.1. Các kết quả và đóng góp chính của đồ án

Sau quá trình nghiên cứu lý thuyết, thiết kế hệ thống và tiến hành thực nghiệm thực tế, đề tài **"Ứng dụng thuật toán Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay thương mại: Trường hợp tại Hoa Kỳ năm 2024"** đã hoàn thành xuất sắc toàn bộ các mục tiêu đặt ra, đạt được các kết quả nổi bật sau:

### 1. Về mặt kỹ thuật dữ liệu lớn (Big Data Engineering)
* **Xử lý toàn vẹn dữ liệu quy mô lớn (Full-scale 7M Records):** Tiếp nhận, kiểm tra tính toàn vẹn và làm sạch thành công toàn bộ **7,079,081 chuyến bay** nội địa Hoa Kỳ năm 2024, không cần phải dùng kỹ thuật lấy mẫu thu nhỏ, đảm bảo tính đại diện và giá trị thực chứng cao nhất.
* **Tối ưu hóa lưu trữ phân tán vượt trội:** Ứng dụng thành công định dạng hướng cột **Apache Parquet nén Snappy phân vùng theo 12 tháng (`partitionBy("Month")`)**, giúp giảm dung lượng lưu trữ trên đĩa tới **82.5%** (từ 2.3 GB dạng CSV xuống còn 409.8 MB) và tăng tốc độ đọc dữ liệu ETL lên tới **10.97 lần** (từ 43.88 giây xuống chỉ còn 4.00 giây).
* **Quy trình chuẩn hóa đặc trưng nói không với rò rỉ dữ liệu:** Xây dựng thành công bài toán phân loại đa lớp 5 nhóm nguyên nhân trễ dựa trên nguyên tắc *Nguyên nhân chi phối lớn nhất (Dominant Cause)*; loại bỏ triệt để các biến rò rỉ dữ liệu (Data Leakage) để đảm bảo mô hình có khả năng dự đoán thực tế trước giờ bay.

### 2. Về mặt hạ tầng tính toán phân tán (Distributed Computing)
* **Hiện thực hóa Cụm Spark 3 Nút vật lý qua Tailscale Mesh VPN:** Vượt qua rào cản mạng gia đình và tường lửa, nhóm đã kết nối thành công 3 máy tính xách tay cá nhân thành một cụm tính toán phân tán thực tế với tổng tài nguyên **32 vCPUs và 40 GB RAM**.
* **Tối ưu hóa siêu tham số Spark:** Cấu hình chuẩn xác các tham số bộ nhớ, số lượng phân vùng Shuffle và bộ tuần tự hóa Kryo (`KryoSerializer`), đảm bảo hệ thống vận hành liên tục không xảy ra lỗi tràn bộ nhớ (OOM) hay nghẽn mạng trong suốt quá trình xử lý 7 triệu bản ghi.

### 3. Về mặt học máy và phân tích thực nghiệm (Machine Learning & Analytics)
* **Khẳng định tính ưu việt của Spark MLlib Random Forest:**
  * Thời gian huấn luyện trên gần 5 triệu dòng của Random Forest phân tán chỉ mất **44.31 giây**, **nhanh hơn 2.41 lần** so với mô hình Random Forest đơn máy (106.79 giây).
  * Tiết kiệm hơn **405 MB RAM** trên máy điều khiển trung tâm (Master Node) nhờ cơ chế san sẻ dữ liệu sang các Worker.
  * Tốc độ suy luận đạt mức cực cao: **1.98 ms trên 1,000 mẫu** (nhanh hơn gần 17 lần so với đơn máy).
  * Đạt chỉ số **Macro F1-Score lên tới 28.25%** (cao gần gấp đôi so với các mô hình đơn máy), duy trì độ nhạy tốt hơn ở các lớp trễ thiểu số.
* **Tổng thời gian thực thi toàn bộ quy trình (End-to-End Pipeline) của Spark chỉ mất 65.24 giây**, **nhanh hơn 2.38 lần** so với toàn bộ pipeline đơn máy (154.97 giây).
* **Xây dựng kho tàng minh chứng học thuật gồm 24 biểu đồ khoa học** phân tích chuyên sâu về phân bố dữ liệu, ma trận nhầm lẫn và tầm quan trọng đặc trưng.

### 4. Về mặt ứng dụng thực tiễn (Serving Application)
* Xây dựng và triển khai thành công ứng dụng **`Web_Air`** trên nền tảng **FastAPI**, tích hợp giao diện điều khiển 3 Tab hiện đại.
* Đạt độ trễ suy luận tức thời **<40 ms / request** khi thực hiện đối sánh đồng thời 3 mô hình học máy, sẵn sàng tích hợp vào các hệ thống vận hành bay thực tế.

---

## 7.2. Những khó khăn và hạn chế của đề tài

Bên cạnh những thành tựu đạt được, đề tài còn tồn tại một số hạn chế khách quan cần được ghi nhận:
1. **Nút thắt cổ chai về băng thông mạng Internet (Network Bandwidth Bottleneck):**
   * Do cụm 3 nút được thiết lập trên các máy tính cá nhân kết nối qua mạng VPN Internet gia đình, tốc độ truyền dữ liệu trong giai đoạn xáo trộn dữ liệu (Shuffle Stage) bị phụ thuộc vào đường truyền mạng công cộng, chưa thể đạt được tốc độ truyền tải cực cao như mạng cáp quang nội bộ 10 Gbps/InfiniBand trong các Trung tâm dữ liệu (Data Center) chuyên nghiệp.
2. **Thách thức mất cân bằng lớp cực đoan (Extreme Class Imbalance):**
   * Trong khi hai lớp `LateAircraftDelay` và `CarrierDelay` chiếm tới gần 70% tổng số mẫu trễ, thì lớp `SecurityDelay` chỉ chiếm chưa đầy 0.4%. Mặc dù Spark MLlib đã cải thiện đáng kể chỉ số Macro F1-Score so với đơn máy, nhưng việc dự đoán chính xác tuyệt đối các ca trễ an ninh hy hữu vẫn là một bài toán hóc búa đối với bất kỳ thuật toán học máy nào nếu không có các kỹ thuật tái lấy mẫu đặc thù.
3. **Phạm vi thuộc tính giới hạn theo lịch trình:**
   * Để chống rò rỉ dữ liệu, mô hình chỉ sử dụng các thuộc tính có thể biết trước giờ bay. Việc chưa tích hợp được dữ liệu khí tượng radar thời gian thực (METAR/TAF) hoặc thông tin tình trạng bảo dưỡng chi tiết của từng số hiệu tàu bay cụ thể (Tail Number) khiến mô hình chưa thể giải thích trọn vẹn 100% các biến động trễ ngẫu nhiên phát sinh ngay tại cửa khởi hành.

---

## 7.3. Hướng phát triển trong tương lai

Từ các kết quả và hạn chế đã chỉ ra, nhóm đề xuất các hướng nghiên cứu mở rộng trong giai đoạn tiếp theo:
1. **Mở rộng cụm tính toán lên nền tảng Điện toán đám mây (Cloud Dataproc / AWS EMR):**
   * Triển khai hệ sinh thái Spark trên dịch vụ Google Cloud Dataproc hoặc Amazon EMR với cấu hình cụm tự động co giãn (Auto-scaling) từ 10 đến 50 nút tính toán, khai thác tối đa sức mạnh phân tán trên quy mô hàng trăm triệu dòng dữ liệu bay qua nhiều năm.
2. **Tích hợp dữ liệu thời tiết thời gian thực (NOAA Weather API):**
   * Xây dựng module tự động thu thập và gắn kết (Join) tọa độ sân bay với dữ liệu thời tiết trực tiếp từ Cơ quan Khí quyển và Đại dương Quốc gia Hoa Kỳ (NOAA), bao gồm: tốc độ gió, tầm nhìn, nhiệt độ, áp suất khí quyển và cảnh báo giông bão tại thời điểm trước giờ bay 2 tiếng.
3. **Xây dựng kiến trúc xử lý luồng thời gian thực (Real-time Streaming Pipeline):**
   * Kết hợp công nghệ hàng đợi thông điệp phân tán **Apache Kafka** với **Spark Structured Streaming** để tiếp nhận dòng dữ liệu trạng thái máy bay trực tiếp, cho phép hệ thống cập nhật lại dự báo nguyên nhân trễ liên tục theo từng phút khi máy bay đang trong quá trình lăn bánh ra đường băng (Taxi-out).
4. **Nghiên cứu các thuật toán cân bằng lớp phân tán chuyên sâu:**
   * Ứng dụng kỹ thuật phân bổ trọng số lớp động (Class-weighted Loss) hoặc thuật toán tạo mẫu thiểu số tổng hợp phân tán (Distributed SMOTE on Spark) nhằm nâng cao hơn nữa độ nhạy đối với các nguyên nhân trễ do an ninh và thời tiết cực đoan.

---

## 7.4. Tổng kết toàn diện đồ án

Đồ án **"Ứng dụng thuật toán Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay thương mại: Trường hợp tại Hoa Kỳ năm 2024"** đã chứng minh một cách thuyết phục rằng: **khi quy mô dữ liệu vượt qua ngưỡng vài triệu bản ghi, việc chuyển đổi sang kiến trúc tính toán phân tán Apache Spark kết hợp định dạng lưu trữ cột Apache Parquet không chỉ là một sự lựa chọn công nghệ, mà là một yêu cầu tất yếu** để đảm bảo tính ổn định, tốc độ xử lý vượt trội và khả năng mở rộng bền vững của hệ thống.

Kết quả nghiên cứu của đề tài không chỉ mang ý nghĩa học thuật sâu sắc trong việc hoàn thành chương trình môn học Nhập môn Big Data tại Trường Đại học Công Thương Thành phố Hồ Chí Minh, mà còn là một minh chứng thực tế về tính khả thi của việc ứng dụng công nghệ dữ liệu lớn để giải quyết các bài toán hóc búa, có giá trị kinh tế cao trong ngành giao thông hàng không hiện đại.
