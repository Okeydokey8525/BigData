# KẾ HOẠCH THỰC HIỆN VÀ THEO DÕI TIẾN ĐỘ (ROADMAP & TASKS)

> Theo lộ trình 11 tuần đã thống nhất trong Đề cương đồ án môn học.

---

## 1. TIẾN ĐỘ 11 TUẦN (11-WEEK ROADMAP)

| Tuần | Nội dung công việc theo đề cương | Trạng thái | Đầu ra dự kiến / Ghi chú |
| :---: | :--- | :---: | :--- |
| **Tuần 1** | Khảo sát bài toán trễ chuyến bay hàng không, nghiên cứu lý thuyết hệ sinh thái Hadoop, Spark và quy định đồ án. | **HOÀN THÀNH** | Đề cương chi tiết [`Nhom6_T4_C10-12_BaoCao.docx`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_T4_C10-12_BaoCao.docx) |
| **Tuần 2** | Thu thập và thẩm định bộ dữ liệu `Flight Delay Dataset 2024`; Thiết lập môi trường dự án. | **HOÀN THÀNH** | Đã tải tập dữ liệu 1.3 GB vào [`Flight Delay Dataset — 2024/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024) |
| **Tuần 3** | Cấu hình Apache Hadoop HDFS; Viết kịch bản tự động nạp dữ liệu lớn vào hệ thống tệp phân tán. | **HOÀN THÀNH** | Docker Compose cho cụm Hadoop/Spark; Hướng dẫn cụm 3 laptop qua Tailscale (`doc/14`) |
| **Tuần 4** | Phát triển pipeline tiền xử lý dữ liệu bằng Apache Spark (PySpark): lọc dữ liệu nhiễu, xử lý khuyết thiếu, chuẩn hóa kiểu và chuyển đổi sang Parquet. | **HOÀN THÀNH** | Hoàn tất ETL Streaming Chunking 7.07M CSV $\to$ 6.96M Parquet phân vùng 12 tháng (`cleaned_flight_data_2024.parquet`) |
| **Tuần 5** | Triển khai kho dữ liệu Apache Hive trên nền HDFS; Thực hiện các truy vấn phân tích tổng hợp (HiveQL). | **HOÀN THÀNH** | Định nghĩa cấu trúc lưu trữ và tối ưu hóa truy vấn trên Parquet/Hive schema |
| **Tuần 6** | Kỹ thuật trích xuất đặc trưng phân tán trên PySpark phục vụ bài toán Machine Learning (StringIndexer, VectorAssembler, Binning). | **HOÀN THÀNH** | Module `src/spark_ml/pipeline.py` và chuẩn hóa 13 đặc trưng thời gian (`dep_min_of_day`, `arr_min_of_day`) |
| **Tuần 7** | Huấn luyện và đánh giá các mô hình Machine Learning phân tán (Spark ML): Random Forest (chính), Decision Tree, Logistic Regression. | **HOÀN THÀNH** | Huấn luyện thành công Spark MLlib RF trên 7M dòng (`models/spark/`) và 6 mô hình CPU/GPU (`models/baseline/`) |
| **Tuần 8** | Đánh giá hiệu năng mô hình (Accuracy, F1, Confusion Matrix, Training Time); Trích xuất kết quả dự báo và nạp vào MongoDB. | **HOÀN THÀNH** | Xuất bản trọn bộ 24 biểu đồ khoa học và bộ số liệu CSV/JSON đối sánh 7M (`results/baseline/`) |
| **Tuần 9** | Xây dựng RESTful API Backend bằng FastAPI để phục vụ truy vấn dữ liệu phân tích và endpoint dự đoán từ mô hình. | **HOÀN THÀNH** | Ứng dụng Backend FastAPI phục vụ đầy đủ 9 API endpoints trong [`Web_Air/app.py`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Web_Air/app.py) |
| **Tuần 10** | Thiết kế giao diện Dashboard trực quan hóa biểu đồ phân tích và kiểm thử tích hợp toàn bộ hệ thống. | **HOÀN THÀNH** | Giao diện Web_Air Dashboard 3 Tab (Dự đoán, Đối sánh 7M, Thư viện biểu đồ) kèm kịch bản 1-click `run.bat` |
| **Tuần 11** | Hoàn thiện tài liệu báo cáo học phần (7 chương), đóng gói mã nguồn, thiết kế slide trình chiếu và chuẩn bị kịch bản demo. | **ĐANG THỰC HIỆN** | File báo cáo hoàn chỉnh trong [`Bao_Cao/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao) và kịch bản demo bảo vệ |

---

## 2. NHIỆM VỤ ƯU TIÊN TIẾP THEO (NEXT IMMEDIATE STEPS CHO AI AGENTS)

Khi một AI Agent tiếp nhận công việc tiếp theo từ người dùng, hãy ưu tiên các nhiệm vụ sau theo thứ tự:

1. **Đồng bộ hóa mã nguồn lên GitHub:**
   * Commit toàn bộ mã nguồn `Web_Air/`, tài liệu cập nhật và đẩy lên nhánh `main` (`git push origin main`).
2. **Hoàn thiện bản thảo Báo cáo tổng kết 7 chương:**
   * Tích hợp bảng số liệu Grand Comparison 7M và các biểu đồ khoa học (Radar Chart, Bubble Plot, Confusion Matrix, Feature Importance) vào tệp báo cáo trong `Bao_Cao/`.
3. **Chuẩn bị kịch bản Demo trực tiếp cho Giảng viên:**
   * Sử dụng ứng dụng [`Web_Air/run.bat`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Web_Air/run.bat) để trình diễn suy luận thời gian thực và so sánh đối kháng 6 mô hình.
