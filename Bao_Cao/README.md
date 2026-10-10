# HỆ THỐNG TÀI LIỆU BÁO CÁO ĐỒ ÁN MÔN HỌC BIG DATA (MARKDOWN TO WORD)

> **Môn học:** Nhập môn Big Data (Học kỳ VII) - Trường Đại học Công Thương TP. Hồ Chí Minh (HUIT)  
> **Đề tài:** Ứng dụng thuật toán Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay thương mại: Trường hợp tại Hoa Kỳ năm 2024  
> **GVHD:** TS. Phan Hồ Viết Trường  
> **Nhóm thực hiện:** Nhóm 6 (Cù Văn Vĩ An, Lê Đức Lương - Trưởng nhóm, Trần Huỳnh Tuấn Anh)  

---

## 1. Cấu trúc các tập tin Markdown trong thư mục `content/`

Toàn bộ nội dung báo cáo được phân rã thành **9 module Markdown độc lập**, bám sát 100% khung đề cương 7 chương của HUIT và tích hợp toàn bộ các kết quả thực nghiệm thực tế từ cụm 3 laptop Tailscale Mesh VPN và 7 mô hình học máy:

| Thứ tự | Tên tập tin Markdown | Nội dung chi tiết |
| :---: | :--- | :--- |
| **00** | [`00_TRANG_BIA_LỜI_CAM_ĐOAN_MUC_LUC.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/00_TRANG_BIA_L%E1%BB%9CI_CAM_%C4%90OAN_MUC_LUC.md) | Trang bìa, Lời cam đoan, Lời cảm ơn, Mục lục tổng quát và Danh mục 21 từ viết tắt chuyên ngành. |
| **01** | [`01_CHUONG_1_TONG_QUAN_DE_TAI.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/01_CHUONG_1_TONG_QUAN_DE_TAI.md) | **Chương 1: Tổng quan đề tài**<br>Bối cảnh hàng không Hoa Kỳ 2024, tính cấp thiết của 5 nguyên nhân trễ, 4V Big Data, quy trình chuẩn CRISP-DM và bố cục đề án. |
| **02** | [`02_CHUONG_2_CO_SO_LY_THUYET.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/02_CHUONG_2_CO_SO_LY_THUYET.md) | **Chương 2: Cơ sở lý thuyết và Công nghệ liên quan**<br>Hadoop HDFS, Spark DAG, cơ chế In-memory, công thức toán học Gini, Bagging, cơ chế phân tán của Random Forest trên Spark MLlib, các mô hình Boosting và chỉ số F1 đa lớp. |
| **03** | [`03_CHUONG_3_PHAN_TICH_DU_LIEU_VA_TIEN_XU_LY.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/03_CHUONG_3_PHAN_TICH_DU_LIEU_VA_TIEN_XU_LY.md) | **Chương 3: Phân tích dữ liệu và Tiền xử lý dữ liệu lớn**<br>Từ điển 35 thuộc tính BTS 2024 (7.08 triệu dòng), so sánh CSV vs Parquet Snappy giảm 82.5% dung lượng, quy tắc gán nhãn Dominant Cause và kỹ thuật chống rò rỉ dữ liệu (Data Leakage). |
| **04** | [`04_CHUONG_4_XAY_DUNG_MO_HINH_ML_PHAN_TAN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/04_CHUONG_4_XAY_DUNG_MO_HINH_ML_PHAN_TAN.md) | **Chương 4: Thiết kế hệ thống và Xây dựng mô hình phân tán**<br>Kiến trúc 2 nhánh song song (Đơn máy vs Phân tán), sơ đồ cụm 3 laptop vật lý kết nối qua Tailscale Mesh VPN (32 vCPU, 40GB RAM), Spark Performance Tuning. |
| **05** | [`05_CHUONG_5_THUC_NGHIEM_VA_DANH_GIA.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/05_CHUONG_5_THUC_NGHIEM_VA_DANH_GIA.md) | **Chương 5: Thực nghiệm và Đánh giá kết quả (Trọng tâm)**<br>Bảng đối sánh toàn diện 7 mô hình từ `grand_model_comparison_7m.csv`, bóc tách 4 giai đoạn pipeline (Spark nhanh hơn 2.38x), phân tích ma trận nhầm lẫn và tầm quan trọng đặc trưng qua 24 biểu đồ khoa học. |
| **06** | [`06_CHUONG_6_XAY_DUNG_HE_THONG_TRUC_QUAN_HOA_DEMO.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/06_CHUONG_6_XAY_DUNG_HE_THONG_TRUC_QUAN_HOA_DEMO.md) | **Chương 6: Xây dựng hệ thống phục vụ mô hình và Demo (Web_Air)**<br>Kiến trúc RESTful API bằng FastAPI, thiết kế 3 Tab chức năng (Live Prediction, Grand Benchmark, Visuals Gallery), kiểm thử độ trễ suy luận <40ms. |
| **07** | [`07_CHUONG_7_KET_LUAN_VA_HUONG_PHAT_TRIEN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/07_CHUONG_7_KET_LUAN_VA_HUONG_PHAT_TRIEN.md) | **Chương 7: Kết luận và Hướng phát triển**<br>Tổng kết 4 thành tựu lớn, 3 khó khăn khách quan (băng thông VPN, mất cân bằng lớp), 4 định hướng mở rộng (Cloud Dataproc, NOAA Weather, Kafka Streaming). |
| **08** | [`08_TAI_LIEU_THAM_KHAO.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/content/08_TAI_LIEU_THAM_KHAO.md) | **Tài liệu tham khảo**<br>Danh mục 18 bài báo khoa học quốc tế, tài liệu chính thức của BTS, FAA, Apache Spark và các công trình nền tảng của Leo Breiman, Matei Zaharia. |

---

## 2. Hướng dẫn sử dụng để chuyển đổi sang Microsoft Word (.docx)

1. **Cách 1: Sao chép thủ công (Khuyến nghị cho kiểm soát định dạng tối đa):**
   * Mở file Word mẫu [`conference-template-a4.docx`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao/conference-template-a4.docx) hoặc tạo file Word mới với định dạng Times New Roman, Font size 13, Line spacing 1.3 - 1.5, Margin 2 - 2 - 2.5 - 2 cm theo chuẩn HUIT.
   * Sao chép nội dung từng chương từ Markdown vào Word.
   * Các bảng biểu trong Markdown đều đã được định dạng chuẩn cột, Word sẽ tự động chuyển thành bảng Word đẹp mắt.
   * Chèn các ảnh biểu đồ tương ứng từ thư mục [`results/baseline/figures/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/results/baseline/figures/) vào đúng vị trí chú thích hình ảnh trong Chương 5.

2. **Cách 2: Sử dụng công cụ Pandoc tự động (Nhanh và giữ nguyên cấu trúc Heading):**
   Nếu máy tính đã cài đặt công cụ `pandoc`, bạn có thể gộp tất cả các file markdown thành 1 file duy nhất hoặc xuất thẳng sang file Word bằng lệnh PowerShell:
   ```powershell
   cd 'c:\LeDucLuong\HK VII\NhapMonBigData\DoAn\Bao_Cao'
   Get-Content content\*.md | Out-File -Encoding utf8 BaoCao_Full.md
   pandoc BaoCao_Full.md -o BaoCao_Nhom6_BigData_2026.docx --reference-doc=conference-template-a4.docx
   ```
