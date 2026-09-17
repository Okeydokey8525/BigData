# TỔNG QUAN DỰ ÁN & HƯỚNG DẪN ĐIỀU PHỐI (AI AGENT CONTEXT)

> **Tài liệu dành cho các AI Agent và thành viên nhóm phát triển.**  
> Vui lòng đọc kỹ tài liệu này trước khi thực hiện bất kỳ tác vụ đọc/ghi dữ liệu, sinh mã nguồn hoặc huấn luyện mô hình.

---

## 1. THÔNG TIN CHUNG ĐỀ TÀI

* **Đề tài:** **ỨNG DỤNG THUẬT TOÁN RANDOM FOREST PHÂN TÁN DỰ ĐOÁN NGUYÊN NHÂN TRỄ CHUYẾN BAY THƯƠNG MẠI: TRƯỜNG HỢP TẠI HOA KỲ NĂM 2024**
* **Học phần:** Nhập môn Big Data (HK VII)
* **Đơn vị:** Khoa Công nghệ Thông tin – Trường Đại học Công Thương TP. Hồ Chí Minh (HUIT)
* **Giảng viên hướng dẫn:** TS. Phan Hồ Viết Trường
* **Nhóm thực hiện:** Nhóm 6 (Thứ 4, Ca 10 - 12)
  1. **Cù Văn Vĩ An** - MSSV: `2001230005`
  2. **Lê Đức Lương** - MSSV: `2001230490`
  3. **Trần Huỳnh Tuấn Anh** - MSSV: `2001230023`

---

## 2. HIỆN TRẠNG CÁC THƯ MỤC TRONG DỰ ÁN (`WORKSPACE STATUS`)

Cấu trúc thư mục gốc: `c:\LeDucLuong\HK VII\NhapMonBigData\DoAn\`

| Tên Thư mục / Tệp tin | Trạng thái hiện tại | Mô tả & Nhiệm vụ của thư mục |
| :--- | :---: | :--- |
| [`Flight Delay Dataset — 2024/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Flight%20Delay%20Dataset%20%E2%80%94%202024) | **Sẵn sàng (Data Ready)** | Chứa toàn bộ dữ liệu thô tải từ Kaggle (~1.31 GB):<br>• `flight_data_2024.csv`: 7,079,081 dòng, 35 cột.<br>• `flight_data_2024_sample.csv`: 10,000 dòng mẫu để dev/test.<br>• `flight_data_2024_data_dictionary.csv`: metadata từ điển dữ liệu. |
| [`doc/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc) | **Đang cập nhật (Active)** | Trung tâm tài liệu hóa cho dự án và bộ nhớ ngữ cảnh cho các AI Agent (Dataset Spec, Architecture, Roadmap, AI Guidelines). |
| [`Bao_Cao/`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Bao_Cao) | **Khởi tạo (Initialized)** | Thư mục chứa các tài liệu báo cáo chính thức, slide thuyết trình, bản thảo báo cáo nghiệm thu 7 chương của đồ án. |
| [`Nhom6_T4_C10-12_BaoCao.docx`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/Nhom6_T4_C10-12_BaoCao.docx) | **Đã hoàn thành đề cương** | File Word đề cương chi tiết của đồ án (Đã được GVHD duyệt định hướng và khung 7 chương). |
| *(Dự kiến tạo)* `src/` hoặc `notebooks/` | **Sắp thực hiện** | Sẽ chứa mã nguồn PySpark, kịch bản ETL HDFS, Spark MLlib pipeline huấn luyện mô hình Random Forest. |
| *(Dự kiến tạo)* `backend/` | **Sắp thực hiện** | REST API xây dựng bằng FastAPI để truy vấn MongoDB và phục vụ mô hình inference. |
| *(Dự kiến tạo)* `dashboard/` | **Sắp thực hiện** | Giao diện Dashboard trực quan hóa (React + Plotly.js / Chart.js). |

---

## 3. MỤC TIÊU & BÀI TOÁN KỸ THUẬT CỦA DỰ ÁN

1. **Xử lý Dữ liệu lớn phân tán (Distributed Big Data Processing):**
   * Lưu trữ dữ liệu trên cụm **Hadoop HDFS**.
   * Tiền xử lý, lọc nhiễu, làm sạch, tối ưu hóa định dạng lưu trữ (chuyển đổi CSV $\to$ Parquet có Partitioning) bằng **Apache Spark (PySpark)**.
2. **Machine Learning phân tán (Spark MLlib):**
   * Huấn luyện mô hình **Random Forest phân tán** để dự đoán và phân loại nhóm nguyên nhân chính gây trễ chuyến bay (Thời tiết, Hãng bay, Kiểm soát không lưu NAS, Trễ dây chuyền).
   * So sánh với các mô hình baseline (**Logistic Regression**, **Decision Tree**).
3. **Phục vụ & Trực quan hóa (Serving & Visualization):**
   * Lưu kết quả dự báo và thống kê tổng hợp vào **MongoDB**.
   * Xây dựng dịch vụ **FastAPI** phục vụ API.
   * Xây dựng **Dashboard React** trực quan hóa các chỉ số đo lường hiệu năng chuyến bay năm 2024.

---

## 4. DANH MỤC TÀI LIỆU HƯỚNG DẪN TRONG THƯ MỤC `doc/`

Các AI Agent khi tham gia dự án cần tra cứu các file chi tiết tương ứng:

0. [`00_AI_READ_FIRST.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/00_AI_READ_FIRST.md): **[ĐIỂM XUẤT PHÁT]** Chỉ mục điều hướng và bản tóm tắt nhanh nhất cho AI.
1. [`01_DATASET_SPECIFICATION.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/01_DATASET_SPECIFICATION.md): Chi tiết 35 cột, kiểu dữ liệu, tỷ lệ null, định nghĩa bài toán phân loại và nguyên tắc chống rò rỉ dữ liệu (Data Leakage).
2. [`02_ARCHITECTURE_AND_TECHSTACK.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/02_ARCHITECTURE_AND_TECHSTACK.md): Kiến trúc toàn diện hệ thống (HDFS $\to$ PySpark $\to$ Hive $\to$ Spark MLlib $\to$ MongoDB $\to$ FastAPI $\to$ React).
3. [`03_ROADMAP_AND_TASKS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/03_ROADMAP_AND_TASKS.md): Kế hoạch 11 tuần theo đề cương, trạng thái các công việc và nhiệm vụ ưu tiên tiếp theo.
4. [`04_AI_AGENT_GUIDELINES.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/04_AI_AGENT_GUIDELINES.md): Quy chuẩn lập trình, quy tắc an toàn tài nguyên (tránh OOM khi đọc file 1.2 GB), quy ước đặt tên và cách phối hợp giữa các AI.
5. [`05_WORKLOG_AND_HANDOVER.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/05_WORKLOG_AND_HANDOVER.md): **[BẮT BUỘC ĐỌC ĐẦU TIÊN]** Nhật ký công việc, tác vụ đang dở dang và chỉ định chính xác việc tiếp theo cần làm.
6. [`06_ENVIRONMENT_SETUP.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/06_ENVIRONMENT_SETUP.md): Hướng dẫn cài đặt Java JDK, Hadoop winutils trên Windows và Docker Compose.
7. [`07_EXPERIMENT_AND_EVALUATION_METRICS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/07_EXPERIMENT_AND_EVALUATION_METRICS.md): Kịch bản thực nghiệm, công thức đo lường và bảng số liệu mẫu cho Chương 5 Báo cáo.
8. [`08_HARDWARE_AND_ENVIRONMENT_SPECS.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/08_HARDWARE_AND_ENVIRONMENT_SPECS.md): Đo đạc phần cứng máy tính cục bộ thực tế và so sánh với Kaggle Cloud 2x T4.
9. [`09_DUAL_APPROACH_PLAN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/09_DUAL_APPROACH_PLAN.md): Phương pháp luận giải trình 2 hướng: Hướng Thuần (CPU/GPU) vs Hướng Spark.
10. [`10_MASTER_EXECUTION_PLAN.md`](file:///c:/LeDucLuong/HK%20VII/NhapMonBigData/DoAn/doc/10_MASTER_EXECUTION_PLAN.md): Lộ trình hành động tổng thể từ lúc nạp dataset đến khi nghiệm thu đồ án.


---

## 5. QUY TRÌNH BẮT BUỘC ĐỂ CẬP NHẬT VÀ BÀN GIAO TIẾN ĐỘ

Mọi AI Agent khi làm việc trong repository này **bắt buộc** tuân theo chu trình 3 bước:

```text
[BƯỚC 1: CHECK-IN]
Mở `doc/05_WORKLOG_AND_HANDOVER.md`
-> Đọc mục "1. TRẠNG THÁI HIỆN TẠI" & "1.3 VIỆC TIẾP THEO CẦN LÀM NGAY"
-> Nắm rõ ngữ cảnh, không làm trùng lặp công việc của phiên trước.

[BƯỚC 2: THỰC THI (EXECUTION)]
-> Kiểm tra logic trên `flight_data_2024_sample.csv` trước khi chạy toàn bộ dữ liệu 1.2 GB.
-> Tuân thủ cấu trúc thư mục quy định trong `doc/04_AI_AGENT_GUIDELINES.md`.

[BƯỚC 3: CHECK-OUT & BÀN GIAO]
-> Cập nhật bảng Worklog tại `doc/05_WORKLOG_AND_HANDOVER.md` (Thời gian, việc đã làm, file tạo/sửa).
-> Cập nhật mục "1. TRẠNG THÁI HIỆN TẠI" và nêu rõ nhiệm vụ kế tiếp cho AI sau.
-> Đánh dấu hoàn thành (nếu có) trong `doc/03_ROADMAP_AND_TASKS.md`.
```

