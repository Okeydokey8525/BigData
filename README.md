# ỨNG DỤNG THUẬT TOÁN RANDOM FOREST PHÂN TÁN DỰ ĐOÁN NGUYÊN NHÂN TRỄ CHUYẾN BAY THƯƠNG MẠI: TRƯỜNG HỢP TẠI HOA KỲ NĂM 2024

[![Học phần](https://img.shields.io/badge/H%E1%BB%8Dc%20Ph%E1%BA%A7n-Nh%E1%BA%ADp%20M%C3%B4n%20Big%20Data-blue)](https://huit.edu.vn)
[![Đơn vị](https://img.shields.io/badge/%C4%90%C6%A1n%20V%E1%BB%8B-HUIT-orange)](https://huit.edu.vn)
[![Apache Spark](https://img.shields.io/badge/Distributed%20Computing-Apache%20Spark-red)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.13-brightgreen)](https://www.python.org/)

---

## 📌 THÔNG TIN ĐỀ TÀI

* **Tên đề tài:** ỨNG DỤNG THUẬT TOÁN RANDOM FOREST PHÂN TÁN DỰ ĐOÁN NGUYÊN NHÂN TRỄ CHUYẾN BAY THƯƠNG MẠI: TRƯỜNG HỢP TẠI HOA KỲ NĂM 2024
* **Học phần:** Nhập môn Big Data (HK VII)
* **Trường:** Đại học Công Thương Thành phố Hồ Chí Minh (HUIT)
* **Giảng viên hướng dẫn:** TS. Phan Hồ Viết Trường
* **Nhóm thực hiện:** Nhóm 6 (Thứ 4, Ca 10 - 12)
  1. **Cù Văn Vĩ An** - MSSV: `2001230005`
  2. **Lê Đức Lương** - MSSV: `2001230490`
  3. **Trần Huỳnh Tuấn Anh** - MSSV: `2001230023`

---

## 🚀 MỤC TIÊU & BÀI TOÁN KỸ THUẬT

Đồ án triển khai đối chứng thực nghiệm **2 HƯỚNG SONG SONG**:
1. **Hướng Thuần (Single-node Baseline):** Sử dụng các công cụ truyền thống (Pandas, Scikit-learn Random Forest, LightGBM, CatBoost, XGBoost) trên CPU/GPU đơn máy để đo đạc và chỉ ra giới hạn nghẽn tài nguyên (RAM/OOM) khi xử lý dữ liệu quy mô lớn.
2. **Hướng Spark (Distributed Big Data - Trọng tâm đề tài):** Xây dựng hệ thống phân tán toàn diện dựa trên **Apache Spark (PySpark, Spark SQL, Spark MLlib)** và **Hadoop HDFS**; tối ưu hóa I/O bằng định dạng **Apache Parquet (Snappy)** phân vùng theo tháng; huấn luyện mô hình **Random Forest phân tán** để phân loại 5 nhóm nguyên nhân gây trễ chính (`Carrier`, `Weather`, `NAS`, `Security`, `LateAircraft`).

---

## 📊 BỘ DỮ LIỆU (DATASET)

* **Tên tập dữ liệu:** [Flight Delay Dataset — 2024 (Kaggle)](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)
* **Nguồn gốc gốc:** Cục Thống kê Giao thông Vận tải Hoa Kỳ (Bureau of Transportation Statistics - BTS DOT).
* **Quy mô:** **7,079,081 bản ghi** (hơn 7.07 triệu chuyến bay năm 2024), **35 thuộc tính đo lường**, dung lượng CSV thô: **~1.22 GB**.
* **Lưu ý tải dữ liệu lớn:** Tệp CSV gốc `flight_data_2024.csv` vượt quá giới hạn dung lượng của GitHub (100 MB) nên đã được thêm vào `.gitignore`. Vui lòng tải trực tiếp từ link Kaggle ở trên và lưu vào thư mục `Flight Delay Dataset — 2024/`.
* Tệp dữ liệu mẫu `flight_data_2024_sample.csv` (10,000 dòng, 1.85 MB) và từ điển dữ liệu tiếng Việt `flight_data_2024_data_dictionary.csv` đã được tích hợp sẵn trong repo để kiểm thử.

---

## 🗂️ CẤU TRÚC THƯ MỤC DỰ ÁN

```text
├── .gitignore                          # Quy định loại trừ file lớn (>100MB) và môi trường ảo
├── README.md                           # Giới thiệu tổng quan dự án (File này)
├── Nhom6_T4_C10-12_BaoCao.docx         # Đề cương đồ án học phần đã được duyệt
├── Flight Delay Dataset — 2024/        # Thư mục dữ liệu
│   ├── flight_data_2024_data_dictionary.csv  # Từ điển dữ liệu (kèm cột nghĩa tiếng Việt)
│   ├── flight_data_2024_sample.csv           # 10,000 dòng mẫu để kiểm thử nhanh
│   └── flight_data_2024.csv                  # [Download từ Kaggle] 7.07M dòng (1.22 GB)
├── Bao_Cao/                            # Báo cáo học phần chính thức, slide thuyết trình
├── doc/                                # Hệ tri thức dự án & Hướng dẫn kỹ thuật
│   ├── 00_AI_READ_FIRST.md             # Điểm xuất phát điều hướng cho AI Agent
│   ├── 01_DATASET_SPECIFICATION.md     # Đặc tả 35 cột dữ liệu & Target Labeling
│   ├── 02_ARCHITECTURE_AND_TECHSTACK.md# Kiến trúc phân tán HDFS, Spark, Mongo, FastAPI, React
│   ├── 03_ROADMAP_AND_TASKS.md         # Kế hoạch 11 tuần bám sát đề cương
│   ├── 04_AI_AGENT_GUIDELINES.md       # Quy tắc an toàn RAM (chống OOM) & Code convention
│   ├── 05_WORKLOG_AND_HANDOVER.md      # Nhật ký làm việc & Bàn giao nhiệm vụ luân phiên
│   ├── 06_ENVIRONMENT_SETUP.md         # Cài đặt Java JDK 17, Hadoop winutils, Docker
│   ├── 07_EXPERIMENT_AND_EVALUATION_METRICS.md # Tiêu chuẩn đánh giá 6 mô hình ML
│   ├── 08_HARDWARE_AND_ENVIRONMENT_SPECS.md    # Đo đạc thông số máy Local vs Kaggle Cloud
│   ├── 09_DUAL_APPROACH_PLAN.md        # Giải trình 2 hướng Thuần vs Spark & Phân công nhóm
│   ├── 10_MASTER_EXECUTION_PLAN.md     # Lộ trình hành động 6 giai đoạn chi tiết
│   ├── README.md                       # Mục lục tài liệu nội bộ
│   └── nguyen-tac-lam-viec-dai.md      # Quy tắc làm việc dài & Chuẩn mực AI
├── src/                                # [Sắp triển khai] Mã nguồn ETL, Spark ML, Baseline
├── docker/                             # [Sắp triển khai] Docker Compose cụm Hadoop, Spark, MongoDB
├── backend/                            # [Sắp triển khai] FastAPI REST API
└── dashboard/                          # [Sắp triển khai] React Frontend Dashboard
```

---

## 🛠️ CÔNG NGHỆ CHÍNH

* **Tầng Lưu trữ & Xử lý:** Apache Hadoop HDFS, Apache Spark (PySpark DataFrame, Spark SQL, Spark MLlib).
* **Tối ưu hóa định dạng:** Apache Parquet có nén Snappy phân vùng theo tháng.
* **Mô hình học máy:**
  * *Nền tảng:* Logistic Regression, Decision Tree, Random Forest (Đề xuất phân tán).
  * *Cải tiến SOTA:* LightGBM, CatBoost, XGBoost.
* **Tầng Serving & Trực quan hóa:** MongoDB, FastAPI, React, Plotly.js / Chart.js.
