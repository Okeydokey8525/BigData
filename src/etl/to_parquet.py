"""Module: to_parquet.py
Description: Kịch bản chuyển đổi tệp CSV sang định dạng nén tối ưu
Apache Parquet (Snappy) có phân vùng (partitioning) theo tháng.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import time
import argparse
import pandas as pd

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# Đảm bảo import được module từ thư mục gốc
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from src.etl.clean_data import clean_flight_data

def get_directory_size(path):
    """Tính tổng dung lượng (bytes) của một file hoặc thư mục."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total_size = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def convert_csv_to_parquet(input_csv, output_parquet_dir, partition_col="month"):
    """Chuyển đổi CSV sang Parquet có phân vùng và nén Snappy."""
    print("="*60)
    print(" BẮT ĐẦU CHUYỂN ĐỔI SANG ĐỊNH DẠNG PARQUET")
    print("="*60)
    print(f" • Nguồn dữ liệu CSV : {input_csv}")
    print(f" • Đích lưu Parquet : {output_parquet_dir}")
    print(f" • Cột phân vùng    : {partition_col}")
    
    start_time = time.perf_counter()
    
    # 1. Đọc dữ liệu
    print("\n[*] Bước 1: Đang nạp dữ liệu từ CSV...")
    df = pd.read_csv(input_csv)
    raw_size_bytes = os.path.getsize(input_csv)
    print(f"[✓] Đọc thành công {len(df):,} dòng (Dung lượng thô: {raw_size_bytes / (1024**2):.2f} MB)")
    
    # 2. Tiền xử lý và làm sạch
    print("\n[*] Bước 2: Đang thực hiện quy trình làm sạch & gán nhãn...")
    cleaned_df = clean_flight_data(df)
    
    # 3. Ghi ra định dạng Parquet có phân vùng
    print(f"\n[*] Bước 3: Đang ghi ra Parquet (Nén Snappy, phân vùng theo '{partition_col}')...")
    os.makedirs(output_parquet_dir, exist_ok=True)
    
    cleaned_df.to_parquet(
        output_parquet_dir,
        engine='pyarrow',
        compression='snappy',
        partition_cols=[partition_col],
        index=False
    )
    
    elapsed = time.perf_counter() - start_time
    parquet_size_bytes = get_directory_size(output_parquet_dir)
    reduction_pct = (1 - (parquet_size_bytes / raw_size_bytes)) * 100 if raw_size_bytes > 0 else 0
    
    print("\n" + "="*60)
    print(" KẾT QUẢ TỐI ƯU HÓA ĐỊNH DẠNG DỮ LIỆU")
    print("="*60)
    print(f" • Dung lượng CSV ban đầu   : {raw_size_bytes / (1024**2):.2f} MB")
    print(f" • Dung lượng Parquet sau nén: {parquet_size_bytes / (1024**2):.2f} MB")
    print(f" • Tỷ lệ giảm dung lượng     : {reduction_pct:.2f}%")
    print(f" • Tổng thời gian thực thi   : {elapsed:.2f} giây")
    print("="*60 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Chuyển đổi CSV sang Parquet phân vùng")
    parser.add_argument(
        "--input",
        type=str,
        default=r"Flight Delay Dataset — 2024/flight_data_2024_sample.csv",
        help="Đường dẫn tệp CSV đầu vào"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=r"Flight Delay Dataset — 2024/sample_parquet_partitioned",
        help="Thư mục xuất tệp Parquet phân vùng"
    )
    args = parser.parse_args()
    
    convert_csv_to_parquet(args.input, args.output_dir)

if __name__ == "__main__":
    main()
