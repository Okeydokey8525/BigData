import os
import sys
import io
import time
import shutil
import argparse
import psutil
import gc
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

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

from src.etl.clean_data import clean_flight_data, DTYPE_OPTIMIZED

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

def convert_csv_to_parquet_stream(input_csv, output_parquet_dir, partition_col="month", chunksize=500_000):
    """Chuyển đổi CSV lớn sang Parquet có phân vùng bằng cơ chế Streaming Chunking tối ưu RAM."""
    print("="*65)
    print(" BẮT ĐẦU CHUYỂN ĐỔI STREAMING SANG PARQUET PHÂN VÙNG")
    print("="*65)
    print(f" • Nguồn dữ liệu CSV : {input_csv}")
    print(f" • Đích lưu Parquet : {output_parquet_dir}")
    print(f" • Cột phân vùng    : {partition_col}")
    print(f" • Kích thước Chunk : {chunksize:,} dòng/mẩu")
    
    raw_size_bytes = os.path.getsize(input_csv)
    print(f" • Dung lượng CSV thô: {raw_size_bytes / (1024**2):.2f} MB")
    
    # Xóa thư mục cũ nếu tồn tại để tránh rác
    if os.path.exists(output_parquet_dir):
        print(f"[*] Đang làm sạch thư mục đích trước khi ghi: {output_parquet_dir}")
        shutil.rmtree(output_parquet_dir)
    os.makedirs(output_parquet_dir, exist_ok=True)
    
    process = psutil.Process()
    start_time = time.perf_counter()
    peak_ram_bytes = 0
    total_raw_rows = 0
    total_cleaned_rows = 0
    
    # Đọc CSV theo từng khối (chunk iterator)
    chunk_iterator = pd.read_csv(
        input_csv,
        chunksize=chunksize,
        low_memory=False
    )
    
    for chunk_idx, chunk_df in enumerate(chunk_iterator, start=1):
        chunk_start = time.perf_counter()
        raw_count = len(chunk_df)
        total_raw_rows += raw_count
        
        # Tiền xử lý mẩu dữ liệu bằng Vectorized NumPy
        cleaned_chunk = clean_flight_data(chunk_df, verbose=False)
        clean_count = len(cleaned_chunk)
        total_cleaned_rows += clean_count
        
        # Chuyển đổi các cột category sang string để đảm bảo schema pyarrow đồng nhất giữa các chunk
        for c in cleaned_chunk.select_dtypes(include=['category']).columns:
            cleaned_chunk[c] = cleaned_chunk[c].astype(str)
            
        # Ghi chunk ra Parquet phân vùng theo tháng
        table = pa.Table.from_pandas(cleaned_chunk, preserve_index=False)
        ds.write_dataset(
            data=table,
            base_dir=output_parquet_dir,
            format="parquet",
            partitioning=[partition_col],
            partitioning_flavor="hive",
            basename_template=f"chunk_{chunk_idx}_{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore"
        )
        
        # Đo lường tài nguyên
        current_ram = process.memory_info().rss
        if current_ram > peak_ram_bytes:
            peak_ram_bytes = current_ram
            
        chunk_time = time.perf_counter() - chunk_start
        print(f"  [+] Chunk {chunk_idx:>2}: Đọc {raw_count:>7,} -> Sạch {clean_count:>7,} dòng | RAM: {current_ram / (1024**2):>6.1f} MB | Thời gian: {chunk_time:>5.2f}s")
        
        # Thu hồi bộ nhớ
        del chunk_df, cleaned_chunk, table
        gc.collect()
        
    total_elapsed = time.perf_counter() - start_time
    parquet_size_bytes = get_directory_size(output_parquet_dir)
    reduction_pct = (1 - (parquet_size_bytes / raw_size_bytes)) * 100 if raw_size_bytes > 0 else 0
    throughput = total_raw_rows / total_elapsed if total_elapsed > 0 else 0
    
    print("\n" + "="*65)
    print(" KẾT QUẢ STREAMING ETL & TỐI ƯU HÓA PARQUET")
    print("="*65)
    print(f" • Tổng số dòng thô đọc vào : {total_raw_rows:,} dòng")
    print(f" • Tổng số dòng sạch đầu ra : {total_cleaned_rows:,} dòng (Lọc {total_raw_rows - total_cleaned_rows:,} dòng)")
    print(f" • Dung lượng CSV thô       : {raw_size_bytes / (1024**2):.2f} MB")
    print(f" • Dung lượng Parquet sau nén: {parquet_size_bytes / (1024**2):.2f} MB")
    print(f" • Tỷ lệ giảm dung lượng     : {reduction_pct:.2f}%")
    print(f" • Mức RAM đỉnh tiêu thụ     : {peak_ram_bytes / (1024**2):.2f} MB")
    print(f" • Tốc độ xử lý (Throughput) : {throughput:,.0f} dòng/giây")
    print(f" • Tổng thời gian thực thi   : {total_elapsed:.2f} giây (~{total_elapsed/60:.2f} phút)")
    print("="*65 + "\n")
    
    return {
        "total_raw_rows": total_raw_rows,
        "total_cleaned_rows": total_cleaned_rows,
        "raw_size_mb": round(raw_size_bytes / (1024**2), 2),
        "parquet_size_mb": round(parquet_size_bytes / (1024**2), 2),
        "reduction_pct": round(reduction_pct, 2),
        "peak_ram_mb": round(peak_ram_bytes / (1024**2), 2),
        "throughput_rows_per_sec": round(throughput, 2),
        "elapsed_sec": round(total_elapsed, 2)
    }

def convert_csv_to_parquet(input_csv, output_parquet_dir, partition_col="month", chunksize=500_000, stream=False):
    """Điều phối chuyển đổi CSV sang Parquet (In-Memory hoặc Streaming)."""
    raw_size_bytes = os.path.getsize(input_csv) if os.path.exists(input_csv) else 0
    # Nếu file > 50 MB hoặc có cờ stream -> dùng streaming chunking
    if stream or raw_size_bytes > 50 * (1024**2):
        return convert_csv_to_parquet_stream(input_csv, output_parquet_dir, partition_col, chunksize=chunksize)
        
    print("="*60)
    print(" BẮT ĐẦU CHUYỂN ĐỔI SANG ĐỊNH DẠNG PARQUET (IN-MEMORY)")
    print("="*60)
    start_time = time.perf_counter()
    df = pd.read_csv(input_csv)
    cleaned_df = clean_flight_data(df)
    
    if os.path.exists(output_parquet_dir):
        shutil.rmtree(output_parquet_dir)
    os.makedirs(output_parquet_dir, exist_ok=True)
    
    for c in cleaned_df.select_dtypes(include=['category']).columns:
        cleaned_df[c] = cleaned_df[c].astype(str)
        
    table = pa.Table.from_pandas(cleaned_df, preserve_index=False)
    ds.write_dataset(
        data=table,
        base_dir=output_parquet_dir,
        format="parquet",
        partitioning=[partition_col],
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore"
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
    parser = argparse.ArgumentParser(description="Chuyển đổi CSV sang Parquet phân vùng tối ưu RAM")
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
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Số dòng mỗi mẩu dữ liệu khi streaming"
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Bắt buộc sử dụng chế độ Streaming Chunking"
    )
    args = parser.parse_args()
    
    convert_csv_to_parquet(args.input, args.output_dir, chunksize=args.chunksize, stream=args.stream)

if __name__ == "__main__":
    main()

