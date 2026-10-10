"""Module: generate_14m_enriched.py
Description: Tự động sinh tập dữ liệu làm giàu 14.16 triệu dòng x 41 cột
(kết hợp nhân đôi số dòng và làm giàu đặc trưng trước tiền xử lý).
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import gc
import time
import shutil
import numpy as np
import pandas as pd

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
DATA_DIR = os.path.join(BASE_DIR, "Flight Delay Dataset — 2024")
INPUT_CSV = os.path.join(DATA_DIR, "flight_data_2024.csv")
OUTPUT_CSV = os.path.join(DATA_DIR, "flight_data_2024_14m_enriched.csv")
PART1_CSV = os.path.join(DATA_DIR, "temp_part1_orig.csv")
PART2_CSV = os.path.join(DATA_DIR, "temp_part2_sim.csv")

def enrich_chunk(chunk_df, is_simulation=False):
    """Bổ sung 6 cột đặc trưng làm giàu chuyên sâu và áp dụng jittering nếu là batch mô phỏng."""
    df = chunk_df.copy()
    
    # 1. Thêm nhiễu ngẫu nhiên thực tế nếu là batch nhân đôi
    if is_simulation:
        np.random.seed(len(df) + 42)
        jitter = np.random.choice([-2, -1, 1, 2, 3], size=len(df), p=[0.2, 0.25, 0.25, 0.2, 0.1])
        valid_mask = (df['cancelled'] == 0) & (df['diverted'] == 0)
        
        if 'dep_delay' in df.columns:
            df.loc[valid_mask, 'dep_delay'] = df.loc[valid_mask, 'dep_delay'] + jitter[valid_mask]
        if 'arr_delay' in df.columns:
            df.loc[valid_mask, 'arr_delay'] = df.loc[valid_mask, 'arr_delay'] + jitter[valid_mask]
        df['simulation_batch'] = 1
    else:
        df['simulation_batch'] = 0

    # 2. Đặc trưng hiệu suất vận hành mặt đất: taxi_out_ratio
    crs_time = df['crs_elapsed_time'].replace(0, np.nan)
    taxi_ratio = (df['taxi_out'] / crs_time).clip(0, 1).fillna(0.0)
    df['taxi_out_ratio'] = taxi_ratio.round(4)

    # 3. Đặc trưng tốc độ bay thực tế: flight_speed_mph
    air_time_hours = (df['air_time'] / 60.0).replace(0, np.nan)
    speed = (df['distance'] / air_time_hours).clip(50, 700).fillna(0.0)
    df['flight_speed_mph'] = speed.round(2)

    # 4. Đặc trưng lịch trình: is_weekend (Thứ 7 & CN)
    df['is_weekend'] = df['day_of_week'].isin([6, 7]).astype(np.int8)

    # 5. Đặc trưng mùa cao điểm / ngày lễ Mỹ 2024 (is_holiday_season)
    is_holiday = (
        df['month'].isin([7, 11, 12]) | 
        ((df['month'] == 1) & (df['day_of_month'] <= 5)) |
        ((df['month'] == 5) & (df['day_of_month'] >= 24))
    ).astype(np.int8)
    df['is_holiday_season'] = is_holiday

    # 6. Đặc trưng địa lý: is_same_state (Chuyến bay nội bang hay liên bang)
    if 'origin_state_nm' in df.columns and 'dest_state_nm' in df.columns:
        df['is_same_state'] = (df['origin_state_nm'] == df['dest_state_nm']).astype(np.int8)
    else:
        df['is_same_state'] = 0

    return df

def generate_14m_dataset(chunksize=250_000):
    print("=" * 72, flush=True)
    print(" KHỞI CHẠY QUY TRÌNH TỰ ĐỘNG TẠO DATASET LÀM GIÀU 14.16 TRIỆU DÒNG (41 CỘT)", flush=True)
    print("=" * 72, flush=True)
    print(f" • Tệp nguồn CSV gốc : {INPUT_CSV}", flush=True)
    print(f" • Tệp đích 14M      : {OUTPUT_CSV}", flush=True)
    print(f" • Kích thước Chunk  : {chunksize:,} dòng/mẩu (single-pass streaming)", flush=True)

    if not os.path.exists(INPUT_CSV):
        print(f"[!] Lỗi: Không tìm thấy tệp gốc {INPUT_CSV}", flush=True)
        return

    # Dọn dẹp tệp tạm và tệp đích cũ nếu có
    for f in [OUTPUT_CSV, PART1_CSV, PART2_CSV]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception as e:
                print(f"[!] Cảnh báo không thể xóa {f}: {e}", flush=True)

    start_time = time.perf_counter()
    rows_orig = 0
    rows_sim = 0
    chunk_idx = 0
    header_written = False

    print("\n[*] Đang đọc nguồn một lượt (Single-Pass) và tạo đồng thời 2 phần...", flush=True)
    
    # Định nghĩa dtype nhẹ cho các cột string/int nếu cần để tránh C parser memory leak
    csv_reader = pd.read_csv(INPUT_CSV, chunksize=chunksize, low_memory=False)

    for chunk in csv_reader:
        chunk_idx += 1
        n = len(chunk)

        # 1. Phần 1: Gốc 2024 làm giàu (simulation_batch = 0)
        df_orig = enrich_chunk(chunk, is_simulation=False)
        df_orig.to_csv(
            PART1_CSV,
            mode='a',
            header=not header_written,
            index=False,
            encoding='utf-8'
        )
        rows_orig += n
        del df_orig

        # 2. Phần 2: Nhân đôi mô phỏng biến thiên Jittering (simulation_batch = 1)
        # Không ghi header ở phần 2 để sẵn sàng nối binary
        df_sim = enrich_chunk(chunk, is_simulation=True)
        df_sim.to_csv(
            PART2_CSV,
            mode='a',
            header=False,
            index=False,
            encoding='utf-8'
        )
        rows_sim += n
        del df_sim

        header_written = True
        del chunk
        gc.collect()

        print(f"  -> Chunk {chunk_idx:>2}: Đã xử lý {n:>7,} dòng | Tích lũy mỗi phần: {rows_orig:>9,} dòng (Tổng ghi: {rows_orig + rows_sim:>10,} dòng)", flush=True)

    print(f"\n[✓] Xử lý chunk hoàn tất! Gốc: {rows_orig:,} dòng | Mô phỏng: {rows_sim:,} dòng.", flush=True)
    print(f"[*] Đang thực hiện ghép nối nhị phân (Binary Stream Append) siêu tốc vào tệp đích...", flush=True)
    concat_start = time.perf_counter()

    # Di chuyển Part 1 thành Output CSV
    shutil.move(PART1_CSV, OUTPUT_CSV)

    # Nối nhị phân Part 2 vào Output CSV theo buffer 1MB
    with open(OUTPUT_CSV, "ab") as f_out:
        with open(PART2_CSV, "rb") as f_in:
            shutil.copyfileobj(f_in, f_out, length=1024 * 1024)

    # Xóa tệp tạm Part 2
    if os.path.exists(PART2_CSV):
        os.remove(PART2_CSV)

    concat_time = time.perf_counter() - concat_start
    print(f"[✓] Ghép nối hoàn tất trong {concat_time:.2f} giây!", flush=True)

    total_written = rows_orig + rows_sim
    total_time = time.perf_counter() - start_time
    file_size_bytes = os.path.getsize(OUTPUT_CSV)
    file_size_gb = file_size_bytes / (1024**3)

    print("\n" + "=" * 72, flush=True)
    print(" [THÀNH CÔNG] HOÀN TẤT SINH DATASET LÀM GIÀU 14.16 TRIỆU DÒNG!", flush=True)
    print("=" * 72, flush=True)
    print(f" • Tổng số dòng thực tế : {total_written:,} dòng", flush=True)
    print(f" • Dung lượng tệp CSV   : {file_size_gb:.2f} GB ({file_size_bytes:,} bytes)", flush=True)
    print(f" • Thời gian tạo tệp    : {total_time:.2f} giây ({total_time/60:.2f} phút)", flush=True)
    print(f" • Đường dẫn tệp        : {OUTPUT_CSV}", flush=True)
    print("=" * 72, flush=True)

if __name__ == "__main__":
    generate_14m_dataset()
