"""Module: clean_data.py
Description: Tiền xử lý dữ liệu chuyến bay, làm sạch khuyết thiếu,
gán nhãn nguyên nhân trễ và chuẩn hóa đặc trưng (chống rò rỉ dữ liệu).
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import argparse
import numpy as np
import pandas as pd

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# Bản đồ ánh xạ nguyên nhân trễ
CAUSE_MAPPING = {
    'OnTime_or_MinorDelay': 0,
    'Carrier': 1,
    'Weather': 2,
    'NAS': 3,
    'Security': 4,
    'LateAircraft': 5
}

CAUSE_COLUMNS = [
    'carrier_delay',
    'weather_delay',
    'nas_delay',
    'security_delay',
    'late_aircraft_delay'
]

COLUMN_TO_CAUSE_NAME = {
    'carrier_delay': 'Carrier',
    'weather_delay': 'Weather',
    'nas_delay': 'NAS',
    'security_delay': 'Security',
    'late_aircraft_delay': 'LateAircraft'
}

def determine_delay_cause(row):
    """Xác định nguyên nhân trễ chính dựa trên nguyên nhân có số phút cao nhất."""
    if row['arr_delay'] < 15:
        return 'OnTime_or_MinorDelay'
    
    # Tìm cột nguyên nhân trễ lớn nhất
    max_val = -1
    dominant_cause = 'Carrier' # Fallback mặc định nếu nguyên nhân không được ghi nhận
    
    for col in CAUSE_COLUMNS:
        val = row.get(col, 0)
        if pd.notna(val) and val > max_val:
            max_val = val
            dominant_cause = COLUMN_TO_CAUSE_NAME[col]
            
    if max_val <= 0:
        # Trường hợp trễ >= 15 phút nhưng không có chi tiết nguyên nhân
        return 'Carrier'
        
    return dominant_cause

def add_time_features(df):
    """Trích xuất đặc trưng giờ và khung giờ từ crs_dep_time và crs_arr_time."""
    # crs_dep_time có dạng HHMM (ví dụ 1018 là 10:18)
    dep_hour = (df['crs_dep_time'] // 100).clip(0, 23)
    arr_hour = (df['crs_arr_time'] // 100).clip(0, 23)
    
    df['dep_hour'] = dep_hour
    df['arr_hour'] = arr_hour
    
    # Phân nhóm khung giờ trong ngày:
    # Sáng (5-11), Trưa/Chiều (12-16), Tối (17-21), Đêm (22-4)
    conditions = [
        (dep_hour >= 5) & (dep_hour < 12),
        (dep_hour >= 12) & (dep_hour < 17),
        (dep_hour >= 17) & (dep_hour < 22)
    ]
    choices = ['Morning', 'Afternoon', 'Evening']
    df['dep_time_of_day'] = np.select(conditions, choices, default='Night')
    
    return df

def clean_flight_data(df, drop_cancelled_diverted=True):
    """Quy trình làm sạch dữ liệu toàn diện."""
    initial_rows = len(df)
    print(f"[*] Bắt đầu làm sạch: Tổng số dòng ban đầu = {initial_rows:,}")
    
    # 1. Điền giá trị 0 cho các cột delay khuyết thiếu
    for col in CAUSE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(np.float32)
            
    # 2. Lọc các chuyến bay bị hủy hoặc chuyển hướng nếu cần
    if drop_cancelled_diverted and 'cancelled' in df.columns and 'diverted' in df.columns:
        valid_mask = (df['cancelled'] == 0) & (df['diverted'] == 0)
        df = df[valid_mask].copy()
        print(f"[*] Đã lọc chuyến bay hủy/chuyển hướng: Còn lại = {len(df):,} dòng (Loại {initial_rows - len(df):,} dòng)")
        
    # 3. Xử lý giá trị khuyết thiếu ở arr_delay và dep_delay
    if 'arr_delay' in df.columns:
        df = df.dropna(subset=['arr_delay']).copy()
    if 'dep_delay' in df.columns:
        df['dep_delay'] = df['dep_delay'].fillna(0)
        
    # 4. Gán nhãn biến mục tiêu
    print("[*] Đang xác định biến mục tiêu (Target Labeling)...")
    df['is_delayed'] = (df['arr_delay'] >= 15).astype(int)
    df['delay_cause'] = df.apply(determine_delay_cause, axis=1)
    df['delay_cause_code'] = df['delay_cause'].map(CAUSE_MAPPING)
    
    # 5. Trích xuất đặc trưng thời gian
    print("[*] Đang trích xuất đặc trưng thời gian...")
    df = add_time_features(df)
    
    # Thống kê phân bố nhãn
    print("\n--- PHÂN BỐ NGUYÊN NHÂN TRỄ ---")
    dist = df['delay_cause'].value_counts()
    for cause, count in dist.items():
        pct = (count / len(df)) * 100
        print(f"  • {cause:22s}: {count:>6,} ({pct:>5.2f}%)")
    print("--------------------------------\n")
    
    return df

def main():
    parser = argparse.ArgumentParser(description="Tiền xử lý và làm sạch dữ liệu Flight 2024")
    parser.add_argument(
        "--input",
        type=str,
        default=r"Flight Delay Dataset — 2024/flight_data_2024_sample.csv",
        help="Đường dẫn file CSV đầu vào"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=r"Flight Delay Dataset — 2024/cleaned_sample.parquet",
        help="Đường dẫn file đầu ra (Parquet hoặc CSV)"
    )
    args = parser.parse_args()
    
    print(f"[*] Đang đọc file: {args.input}")
    df = pd.read_csv(args.input)
    
    cleaned_df = clean_flight_data(df)
    
    # Xuất file kết quả
    if args.output.endswith('.parquet'):
        print(f"[*] Đang lưu định dạng Parquet: {args.output}")
        cleaned_df.to_parquet(args.output, index=False, engine='pyarrow')
    else:
        print(f"[*] Đang lưu định dạng CSV: {args.output}")
        cleaned_df.to_csv(args.output, index=False)
        
    print(f"[✓] Hoàn thành thành công! Kích thước dữ liệu sạch: {cleaned_df.shape}")

if __name__ == "__main__":
    main()
