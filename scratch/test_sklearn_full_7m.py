import os
import sys
import io
import time
import gc
import psutil
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def get_process_ram_mb():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

def get_free_system_ram_gb():
    return psutil.virtual_memory().available / (1024 * 1024 * 1024)

print("=" * 70)
print(f"KIỂM TRA THỰC NGHIỆM: TRAIN SCIKIT-LEARN RANDOM FOREST TRÊN 100% 7M DÒNG")
print(f"RAM hệ thống còn trống: {get_free_system_ram_gb():.2f} GB")
print("=" * 70)

parquet_path = r"Flight Delay Dataset — 2024/cleaned_flight_data_2024.parquet"
if not os.path.exists(parquet_path):
    print("Không tìm thấy file parquet!")
    sys.exit(1)

num_cols = ['month', 'day_of_month', 'day_of_week', 'dep_hour', 'arr_hour', 'crs_elapsed_time', 'distance']
cat_cols = ['op_unique_carrier', 'origin', 'dest', 'dep_time_of_day']
target_col = 'delay_cause_code'
cols_to_read = num_cols + cat_cols + [target_col]

t0 = time.time()
print(f"[*] 1. Đang nạp toàn bộ 100% dòng dữ liệu từ Parquet...")
df = pd.read_parquet(parquet_path, columns=cols_to_read)
print(f"[✓] Đã nạp thành công: {len(df):,} dòng trong {time.time()-t0:.2f}s")
print(f"    RAM tiến trình: {get_process_ram_mb():.1f} MB | RAM trống hệ thống: {get_free_system_ram_gb():.2f} GB")

# Chuẩn bị dữ liệu dạng float32
t1 = time.time()
print(f"[*] 2. Đang chuẩn bị đặc trưng (Label Encoding & float32)...")
for col in num_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.float32)

for col in cat_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str)).astype(np.float32)

y = df[target_col].values.astype(np.int32)
X = df[num_cols + cat_cols].values.astype(np.float32)
del df
gc.collect()

print(f"[✓] Ma trận X shape: {X.shape}, size: {X.nbytes / (1024*1024):.1f} MB")
print(f"    RAM tiến trình sau khi gom rác: {get_process_ram_mb():.1f} MB | RAM trống: {get_free_system_ram_gb():.2f} GB")

# Tách train/test 80/20
print(f"[*] 3. Tách Train (80%) / Test (20%)...")
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
del X, y
gc.collect()

print(f"[✓] Train: {len(X_train):,} dòng | Test: {len(X_test):,} dòng")
print(f"    RAM tiến trình: {get_process_ram_mb():.1f} MB | RAM trống: {get_free_system_ram_gb():.2f} GB")

# Thử nghiệm train Random Forest Scikit-learn
# Để an toàn và công bằng với Spark (Spark dùng 50 trees, maxDepth=10):
# Ta dùng n_estimators=50, max_depth=10, n_jobs=4 (để tránh bùng nổ RAM do joblib đa luồng)
print(f"[*] 4. Bắt đầu huấn luyện Scikit-Learn RandomForestClassifier (n_estimators=50, max_depth=10, n_jobs=4)...")
rf = RandomForestClassifier(
    n_estimators=50,
    max_depth=10,
    n_jobs=4,
    random_state=42
)

t_train_start = time.time()
rf.fit(X_train, y_train)
t_train = time.time() - t_train_start

print(f"[✓] Huấn luyện Scikit-learn THÀNH CÔNG trên {len(X_train):,} dòng!")
print(f"    Thời gian huấn luyện: {t_train:.2f} giây")
print(f"    RAM tiến trình đỉnh: {get_process_ram_mb():.1f} MB | RAM trống: {get_free_system_ram_gb():.2f} GB")

print(f"[*] 5. Dự đoán trên tập Test ({len(X_test):,} dòng)...")
t_pred_start = time.time()
y_pred = rf.predict(X_test)
t_pred = time.time() - t_pred_start

acc = accuracy_score(y_test, y_pred) * 100
f1 = f1_score(y_test, y_pred, average='weighted') * 100

print(f"[✓] ĐÁNH GIÁ KẾT QUẢ THỰC TẾ TRÊN 100% TẬP DỮ LIỆU:")
print(f"    Accuracy: {acc:.2f}%")
print(f"    Weighted F1: {f1:.2f}%")
print(f"    Thời gian train: {t_train:.2f}s")
print(f"    Thời gian test: {t_pred:.2f}s")
print("=" * 70)
