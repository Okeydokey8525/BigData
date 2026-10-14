import sys
import os
import pandas as pd
import numpy as np

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

sys.path.append('Web_Air')
from core.model_loader import model_manager
from core.predictor import build_13_features, predict_single_model

model_manager.load_all()

# Đọc mẫu dữ liệu
sample_path = 'Flight Delay Dataset — 2024/flight_data_2024_sample.csv'
df = pd.read_csv(sample_path, nrows=50000)

# Lọc những chuyến bay bị trễ thực tế
delayed_df = df[df['arr_delay'] >= 30].copy()
print(f"Tổng số dòng trễ >= 30p: {len(delayed_df)}")

results_found = {}

for idx, r in delayed_df.iterrows():
    carrier = str(r['op_unique_carrier'])
    orig = str(r['origin'])
    dest = str(r['dest'])
    m = int(r['month'])
    d = int(r['day_of_month'])
    fl_date = f"2024-{m:02d}-{d:02d}"
    crs_dep = int(r['crs_dep_time'])
    dep_h = crs_dep // 100
    dep_min = crs_dep % 100
    t_str = f"{dep_h:02d}:{dep_min:02d}"
    elap = float(r['crs_elapsed_time'])
    dist = float(r['distance'])

    try:
        feats = build_13_features(carrier, orig, dest, fl_date, t_str, elap, dist)
        for m_name in ['decision_tree', 'random_forest_cpu', 'catboost', 'lightgbm', 'xgboost']:
            pred = predict_single_model(m_name, feats)
            code = pred['predicted_code']
            if code != 0:
                key = (code, m_name)
                if key not in results_found:
                    results_found[key] = {
                        "model": m_name,
                        "code": code,
                        "title": pred['predicted_title'],
                        "conf": pred['confidence_percent'],
                        "carrier": carrier,
                        "origin": orig,
                        "dest": dest,
                        "date": fl_date,
                        "time": t_str,
                        "elapsed": elap,
                        "distance": dist,
                        "probs": {p['name']: p['percent'] for p in pred['probabilities'] if p['percent'] > 10.0}
                    }
    except Exception:
        continue

    if len(results_found) >= 10:
        break

print(f"\n=== ĐÃ TÌM THẤY {len(results_found)} CẤU HÌNH THỰC TẾ RA NHÃN TRỄ ===")
for key, data in sorted(results_found.items()):
    print(f"\n[NHÃN {data['code']}]: {data['title']} (Mô hình: {data['model']}, Tin cậy: {data['conf']}%)")
    print(f"  - Hãng bay (Carrier): {data['carrier']}")
    print(f"  - Sân bay: {data['origin']} -> {data['dest']}")
    print(f"  - Ngày bay: {data['date']} | Giờ cất cánh: {data['time']}")
    print(f"  - Thời gian bay: {data['elapsed']} phút | Khoảng cách: {data['distance']} dặm")
    print(f"  - Chi tiết xác suất: {data['probs']}")
