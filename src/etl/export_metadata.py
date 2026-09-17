"""Module: export_metadata.py
Description: Trích xuất danh mục hãng bay (Carriers), sân bay (Airports),
lưu trữ bộ LabelEncoder chuẩn vào models/baseline/encoders.joblib và metadata.json
để phục vụ Web Dashboard FastAPI.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import json
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import joblib

# Đảm bảo tiếng Việt không bị lỗi encoding console
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

CARRIER_NAMES = {
    "AA": "American Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue Airways",
    "DL": "Delta Air Lines",
    "F9": "Frontier Airlines",
    "G4": "Allegiant Air",
    "HA": "Hawaiian Airlines",
    "NK": "Spirit Airlines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "9E": "Endeavor Air",
    "C5": "CommuteAir",
    "G7": "GoJet Airlines",
    "MQ": "Envoy Air",
    "OH": "PSA Airlines",
    "OO": "SkyWest Airlines",
    "PT": "Piedmont Airlines",
    "QX": "Horizon Air",
    "YX": "Republic Airways",
    "ZW": "Air Wisconsin"
}

TOP_AIRPORTS = {
    "ATL": "Hartsfield-Jackson Atlanta (ATL)",
    "DFW": "Dallas/Fort Worth (DFW)",
    "DEN": "Denver International (DEN)",
    "ORD": "Chicago O'Hare (ORD)",
    "LAX": "Los Angeles International (LAX)",
    "JFK": "John F. Kennedy New York (JFK)",
    "LAS": "Harry Reid Las Vegas (LAS)",
    "MCO": "Orlando International (MCO)",
    "MIA": "Miami International (MIA)",
    "CLT": "Charlotte Douglas (CLT)",
    "SEA": "Seattle-Tacoma (SEA)",
    "PHX": "Phoenix Sky Harbor (PHX)",
    "EWR": "Newark Liberty (EWR)",
    "SFO": "San Francisco (SFO)",
    "IAH": "George Bush Houston (IAH)",
    "BOS": "Boston Logan (BOS)",
    "MSP": "Minneapolis-Saint Paul (MSP)",
    "DTW": "Detroit Metropolitan (DTW)",
    "FLL": "Fort Lauderdale-Hollywood (FLL)",
    "LGA": "LaGuardia New York (LGA)"
}

FEATURE_COLUMNS_CAT = ['op_unique_carrier', 'origin', 'dest', 'dep_time_of_day']

def main():
    data_path = r"Flight Delay Dataset — 2024/cleaned_sample.parquet"
    out_dir = "models/baseline"
    os.makedirs(out_dir, exist_ok=True)
    
    print(f"[*] Đang nạp dữ liệu từ: {data_path}")
    df = pd.read_parquet(data_path)
    
    encoders = {}
    for col in FEATURE_COLUMNS_CAT:
        le = LabelEncoder()
        le.fit(df[col].astype(str))
        encoders[col] = le
        print(f"  - Đã khớp LabelEncoder cho [{col}]: {len(le.classes_)} giá trị")
        
    encoders_path = os.path.join(out_dir, "encoders.joblib")
    joblib.dump(encoders, encoders_path)
    print(f"[✓] Đã lưu encoders tại: {encoders_path}")
    
    # Chuẩn bị danh mục hãng bay
    unique_carriers = sorted(df['op_unique_carrier'].unique().tolist())
    carriers_list = [
        {"code": c, "name": f"{c} - {CARRIER_NAMES.get(c, 'Hãng hàng không ' + c)}"}
        for c in unique_carriers
    ]
    
    # Chuẩn bị danh mục sân bay
    unique_origins = sorted(list(set(df['origin'].unique().tolist() + df['dest'].unique().tolist())))
    airports_list = [
        {"code": a, "name": TOP_AIRPORTS.get(a, f"{a} - Sân bay {a}")}
        for a in unique_origins
    ]
    
    metadata = {
        "carriers": carriers_list,
        "airports": airports_list,
        "times_of_day": ["Morning (05:00 - 11:59)", "Afternoon (12:00 - 16:59)", "Evening (17:00 - 20:59)", "Night (21:00 - 04:59)"],
        "models": [
            {"id": "random_forest_cpu", "name": "Random Forest (Scikit-Learn) [Mô hình Trọng tâm]", "type": "Ensemble Bagging"},
            {"id": "lightgbm", "name": "LightGBM (Microsoft) [Cực nhanh]", "type": "Gradient Boosting"},
            {"id": "xgboost", "name": "XGBoost (DMLC) [Macro F1 cao nhất]", "type": "Gradient Boosting"},
            {"id": "catboost", "name": "CatBoost (Yandex) [Mạnh về Categorical]", "type": "Gradient Boosting"},
            {"id": "decision_tree", "name": "Decision Tree (Cây đơn lẻ)", "type": "Single Tree"},
            {"id": "logistic_regression", "name": "Logistic Regression (Baseline tuyến tính)", "type": "Linear"}
        ],
        "causes": {
            0: {"name": "OnTime_or_MinorDelay", "title": "Đúng giờ hoặc Trễ nhẹ (< 15 phút)", "color": "#10B981", "badge": "success"},
            1: {"name": "Carrier Delay", "title": "Trễ do Lỗi Hãng Hàng Không", "color": "#3B82F6", "badge": "primary"},
            2: {"name": "Weather Delay", "title": "Trễ do Thời Tiết Cực Đoan", "color": "#F59E0B", "badge": "warning"},
            3: {"name": "NAS Delay", "title": "Trễ do Hệ Thống Không Lưu (NAS)", "color": "#8B5CF6", "badge": "secondary"},
            4: {"name": "Security Delay", "title": "Trễ do An Ninh Sân Bay", "color": "#EC4899", "badge": "danger"},
            5: {"name": "LateAircraft Delay", "title": "Trễ Dây Chuyền do Máy Bay Đến Muộn", "color": "#EF4444", "badge": "danger"}
        }
    }
    
    meta_path = os.path.join(out_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"[✓] Đã lưu metadata tại: {meta_path}\n")

if __name__ == "__main__":
    main()
