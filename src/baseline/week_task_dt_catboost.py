import os
import sys
import io
import time
import pandas as pd
import numpy as np

# Đảm bảo in tiếng Việt trên console Windows mượt mà không lỗi
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.tree import DecisionTreeClassifier
from catboost import CatBoostClassifier
from sklearn.ensemble import VotingClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.etl.clean_data import clean_flight_data

def print_step(msg):
    print(f"\n{'='*70}\n[*] {msg}\n{'='*70}")

def main():
    # 1. NẠP VÀ LÀM SẠCH DỮ LIỆU CƠ BẢN
    print_step("BƯỚC 1: NẠP VÀ LÀM SẠCH DỮ LIỆU CƠ BẢN")
    data_path = r"Flight Delay Dataset — 2024/flight_data_2024.csv"
    print(f"Đang đọc dữ liệu từ: {data_path} (NẠP TOÀN BỘ 7 TRIỆU DÒNG - KIỂM TRA GIỚI HẠN RAM)")
    
    # Đọc toàn bộ file 1.2GB vào RAM (Rủi ro Out Of Memory)
    df = pd.read_csv(data_path)
    
    # [QUAN TRỌNG] Chỉ giữ lại các chuyến bay THỰC SỰ BỊ TRỄ (arr_delay >= 15)
    # Vì bài toán là phân loại 5 nguyên nhân trễ, ta không quan tâm tới các chuyến OnTime
    if 'arr_delay' in df.columns:
        df = df[df['arr_delay'] >= 15].copy()
        print(f"Đã lọc bỏ chuyến bay đúng giờ. Số chuyến bay trễ còn lại: {len(df):,} dòng.")
    
    # Sử dụng bộ clean_data gốc để tạo target 'delay_cause_code' (Từ nhãn 1 đến 5)
    df = clean_flight_data(df, verbose=False)
    print(f"Số lượng bản ghi sẵn sàng đưa vào luồng IQR: {len(df):,} dòng.")

    # 2. LỌC DÒNG RÁC BẰNG KHOẢNG PHÂN VỊ (IQR)
    print_step("BƯỚC 2: TỰ ĐỘNG LỌC OUTLIERS (DÒNG RÁC) BẰNG KHOẢNG PHÂN VỊ (IQR)")
    # Ví dụ: Lọc các chuyến bay có thời gian trễ khởi hành (dep_delay) lớn phi lý do lỗi nhập liệu
    cols_to_filter = ['dep_delay', 'taxi_out']
    cols_to_filter = [c for c in cols_to_filter if c in df.columns]
    
    initial_len = len(df)
    for col in cols_to_filter:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
        
    print(f"Số dòng lỗi/nhiễu bị loại bỏ bằng IQR: {initial_len - len(df):,} dòng.")
    print(f"Dữ liệu sạch còn lại: {len(df):,} dòng.")

    # 3. LOẠI BỎ CỘT THỪA BẰNG MA TRẬN TƯƠNG QUAN
    print_step("BƯỚC 3: LOẠI BỎ CỘT THỪA BẰNG MA TRẬN TƯƠNG QUAN (MULTICOLLINEARITY)")
    
    # [QUAN TRỌNG] Chống Rò rỉ dữ liệu (Data Leakage): Loại bỏ các cột nguyên nhân trễ gốc và thời gian thực tế
    leakage_cols = ['arr_delay', 'dep_delay', 'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay', 'actual_elapsed_time', 'arr_time', 'dep_time', 'wheels_on', 'wheels_off', 'is_delayed', 'delay_cause']
    df = df.drop(columns=[c for c in leakage_cols if c in df.columns], errors='ignore')
    
    # Tạm tách target_col ra để không bị loại bỏ nhầm do tương quan
    target_col = 'delay_cause_code'
    y_target = df[target_col] if target_col in df.columns else None
    if y_target is not None:
        df = df.drop(columns=[target_col])
        
    num_df = df.select_dtypes(include=[np.number])
    corr_matrix = num_df.corr().abs()
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    
    # Mốc tương quan 0.85
    threshold = 0.85
    to_drop = [column for column in upper.columns if any(upper[column] > threshold)]
    
    print(f"Các cột có độ tương quan quá cao (>{threshold}) bị gỡ bỏ tự động: {to_drop}")
    df = df.drop(columns=to_drop, errors='ignore')
    
    # Gắn lại biến mục tiêu
    if y_target is not None:
        df[target_col] = y_target

    # 4. CHUẨN BỊ DỮ LIỆU ĐỂ TÌM FEATURE IMPORTANCE
    print_step("CHUẨN BỊ DỮ LIỆU VÀ MÃ HÓA")
    if target_col not in df.columns:
        print(f"Cột {target_col} không tồn tại, sẽ thử dùng cột cuối.")
        target_col = df.columns[-1]
    
    # Mã hóa
    cat_cols = df.select_dtypes(include=['object', 'category']).columns
    for col in cat_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        
    X = df.drop(columns=[target_col]).fillna(0)
    y = df[target_col]

    # 5. CHỌN CỘT BẰNG FEATURE IMPORTANCE (DÙNG DECISION TREE NHÁP)
    print_step("BƯỚC 4: LỌC CỘT THỪA BẰNG FEATURE IMPORTANCE (DRAFT MODEL)")
    draft_tree = DecisionTreeClassifier(max_depth=5, random_state=42)
    draft_tree.fit(X, y)
    
    importances = draft_tree.feature_importances_
    cols_to_drop_by_imp = [X.columns[i] for i in range(len(X.columns)) if importances[i] == 0]
    print(f"Các cột có Information Gain = 0 bị loại bỏ ({len(cols_to_drop_by_imp)} cột):")
    if len(cols_to_drop_by_imp) > 0:
        print("  ->", cols_to_drop_by_imp)
        
    X_core = X.drop(columns=cols_to_drop_by_imp)
    print(f"Số lượng cột đặc trưng (Features) giữ lại huấn luyện: {X_core.shape[1]}")

    # 6. HUẤN LUYỆN 2 MÔ HÌNH VÀ KẾT HỢP (ENSEMBLE)
    print_step("BƯỚC 5: HUẤN LUYỆN ĐƠN LẺ VÀ KẾT HỢP (DECISION TREE & CATBOOST)")
    X_train, X_test, y_train, y_test = train_test_split(X_core, y, test_size=0.2, random_state=42, stratify=y)
    
    print("1. Đang huấn luyện Decision Tree...")
    dt_clf = DecisionTreeClassifier(max_depth=12, class_weight='balanced', random_state=42)
    t0 = time.time()
    dt_clf.fit(X_train, y_train)
    print(f" -> Xong Decision Tree trong {time.time() - t0:.2f}s")
    
    print("2. Đang huấn luyện CatBoost...")
    cb_clf = CatBoostClassifier(iterations=80, learning_rate=0.1, auto_class_weights='Balanced', verbose=0, random_state=42)
    t0 = time.time()
    cb_clf.fit(X_train, y_train)
    print(f" -> Xong CatBoost trong {time.time() - t0:.2f}s")
    
    print("3. Đang huấn luyện Ensemble Voting (Decision Tree + CatBoost)...")
    ensemble_clf = VotingClassifier(
        estimators=[('dt', dt_clf), ('cb', cb_clf)],
        voting='soft'
    )
    t0 = time.time()
    ensemble_clf.fit(X_train, y_train)
    print(f" -> Xong Mô hình Kết hợp trong {time.time() - t0:.2f}s")

    # 7. ĐÁNH GIÁ KẾT QUẢ VÀ CONFUSION MATRIX
    print_step("BƯỚC 6: ĐÁNH GIÁ (ACCURACY, PRECISION, RECALL, F1, CONFUSION MATRIX)")
    
    models = {
        "DECISION TREE ĐƠN LẺ": dt_clf,
        "CATBOOST ĐƠN LẺ": cb_clf,
        "ENSEMBLE (DT + CATBOOST)": ensemble_clf
    }
    
    for name, model in models.items():
        print(f"\n[{name}]")
        y_pred = model.predict(X_test)
        
        if hasattr(y_pred, "flatten"):
            y_pred = y_pred.flatten()
            
        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, average='weighted', zero_division=0)
        rec = recall_score(y_test, y_pred, average='weighted', zero_division=0)
        f1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)
        
        print(f"  Accuracy  : {acc*100:.2f}%")
        print(f"  Precision : {prec*100:.2f}%")
        print(f"  Recall    : {rec*100:.2f}%")
        print(f"  F1-Score  : {f1*100:.2f}%")
        print(f"  Confusion Matrix:\n{confusion_matrix(y_test, y_pred)}")

if __name__ == '__main__':
    main()
