"""Module: metrics.py
Description: Các hàm đo lường chất lượng mô hình phân loại đa lớp
và theo dõi hiệu năng thời gian thực thi (Latency/Speedup).
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import time
import json
import numpy as np
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report
)

class BenchmarkTimer:
    """Class đo thời gian thực thi chính xác của từng giai đoạn."""
    def __init__(self, name="Task"):
        self.name = name
        self.start_time = None
        self.elapsed_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        print(f"[*] Bắt đầu thực thi: {self.name}...")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed_time = time.perf_counter() - self.start_time
        print(f"[✓] Hoàn tất: {self.name} trong {self.elapsed_time:.3f} giây.")

def evaluate_multiclass_predictions(y_true, y_pred, labels=None, target_names=None):
    """Tính toán toàn bộ các chỉ số đo lường cho bài toán phân loại đa lớp."""
    acc = accuracy_score(y_true, y_pred)
    prec_weighted = precision_score(y_true, y_pred, average='weighted', zero_division=0)
    rec_weighted = recall_score(y_true, y_pred, average='weighted', zero_division=0)
    f1_weighted = f1_score(y_true, y_pred, average='weighted', zero_division=0)
    
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    
    report_dict = {
        'accuracy': float(acc),
        'weighted_precision': float(prec_weighted),
        'weighted_recall': float(rec_weighted),
        'weighted_f1_score': float(f1_weighted),
        'confusion_matrix': cm.tolist()
    }
    
    print("\n" + "="*50)
    print(" KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH")
    print("="*50)
    print(f" • Accuracy           : {acc * 100:.2f}%")
    print(f" • Weighted Precision : {prec_weighted * 100:.2f}%")
    print(f" • Weighted Recall    : {rec_weighted * 100:.2f}%")
    print(f" • Weighted F1-Score  : {f1_weighted * 100:.2f}%")
    print("\nChi tiết từng lớp:")
    print(classification_report(y_true, y_pred, target_names=target_names, zero_division=0))
    print("="*50 + "\n")
    
    return report_dict
