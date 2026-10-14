"""Module: model_loader.py
Description: Nạp các mô hình học máy, Scaler, LabelEncoder và siêu dữ liệu vào RAM.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import json
import joblib
import threading
import psutil
from config import MODELS_DIR

class ModelManager:
    """Quản lý Singleton nạp và truy xuất tài nguyên mô hình."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ModelManager, cls).__new__(cls)
            cls._instance.load_lock = threading.RLock()
            cls._instance.models = {}
            cls._instance.scaler = None
            cls._instance.encoders = {}
            cls._instance.metadata = {}
            cls._instance.is_loaded = False
        return cls._instance

    def _artifact_signature(self):
        return tuple(sorted((f, os.stat(os.path.join(MODELS_DIR,f)).st_mtime_ns, os.stat(os.path.join(MODELS_DIR,f)).st_size)
            for f in os.listdir(MODELS_DIR) if f.endswith((".joblib", ".json"))))

    def load_all(self):
        with self.load_lock:
            return self._load_all_locked()

    def _load_all_locked(self):
        """Nạp toàn bộ mô hình và đối tượng phụ trợ vào RAM."""
        signature = self._artifact_signature()
        if self.is_loaded and getattr(self, "signature", None) == signature:
            return self
        self.models = {}
        self.encoders = {}
        self.scaler = None
        self.metadata = {}

        print("[*] Đang nạp tài nguyên mô hình vào RAM từ:", MODELS_DIR)

        # 1. Nạp Scaler
        scaler_file = os.path.join(MODELS_DIR, "scaler.joblib")
        if os.path.exists(scaler_file):
            self.scaler = joblib.load(scaler_file)
            print("  [✓] Đã nạp StandardScaler")
        else:
            print("  [!] Không tìm thấy scaler.joblib")

        # 2. Nạp Categorical Encoders
        encoders_file = os.path.join(MODELS_DIR, "encoders.joblib")
        if os.path.exists(encoders_file):
            self.encoders = joblib.load(encoders_file)
            print("  [✓] Đã nạp Categorical Encoders")
        else:
            print("  [!] Không tìm thấy encoders.joblib")

        # 3. Nạp Metadata danh mục (Hãng bay, sân bay)
        meta_file = os.path.join(MODELS_DIR, "metadata.json")
        if os.path.exists(meta_file):
            with open(meta_file, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
            print("  [✓] Đã nạp Metadata danh mục")
        else:
            print("  [!] Không tìm thấy metadata.json")

        # 4. Danh sách các mô hình cần nạp
        model_targets = [
            ("random_forest_cpu", "random_forest_cpu.joblib", "Random Forest (Scikit-Learn CPU)"),
            ("catboost", "catboost.joblib", "CatBoost (CPU)"),
            ("xgboost", "xgboost.joblib", "XGBoost (CPU)"),
            ("lightgbm", "lightgbm.joblib", "LightGBM (Microsoft GOSS/EFB)"),
            ("decision_tree", "decision_tree.joblib", "Decision Tree (Cây Quyết Định Đơn)"),
            ("logistic_regression", "logistic_regression.joblib", "Logistic Regression (Tuyến Tính Chuẩn)")
        ]

        for model_id, filename, name in model_targets:
            path = os.path.join(MODELS_DIR, filename)
            if os.path.exists(path):
                try:
                    self.models[model_id] = {
                        "model": joblib.load(path),
                        "name": name,
                        "filename": filename
                    }
                    print(f"  [✓] Đã nạp mô hình: {model_id} ({name})")
                except Exception as e:
                    print(f"  [!] Lỗi khi nạp {model_id}: {e}")
            else:
                print(f"  [!] Tệp mô hình không tồn tại: {path}")

        self.signature = signature
        self.is_loaded = True
        rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        print(f"[✓] Hoàn tất nạp {len(self.models)} mô hình! RAM hiện tại: {rss_mb:.1f} MB\n")
        return self

    def get_model(self, model_id: str):
        """Lấy một đối tượng mô hình theo ID."""
        self.load_all()
        return self.models.get(model_id, {}).get("model", None)

    def get_models_list(self):
        """Trả về danh mục các mô hình khả dụng kèm tên thân thiện."""
        self.load_all()
        return [
            {"id": mid, "name": info["name"]}
            for mid, info in self.models.items()
        ]

# Instance Singleton dùng chung
model_manager = ModelManager()
