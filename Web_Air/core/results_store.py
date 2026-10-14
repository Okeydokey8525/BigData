"""Read measured results and saved artifacts without fabricating missing metrics."""
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
CPU_NAMES = ['Logistic Regression', 'Decision Tree', 'Random Forest (CPU)', 'LightGBM', 'XGBoost', 'CatBoost']

def read_json(path):
    with path.open(encoding='utf-8') as stream:
        return json.load(stream)

def latest_spark():
    folder = ROOT / 'results/spark/metrics'
    candidates = []
    for path in folder.rglob('*.json'):
        if 'archive' in path.parts:
            continue
        try:
            data = read_json(path)
            if data.get('synthetic_data') or not isinstance(data.get('mllib_stage'), dict):
                continue
            candidates.append((str(data.get('timestamp', '')), path.stat().st_mtime, path, data))
        except (OSError, ValueError):
            continue
    if not candidates:
        return None, {}
    _, _, path, data = max(candidates, key=lambda item: item[:2])
    return path, data

def metrics_rows():
    summary = read_json(ROOT / 'results/metrics/baseline_summary_7m.json')
    # Precision/recall are available only in the CPU CSV, not the JSON summary.
    import csv
    csv_path = ROOT / 'results/metrics/grand_model_comparison_7m.csv'
    extra = {}
    if csv_path.exists():
        with csv_path.open(encoding='utf-8', newline='') as stream:
            extra = {r['Model']: r for r in csv.DictReader(stream)}
    rows = []
    for name in CPU_NAMES:
        if name not in summary:
            continue
        r = summary[name]
        e = extra.get(name, {})
        rows.append({'Model': name, 'Train Time (s)': r.get('train_time_sec'),
            'Latency (ms/1k)': r.get('latency_ms_per_1k'), 'RAM Usage (MB)': r.get('peak_ram_mb'),
            'Accuracy (%)': r.get('accuracy'), 'Weighted F1 (%)': r.get('weighted_f1'),
            'Macro F1 (%)': r.get('macro_f1'),
            'Weighted Precision (%)': float(e['Weighted Precision (%)']) if e.get('Weighted Precision (%)') else None,
            'Weighted Recall (%)': float(e['Weighted Recall (%)']) if e.get('Weighted Recall (%)') else None})
    _, data = latest_spark()
    for key, r in data.get('mllib_stage', {}).items():
        measured = r.get('status') == 'success'
        legacy_native = key in ('logistic_regression','decision_tree','random_forest') and r.get('status') == 'Hoàn tất thành công'
        if not (measured or legacy_native):
            continue
        percent = lambda field: round(r[field] * 100, 4) if r.get(field) is not None else None
        rows.append({'Model': r.get('name', key), 'Train Time (s)': r.get('wall_clock_time_seconds'),
            'Latency (ms/1k)': r.get('latency_ms_per_1k'), 'RAM Usage (MB)': None,
            'Accuracy (%)': percent('accuracy'), 'Weighted F1 (%)': percent('f1_score_weighted'),
            'Macro F1 (%)': percent('f1_score_macro'), 'Weighted Precision (%)': percent('precision_weighted'),
            'Weighted Recall (%)': percent('recall_weighted')})
    return rows

def result_status():
    path, data = latest_spark()
    inventory = []
    folder = ROOT / 'models/spark'
    for model in sorted(folder.glob('*_model')):
        if not model.is_dir():
            continue
        files = list(model.rglob('*'))
        latest = max((f.stat().st_mtime for f in files if f.is_file()), default=model.stat().st_mtime)
        inventory.append({'name': model.name, 'saved_at': datetime.fromtimestamp(latest).isoformat(timespec='seconds'),
            'prediction_available': False,
            'note': 'Đã lưu mô hình Spark; chưa tích hợp suy luận Web cùng pipeline đặc trưng tương ứng.'})
    excluded = [{'name': r.get('name', key), 'status': r.get('status'), 'error': r.get('error')}
        for key,r in data.get('mllib_stage', {}).items()
        if not (r.get('status') == 'success' or (key in ('logistic_regression','decision_tree','random_forest') and r.get('status') == 'Hoàn tất thành công'))]
    return {'spark_timestamp': data.get('timestamp'), 'spark_source': str(path.relative_to(ROOT)) if path else None,
        'cpu_source': 'results/metrics/baseline_summary_7m.json', 'excluded_models': excluded,
        'spark_saved_models': inventory,
        'note': 'Mốc lưu mô hình và mốc file chỉ số có thể khác nhau. Chưa có chỉ số mới thì không thay bằng số của lần cũ.'}

def pipeline_rows():
    rows = metrics_rows()
    _, data = latest_spark()
    return [{'Approach': 'CPU: tổng thời gian train 6 mô hình', 'Training': round(sum(r['Train Time (s)'] or 0 for r in rows if r['Model'] in CPU_NAMES),2),
        'ETL': None, 'Feature_Engineering': None, 'Evaluation': None, 'Total_Time': None, 'Peak_RAM_MB': None},
        {'Approach': 'Spark: ETL đã lưu; chưa đối chiếu tổng toàn trình', 'ETL': data.get('etl_stage',{}).get('wall_clock_time_seconds'),
        'Feature_Engineering': None, 'Training': None, 'Evaluation': None, 'Total_Time': None, 'Peak_RAM_MB': None}]
