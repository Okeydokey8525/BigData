import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'Web_Air/core/results_store.py'
spec = importlib.util.spec_from_file_location('web_results_test_target', SOURCE)
store = importlib.util.module_from_spec(spec)
spec.loader.exec_module(store)


class ResultsTests(unittest.TestCase):
    def write(self, root, name, data):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding='utf-8')

    def test_legacy_fallback_is_excluded_and_missing_macro_stays_missing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.write(root, 'results/metrics/baseline_summary_7m.json', {
                'Logistic Regression': {'accuracy': 79.18, 'train_time_sec': 11.03},
                'Random Forest (Spark MLlib)': {'accuracy': 78.88}})
            self.write(root, 'results/spark/metrics/spark_end_to_end_metrics.json', {
                'timestamp': '2026-10-10 21:09:08', 'mllib_stage': {
                    'random_forest': {'name': 'Random Forest (Spark MLlib)',
                        'status': 'Hoàn tất thành công', 'accuracy': .792,
                        'wall_clock_time_seconds': 31.03},
                    'xgboost_spark': {'status': 'Hoàn tất (Kiến trúc phân tán)', 'accuracy': .7927}}})
            with patch.object(store, 'ROOT', root):
                rows = store.metrics_rows()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[1]['Accuracy (%)'], 79.2)
                self.assertIsNone(rows[1]['Macro F1 (%)'])
                self.assertIsNone(rows[1]['RAM Usage (MB)'])

    def test_new_run_selected_without_mixing_failed_models_with_older_run(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.write(root, 'results/metrics/baseline_summary_7m.json', {})
            self.write(root, 'results/spark/metrics/old.json', {
                'timestamp': '2026-10-10', 'mllib_stage': {'random_forest': {'status': 'success'}}})
            self.write(root, 'results/spark/metrics/runs/new/metrics.json', {
                'timestamp': '2026-10-13', 'mllib_stage': {
                    'random_forest': {'status': 'failed'},
                    'lightgbm_spark': {'status': 'success', 'name': 'LightGBM Spark', 'accuracy': .8}}})
            with patch.object(store, 'ROOT', root):
                rows = store.metrics_rows()
                self.assertEqual([r['Model'] for r in rows], ['LightGBM Spark'])
                self.assertEqual(store.result_status()['spark_timestamp'], '2026-10-13')


if __name__ == '__main__':
    unittest.main()
