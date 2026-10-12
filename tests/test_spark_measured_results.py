"""Regression checks for measured Spark results, without requiring a cluster."""
import ast
import argparse
import os
import sys
import tempfile
import time
import traceback
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace

SOURCE = Path(__file__).resolve().parents[1] / 'src/spark_ml/run_end_to_end_spark.py'
TREE = ast.parse(SOURCE.read_text(encoding='utf-8'))


def load_def(name, namespace):
    node = next(n for n in TREE.body if getattr(n, 'name', None) == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace[name]


class MeasurementTests(unittest.TestCase):
    def test_runtime_selects_matching_scala_artifact(self):
        coordinate = load_def('synapseml_coordinate', {})
        self.assertEqual(coordinate('3.5.3'), 'com.microsoft.azure:synapseml-lightgbm_2.12:1.0.15')
        self.assertEqual(coordinate('4.1.1'), 'com.microsoft.azure:synapseml-lightgbm_2.13:1.1.3-spark4.1')
        self.assertEqual(coordinate('4.2.0'), coordinate('4.1.1'))
        self.assertTrue(coordinate('4.0.1').endswith('spark4.0'))
        with self.assertRaises(RuntimeError):
            coordinate('5.0.0')

    def test_scala_212_packages_rejected_before_starting_spark4(self):
        create = load_def('create_spark_session', {'os': os})
        with patch.dict(sys.modules, {'pyspark': SimpleNamespace(__version__='4.2.0')}):
            with patch.dict(os.environ, {'SPARK_EXTRA_PACKAGES': 'com.microsoft.azure:synapseml_2.12:1.0.15'}):
                with self.assertRaisesRegex(RuntimeError, 'incompatible'):
                    create('local[1]')

    def test_dependencies_do_not_require_catboost_on_windows(self):
        imported = []
        def resolve(module):
            imported.append(module)
            return SimpleNamespace(SparkXGBClassifier=Mock(), LightGBMClassifier=Mock())
        check = load_def('require_training_dependencies', {
            'importlib': SimpleNamespace(import_module=resolve),
            'os': SimpleNamespace(name='nt'),
        })
        check(Mock())
        self.assertEqual(imported, ['xgboost.spark', 'synapse.ml.lightgbm'])

    def test_no_cpu_or_network_estimate(self):
        clock = SimpleNamespace(perf_counter=iter([10.0, 12.5]).__next__)
        cls = load_def('SparkDualTimingBenchmark', {'time': clock})
        result, timing = cls(SimpleNamespace(sparkContext=None)).measure(lambda: 'fit result')
        self.assertEqual(result, 'fit result')
        self.assertEqual(timing['wall_clock_time_s'], 2.5)
        self.assertIsNone(timing['pure_spark_time_s'])
        self.assertIsNone(timing['network_wifi_overhead_s'])

    def test_macro_f1_counts_and_missing_classes(self):
        macro = load_def('macro_f1_from_counts', {})
        self.assertAlmostEqual(macro([[8, 2], [1, 9]]), (16 / 19 + 18 / 21) / 2)
        self.assertEqual(macro([[10, 0], [0, 0]]), 0.5)

    def test_failed_fit_never_receives_benchmark_scores(self):
        node = next(n for n in TREE.body if getattr(n, 'name', '') == 'execute_pure_spark_mllib')
        loop = next(n for n in node.body if isinstance(n, ast.For))
        class Broken:
            def extractParamMap(self):
                return {}
            def fit(self, data):
                raise RuntimeError('worker unavailable')
        benchmark = load_def('SparkDualTimingBenchmark', {'time': time})(SimpleNamespace(sparkContext=None))
        context = dict(os=os, print=lambda *args: None, time=time, traceback=traceback, models_config=[dict(
            id='xgboost_spark', name='XGBoost', type='third_party_spark', estimator=Broken())],
            all_models_results={}, train_count=10, val_count=2, test_count=2,
            train_df=object(), benchmarker=benchmark, get_process_ram_mb=lambda: 10)
        exec(compile(ast.Module(body=[loop], type_ignores=[]), str(SOURCE), 'exec'), context)
        result = context['all_models_results']['xgboost_spark']
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['failed_phase'], 'fit')
        self.assertIn('worker unavailable', result['error'])
        for key in ['accuracy', 'f1_score_weighted', 'latency_ms_per_1k', 'wall_clock_time_seconds']:
            self.assertNotIn(key, result)

    def test_success_requires_fit_evaluation_and_model_save(self):
        node = next(n for n in TREE.body if getattr(n, 'name', '') == 'execute_pure_spark_mllib')
        loop = next(n for n in node.body if isinstance(n, ast.For))
        predictions = Mock()
        predictions.cache.return_value = predictions
        predictions.count.return_value = 10
        predictions.groupBy.return_value.count.return_value.collect.return_value = [
            {"label": 0, "prediction": 0, "count": 8},
            {"label": 1, "prediction": 0, "count": 2},
        ]
        fitted = Mock(spec=['transform', 'write'])
        fitted.transform.return_value = predictions
        estimator = Mock()
        estimator.extractParamMap.return_value = {}
        estimator.fit.return_value = fitted
        benchmark = load_def('SparkDualTimingBenchmark', {'time': time})(SimpleNamespace(sparkContext=None))
        context = dict(os=os, print=lambda *args: None, time=time, traceback=traceback,
            models_config=[dict(id='random_forest', name='Random Forest', type='third_party_spark', estimator=estimator)],
            all_models_results={}, train_count=10, val_count=2, test_count=10,
            train_df=object(), test_df=object(), benchmarker=benchmark,
            get_process_ram_mb=lambda: 15, target_col='label', TARGET_NAMES=['OnTime', 'Carrier'],
            macro_f1_from_counts=load_def('macro_f1_from_counts', {}), MODELS_DIR=Path('models'),
            acc_evaluator=SimpleNamespace(evaluate=lambda _: 0.8),
            f1_evaluator=SimpleNamespace(evaluate=lambda _: 0.71),
            prec_evaluator=SimpleNamespace(evaluate=lambda _: 0.64),
            rec_evaluator=SimpleNamespace(evaluate=lambda _: 0.8))
        exec(compile(ast.Module(body=[loop], type_ignores=[]), str(SOURCE), 'exec'), context)
        result = context['all_models_results']['random_forest']
        self.assertEqual(result['status'], 'success')
        estimator.fit.assert_called_once_with(context['train_df'])
        fitted.write.return_value.overwrite.return_value.save.assert_called_once()
        self.assertEqual(result['confusion_matrix'], [[8, 0], [2, 0]])
        self.assertAlmostEqual(result['f1_score_macro'], 4 / 9)
        self.assertEqual(result['accuracy'], 0.8)
        self.assertIsNone(result['pure_spark_time_seconds'])
        predictions.unpersist.assert_called_once()

    def test_failed_initialization_is_recorded(self):
        node = next(n for n in TREE.body if getattr(n, 'name', '') == 'execute_pure_spark_mllib')
        loop = next(n for n in node.body if isinstance(n, ast.For))
        def missing_jar():
            raise RuntimeError('missing JVM class')
        context = dict(os=os, print=lambda *args: None, time=time, traceback=traceback, models_config=[dict(
            id='lightgbm_spark', name='LightGBM', type='third_party_spark', factory=missing_jar)],
            all_models_results={}, train_count=10, val_count=2, test_count=2)
        exec(compile(ast.Module(body=[loop], type_ignores=[]), str(SOURCE), 'exec'), context)
        result = context['all_models_results']['lightgbm_spark']
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['failed_phase'], 'initialization')
        self.assertNotIn('accuracy', result)


class TrainingInputTests(unittest.TestCase):
    def invoke_cli(self, *arguments):
        run = Mock()
        namespace = dict(argparse=argparse, Path=Path, os=os,
                         CLEAN_PARQUET_DIR=Path('cleaned.parquet'), run_end_to_end_pipeline=run)
        main = load_def('main', namespace)
        with patch.object(sys, 'argv', ['train'] + list(arguments)), patch.dict(os.environ):
            main()
        return run.call_args.kwargs

    def test_default_skips_etl(self):
        args = self.invoke_cli()
        self.assertTrue(args['skip_etl'])
        self.assertEqual(args['parquet_path'], Path('cleaned.parquet'))

    def test_custom_parquet_input_is_used(self):
        args = self.invoke_cli('--parquet', 'other/clean.parquet')
        self.assertEqual(args['parquet_path'], Path('other/clean.parquet'))
        self.assertTrue(args['skip_etl'])

    def test_etl_requires_explicit_flag(self):
        self.assertFalse(self.invoke_cli('--run-etl')['skip_etl'])
        self.assertFalse(self.invoke_cli('--force-etl')['skip_etl'])
        self.assertTrue(self.invoke_cli('--skip-etl')['skip_etl'])

    def test_missing_parquet_fails_before_spark_start(self):
        create_session = Mock()
        namespace = dict(Path=Path, CLEAN_PARQUET_DIR=Path('missing.parquet'),
                         create_spark_session=create_session)
        run = load_def('run_end_to_end_pipeline', namespace)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                run('local[1]', 1, 1, skip_etl=True, parquet_path=Path(directory) / 'missing.parquet')
        create_session.assert_not_called()


if __name__ == '__main__':
    unittest.main()
