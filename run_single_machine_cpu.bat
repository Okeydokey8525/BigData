@echo off
chcp 65001 > nul
title HUAN LUYEN 6 MO HINH THUAN MAY DON CPU - NHOM 6
color 0A

echo ==============================================================================
echo       KHOI CHAY HUAN LUYEN 6 MO HINH HOC MAY THUAN CPU TREN MAY DON
echo              BO DU LIEU 100%% 7 TRIEU DONG (KHONG SU DUNG GPU)
echo ==============================================================================
echo.
echo Cac mo hinh se huan luyen:
echo   1. Logistic Regression (SGDClassifier - CPU n_jobs=8)
echo   2. Decision Tree       (Scikit-Learn - CPU)
echo   3. Random Forest       (Scikit-Learn - CPU n_jobs=8)
echo   4. LightGBM            (Microsoft - CPU n_jobs=8)
echo   5. XGBoost             (DMLC - CPU device='cpu')
echo   6. CatBoost            (Yandex - CPU task_type='CPU')
echo.
echo Uoc tinh thoi gian: ~6 - 8 phut cho toan bo 6 mo hinh tren CPU Ryzen 7.
echo Dang khoi dong tien trinh...
echo ==============================================================================
echo.

set PYTHONIOENCODING=utf-8
".\venv\Scripts\python.exe" src/baseline/train_all_7m.py

echo.
echo ==============================================================================
echo [HOAN TAT] Huan luyen may don CPU da xong! Nhan phim bat ky de thoat...
echo ==============================================================================
pause > nul
