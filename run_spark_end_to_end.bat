@echo off
chcp 65001 > nul
title PIPELINE TOAN TRINH THUAN SPARK (END-TO-END) - NHOM 6
color 0B

echo ==============================================================================
echo       KHOI CHAY PIPELINE HOC MAY PHAN TAN TOAN TRINH THUAN APACHE SPARK
echo                   CUM 3 LAPTOP TAILSCALE (100.65.205.113:7077)
echo ==============================================================================
echo.
echo Quy trinh tu dong thuc thi:
echo   [Giai doan 1] Spark Distributed ETL tren 7,079,081 dong tho CSV
echo   [Giai doan 2] Spark Feature Pipeline (StringIndexer + VectorAssembler)
echo   [Giai doan 3] Spark MLlib Distributed Training (Random Forest 50 Trees)
echo   [Giai doan 4] Danh gia toan dien (Accuracy, F1, Latency) va Luu Model
echo.
echo Dang ket noi Spark Master tai spark://100.65.205.113:7077...
echo.

set PYTHONIOENCODING=utf-8
".\venv\Scripts\python.exe" src/spark_ml/run_end_to_end_spark.py --master spark://100.65.205.113:7077 %*

echo.
echo ==============================================================================
echo [HOAN TAT] Pipeline da thuc thi xong! Nhan phim bat ky de thoat...
echo ==============================================================================
pause > nul
