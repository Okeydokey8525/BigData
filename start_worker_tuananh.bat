@echo off
title Spark Worker 2 - Tran Huynh Tuan Anh (100.112.107.123)
chcp 65001 >nul
echo ======================================================================
echo  KHOI CHAY APACHE SPARK WORKER 2 (TRAN HUYNH TUAN ANH)
echo ======================================================================
echo  IP Tailscale Worker : 100.112.107.123
echo  Ket noi toi Master  : spark://100.65.205.113:7077
echo  Port Worker Web UI  : 8081
echo ======================================================================

set CURRENT_DIR=%~dp0
set SPARK_LOCAL_IP=100.112.107.123
set HADOOP_HOME=%CURRENT_DIR%hadoop
set PATH=%CURRENT_DIR%hadoop\bin;%PATH%

"%CURRENT_DIR%venv\Lib\site-packages\pyspark\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker spark://100.65.205.113:7077 -h 100.112.107.123 --webui-port 8081

pause
