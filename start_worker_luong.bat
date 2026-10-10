@echo off
title Spark Worker 0 - Le Duc Luong (100.65.205.113)
chcp 65001 >nul
echo ======================================================================
echo  KHOI CHAY APACHE SPARK WORKER (LE DUC LUONG)
echo ======================================================================
echo  IP Tailscale Worker : 100.65.205.113
echo  Ket noi toi Master  : spark://100.65.205.113:7077
echo  Port Worker Web UI  : 8082
echo ======================================================================

set CURRENT_DIR=%~dp0
set SPARK_LOCAL_IP=100.65.205.113
set HADOOP_HOME=%CURRENT_DIR%hadoop
set PATH=%CURRENT_DIR%hadoop\bin;%PATH%

"%CURRENT_DIR%venv\Lib\site-packages\pyspark\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker spark://100.65.205.113:7077 -h 100.65.205.113 --webui-port 8082

pause
