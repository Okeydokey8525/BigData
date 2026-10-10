@echo off
title Spark Worker 1 - Cu Van Vi An (100.74.10.115)
chcp 65001 >nul
echo ======================================================================
echo  KHOI CHAY APACHE SPARK WORKER 1 (CU VAN VI AN)
echo ======================================================================
echo  IP Tailscale Worker : 100.74.10.115
echo  Ket noi toi Master  : spark://100.65.205.113:7077
echo  Port Worker Web UI  : 8081
echo ======================================================================

set CURRENT_DIR=%~dp0
set SPARK_LOCAL_IP=100.74.10.115
set HADOOP_HOME=%CURRENT_DIR%hadoop
set PATH=%CURRENT_DIR%hadoop\bin;%PATH%

"%CURRENT_DIR%venv\Lib\site-packages\pyspark\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker spark://100.65.205.113:7077 -h 100.74.10.115 --webui-port 8081

pause
