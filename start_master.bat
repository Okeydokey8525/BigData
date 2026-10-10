@echo off
title Spark Master - Le Duc Luong (100.65.205.113)
chcp 65001 >nul
echo ======================================================================
echo  KHOI CHAY APACHE SPARK MASTER (LE DUC LUONG)
echo ======================================================================
echo  IP Tailscale Master : 100.65.205.113
echo  Port Master RPC     : 7077
echo  Port Web UI         : 8080 (http://100.65.205.113:8080)
echo ======================================================================

set CURRENT_DIR=%~dp0
set SPARK_LOCAL_IP=100.65.205.113
set HADOOP_HOME=%CURRENT_DIR%hadoop
set PATH=%CURRENT_DIR%hadoop\bin;%PATH%

"%CURRENT_DIR%venv\Lib\site-packages\pyspark\bin\spark-class.cmd" org.apache.spark.deploy.master.Master -h 100.65.205.113 -p 7077 --webui-port 8080

pause
