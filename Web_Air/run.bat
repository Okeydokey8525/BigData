@echo off
chcp 65001 > nul
title Web_Air - Flight Delay AI Platform (HUIT Big Data 2024)

echo ===============================================================================
echo   WEB_AIR: HE THONG DU BAO & DOI SANH MO HINH TRE CHUYEN BAY 7M
echo   Hoc phan: Nhap mon Big Data - Nhom 6 (TS. Phan Ho Viet Truong)
echo ===============================================================================
echo.
echo [*] Dang khoi dong Web Server FastAPI tai dia chi: http://127.0.0.1:8000 ...
echo [*] Trinh duyet se tu dong mo trong giay lat...
echo.

cd /d "%~dp0"

:: Kiem tra va uu tien dung Python tu moi truong ao venv
if exist "..\venv\Scripts\python.exe" (
    set "PY_CMD=..\venv\Scripts\python.exe"
) else (
    set "PY_CMD=python"
)

:: Mo trinh duyet sau 2 giay
start "" http://127.0.0.1:8000

:: Chay FastAPI Uvicorn
"%PY_CMD%" -m uvicorn app:app --host 127.0.0.1 --port 8000 --reload

pause
