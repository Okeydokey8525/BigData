@echo off
chcp 65001 > nul
title Chay Web_Air Demo Dashboard
echo [*] Dang khoi dong ung dung Web_Air Demo...
cd /d "%~dp0\Web_Air"
call run.bat
