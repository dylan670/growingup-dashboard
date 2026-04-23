@echo off
cd /d "%~dp0"
if not exist "data" mkdir "data"
if not exist ".venv\Scripts\python.exe" (
    exit /b 1
)

".venv\Scripts\python.exe" "scripts\sync_naver_commerce.py" --days 3
exit /b %ERRORLEVEL%
