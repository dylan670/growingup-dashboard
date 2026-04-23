@echo off
cd /d "%~dp0"
if not exist "data" mkdir "data"
if not exist ".venv\Scripts\python.exe" (
    exit /b 1
)

".venv\Scripts\python.exe" "scripts\sync_naver_ads.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_naver_commerce.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_coupang.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_cafe24.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_meta_ads.py" --days 3

REM 대시보드 빠른 로드용 프리컴퓨트 (시트·캠페인·KPI·인사이트 미리 집계)
".venv\Scripts\python.exe" "scripts\precompute.py"

exit /b %ERRORLEVEL%
