@echo off
REM ================================================================
REM 그로잉업팀 대시보드 상시 실행 (크래시 자동 재시작)
REM   작업 스케줄러 "At startup" 트리거로 등록하여 PC 부팅시 자동 실행.
REM ================================================================

cd /d "%~dp0\.."
title Growing-Up Dashboard (Auto-Restart)

:loop
echo.
echo [%date% %time%] Streamlit starting...
echo.

".venv\Scripts\streamlit.exe" run app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true

echo.
echo [%date% %time%] Streamlit exited (code %ERRORLEVEL%). Restarting in 10s...
echo.
timeout /t 10 /nobreak > nul
goto loop
