@echo off
REM ================================================================
REM 그로잉업팀 대시보드 실행 — 로컬 + 네트워크 접근 가능
REM ================================================================
cd /d "%~dp0"

echo.
echo ================================================================
echo   그로잉업팀 대시보드 실행 중...
echo ================================================================
echo.

REM 현재 PC의 로컬 IP 주소 찾기
for /f "tokens=4" %%a in ('route print ^| findstr "\<0.0.0.0\>"') do (
    set LOCAL_IP=%%a
    goto :found
)
:found

echo   로컬 접속      : http://localhost:8501
echo   같은 WiFi PC   : http://%LOCAL_IP%:8501
echo   같은 WiFi 모바일: http://%LOCAL_IP%:8501  (PC IP 로 접속)
echo.
echo   ^> 모바일/다른 PC 에서 안 열리면 Windows Defender 방화벽에서
echo     'streamlit' 또는 포트 8501 허용 필요.
echo.
echo ================================================================
echo.

".venv\Scripts\streamlit.exe" run app.py
