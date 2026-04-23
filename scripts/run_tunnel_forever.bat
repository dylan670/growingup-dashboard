@echo off
REM ================================================================
REM Cloudflare Tunnel 상시 실행 (크래시 자동 재시작)
REM   외부(사무실 밖) 접근 원할 때만 사용. cloudflared 설치 필요.
REM ================================================================

cd /d "%~dp0\.."
title Cloudflare Tunnel (Auto-Restart)

:loop
echo.
echo [%date% %time%] Cloudflared starting...
echo.

REM cloudflared 경로 자동 탐색 (PATH 또는 C:\tools\)
where cloudflared >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    cloudflared tunnel --url http://localhost:8501
) else if exist "C:\tools\cloudflared.exe" (
    "C:\tools\cloudflared.exe" tunnel --url http://localhost:8501
) else (
    echo.
    echo [ERROR] cloudflared 를 찾을 수 없습니다.
    echo   1. https://github.com/cloudflare/cloudflared/releases 에서 다운로드
    echo   2. C:\tools\cloudflared.exe 로 저장 (또는 PATH 에 추가)
    echo.
    timeout /t 60 /nobreak > nul
    goto loop
)

echo.
echo [%date% %time%] Cloudflared exited. Restarting in 10s...
echo.
timeout /t 10 /nobreak > nul
goto loop
