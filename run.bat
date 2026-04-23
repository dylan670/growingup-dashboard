@echo off
chcp 65001 > nul
cd /d "%~dp0"

echo ====================================
echo   그로잉업팀 마케팅 대시보드
echo ====================================
echo.

REM Python launcher 'py' 확인 (Windows 표준)
where py >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python launcher 'py'를 찾을 수 없습니다.
    echo https://www.python.org/downloads/ 에서 Python 3.10+ 설치 후 다시 실행하세요.
    echo 설치 시 "Add Python to PATH" + "py launcher" 체크!
    pause
    exit /b 1
)

REM 가상환경 생성 (최초 1회만)
if not exist ".venv\" (
    echo [1/3] 가상환경 생성 중... (최초 실행 시 1분 소요)
    py -3.11 -m venv .venv 2>nul
    if errorlevel 1 (
        py -m venv .venv
    )
    if errorlevel 1 (
        echo [ERROR] 가상환경 생성 실패.
        pause
        exit /b 1
    )
    call .venv\Scripts\activate.bat
    echo [2/3] 패키지 설치 중... (최초 실행 시 2~3분 소요)
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] 패키지 설치 실패. 인터넷 연결 확인 또는 이 창을 닫고 다시 실행하세요.
        pause
        exit /b 1
    )
) else (
    call .venv\Scripts\activate.bat
)

echo.
echo [3/3] 대시보드를 실행합니다.
echo 브라우저가 자동으로 열립니다 (기본: http://localhost:8501)
echo 종료하려면 이 창에서 Ctrl+C 를 누르세요.
echo.

streamlit run app.py
