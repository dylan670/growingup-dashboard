@echo off
REM ================================================================
REM 매일 오전 10시 자동 동기화 (Windows 작업 스케줄러)
REM   1. 5개 API 로 로컬 IP(한국)에서 데이터 수집
REM   2. Precompute (Parquet/JSON 집계)
REM   3. Git commit + push → Streamlit Cloud 자동 재배포
REM ================================================================

cd /d "%~dp0"
if not exist "data" mkdir "data"
if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] .venv\Scripts\python.exe not found
    exit /b 1
)

echo [%date% %time%] ============ SYNC START ============

REM 1. 5개 API 동기화 (한국 IP 에서 호출 — 403 회피)
".venv\Scripts\python.exe" "scripts\sync_naver_ads.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_naver_commerce.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_coupang.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_cafe24.py" --days 3
".venv\Scripts\python.exe" "scripts\sync_meta_ads.py" --days 3

REM 2. Precompute
".venv\Scripts\python.exe" "scripts\precompute.py"

REM 3. Git auto commit + push → Streamlit Cloud 자동 재배포
REM    주의: .gitignore 로 orders.csv, cafe24_tokens.json 은 제외
REM    공개 가능 파일만 커밋 (ads.csv, precomputed/ 등)
echo [%date% %time%] Git commit + push 시작...

REM Cafe24 토큰 갱신시 secret 업데이트 (로컬 파일만, git 에는 안 올라감)
REM 이후 자동 갱신되는 토큰은 data/cafe24_tokens.json 에 저장됨

REM Git 설정 확인 및 push
"C:\Program Files\Git\cmd\git.exe" add data/ 2>nul
"C:\Program Files\Git\cmd\git.exe" diff --staged --quiet
if %ERRORLEVEL% NEQ 0 (
    for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set TODAY=%%a-%%b-%%c
    "C:\Program Files\Git\cmd\git.exe" commit -m "🔄 Daily sync %TODAY%" 2>&1
    "C:\Program Files\Git\cmd\git.exe" push 2>&1
    echo [%date% %time%] Push 완료 - Streamlit Cloud 재배포 트리거됨
) else (
    echo [%date% %time%] 데이터 변경 없음 - commit 스킵
)

echo [%date% %time%] ============ SYNC END ============
exit /b 0
