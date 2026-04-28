"""CSV 업로드 — 수동 다운로드 CSV 파일 자동 처리.

지원 파일 종류:
  1. 쿠팡 판매 CSV (Supplier Hub)
     - 출처: https://supplier.coupang.com/ → 애널리틱스 → 판매 분석
     - 저장 위치: data/coupang_sales_upload/
     - 처리 스크립트: scripts/sync_coupang_sales_csv.py
     - 결과: data/coupang_inbound.csv 누적 병합

  2. 쿠팡 광고 CSV (광고센터)
     - 출처: 쿠팡 광고센터 → 리포트 → CSV/Excel 다운로드
     - 저장 위치: data/coupang_ads_upload/
     - 처리 스크립트: scripts/sync_coupang_ads_csv.py
     - 결과: data/ads.csv + 캠페인 parquet
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

from utils.ui import setup_page

ROOT = Path(__file__).parent.parent
SALES_UPLOAD_DIR = ROOT / "data" / "coupang_sales_upload"
ADS_UPLOAD_DIR = ROOT / "data" / "coupang_ads_upload"
LOG_FILE = ROOT / "data" / "sync_log.txt"
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"

setup_page(
    page_title="CSV 업로드",
    page_icon="📤",
    header_title="📤 CSV 업로드",
    header_subtitle=(
        "수동 다운로드 CSV/Excel 파일을 업로드하면 자동으로 파싱·병합됩니다."
    ),
)


# ==========================================================
# 환경 체크
# ==========================================================
is_local = VENV_PYTHON.exists()
if not is_local:
    st.warning(
        "⚠️ 이 기능은 **로컬 PC 전용**입니다.\n\n"
        "Streamlit Cloud 환경에선 파일 시스템에 쓰기/스크립트 실행이 제한되어 "
        "동작하지 않습니다. 로컬 PC에서 실행 중인 대시보드에 접속해주세요."
    )


# ==========================================================
# 공용 처리 헬퍼
# ==========================================================
def _save_uploaded_file(uploaded, target_dir: Path) -> Path:
    """업로드된 파일을 디렉토리에 저장. 파일명 충돌 시 timestamp 추가."""
    target_dir.mkdir(parents=True, exist_ok=True)
    name = uploaded.name
    target = target_dir / name
    if target.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = target.stem
        suffix = target.suffix
        target = target_dir / f"{stem}_{ts}{suffix}"
    target.write_bytes(uploaded.getvalue())
    return target


def _run_sync_script(script_name: str) -> tuple[bool, str]:
    """sync 스크립트 실행 + 출력 캡처.

    Returns:
        (success, combined_output)
    """
    script_path = ROOT / "scripts" / script_name
    if not script_path.exists():
        return False, f"스크립트 없음: {script_path}"
    if not VENV_PYTHON.exists():
        return False, f".venv 없음: {VENV_PYTHON}"

    try:
        result = subprocess.run(
            [str(VENV_PYTHON), str(script_path)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,   # 5분
        )
        out = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
        return result.returncode == 0, out
    except subprocess.TimeoutExpired:
        return False, "5분 timeout — CSV 너무 크거나 처리 멈춤. 콘솔에서 직접 실행해보세요."
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _run_precompute() -> tuple[bool, str]:
    """precompute 재실행 (사용자 화면 즉시 갱신용)."""
    return _run_sync_script("precompute.py")


def _get_existing_files(d: Path) -> list[Path]:
    """업로드 폴더의 기존 파일 목록 (최근순)."""
    if not d.exists():
        return []
    files = [
        f for f in d.iterdir()
        if f.is_file() and f.suffix.lower() in (".csv", ".xlsx", ".xls")
    ]
    return sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)


# ==========================================================
# 탭 구성
# ==========================================================
tab_sales, tab_ads, tab_help = st.tabs([
    "🛒 쿠팡 판매 CSV",
    "📣 쿠팡 광고 CSV",
    "📖 사용 가이드",
])


# ----------------------------------------------------------
# 🛒 쿠팡 판매 CSV (Supplier Hub)
# ----------------------------------------------------------
with tab_sales:
    st.markdown("### 🛒 쿠팡 판매 CSV (Supplier Hub)")
    st.caption(
        "쿠팡 로켓배송(벤더 발주) 매출은 Wing API 미지원 → "
        "Supplier Hub 매출 리포트 CSV 를 업로드해야 함."
    )

    with st.expander("📥 어디서 받나요?", expanded=False):
        st.markdown(
            """
            1. https://supplier.coupang.com/ 로그인
            2. **애널리틱스 → 판매 분석**
            3. **일별 × 상품별 리포트** CSV 다운로드
            4. 아래에 드롭/업로드
            """
        )

    uploaded_sales = st.file_uploader(
        "📂 CSV/Excel 파일 (다중 업로드 가능)",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
        key="sales_uploader",
        disabled=not is_local,
    )

    if uploaded_sales and is_local:
        col_a, col_b = st.columns([1, 1])
        with col_a:
            run_btn = st.button(
                "💾 저장 + 자동 처리",
                key="sales_run",
                type="primary",
                use_container_width=True,
                help="파일 저장 → sync_coupang_sales_csv.py 실행 → precompute 자동 갱신",
            )
        with col_b:
            save_only = st.button(
                "📦 저장만 (나중에 일괄 처리)",
                key="sales_save_only",
                use_container_width=True,
            )

        if run_btn or save_only:
            saved = []
            for up in uploaded_sales:
                p = _save_uploaded_file(up, SALES_UPLOAD_DIR)
                saved.append(p)
                st.success(f"💾 저장: {p.name} ({up.size:,} bytes)")

            if run_btn:
                with st.spinner("쿠팡 판매 CSV 파싱 + 병합 중... (1~3분)"):
                    ok, output = _run_sync_script("sync_coupang_sales_csv.py")
                if ok:
                    st.success("✅ 처리 완료! coupang_inbound.csv 에 병합되었습니다.")
                else:
                    st.error("❌ 처리 실패 — 아래 로그 확인.")

                with st.expander("📜 실행 로그", expanded=not ok):
                    st.code(output[-3000:] if len(output) > 3000 else output)

                if ok:
                    with st.spinner("프리컴퓨트 갱신 중... (1분)"):
                        pc_ok, pc_out = _run_precompute()
                    if pc_ok:
                        st.success("✅ 대시보드 데이터도 즉시 반영됨!")
                        st.cache_data.clear()
                    else:
                        st.warning(
                            "⚠️ 데이터는 저장됐지만 precompute 실패 — "
                            "잠시 뒤 자동 sync에서 정리됩니다."
                        )

    st.divider()

    # 기존 업로드된 파일 목록
    existing = _get_existing_files(SALES_UPLOAD_DIR)
    if existing:
        st.markdown(f"#### 📁 기존 업로드된 파일 ({len(existing)}개)")
        for f in existing[:20]:
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%m/%d %H:%M")
            size_kb = f.stat().st_size / 1024
            st.caption(f"• `{f.name}` — {size_kb:,.1f} KB · {mtime}")
        if len(existing) > 20:
            st.caption(f"…외 {len(existing) - 20}개")
    else:
        st.info("아직 업로드된 파일이 없습니다.")


# ----------------------------------------------------------
# 📣 쿠팡 광고 CSV
# ----------------------------------------------------------
with tab_ads:
    st.markdown("### 📣 쿠팡 광고 CSV")
    st.caption(
        "쿠팡 광고센터의 캠페인/리포트 CSV 를 업로드 → "
        "ads.csv 에 누적 병합 + 캠페인 parquet 갱신."
    )

    with st.expander("📥 어디서 받나요?", expanded=False):
        st.markdown(
            """
            1. 쿠팡 **광고센터** 로그인
            2. **리포트 → 캠페인 리포트** 또는 **광고그룹 리포트**
            3. CSV/Excel 다운로드
            4. 아래에 드롭/업로드
            """
        )

    uploaded_ads = st.file_uploader(
        "📂 CSV/Excel 파일 (다중 업로드 가능)",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
        key="ads_uploader",
        disabled=not is_local,
    )

    if uploaded_ads and is_local:
        col_a, col_b = st.columns([1, 1])
        with col_a:
            run_btn = st.button(
                "💾 저장 + 자동 처리",
                key="ads_run",
                type="primary",
                use_container_width=True,
            )
        with col_b:
            save_only = st.button(
                "📦 저장만",
                key="ads_save_only",
                use_container_width=True,
            )

        if run_btn or save_only:
            saved = []
            for up in uploaded_ads:
                p = _save_uploaded_file(up, ADS_UPLOAD_DIR)
                saved.append(p)
                st.success(f"💾 저장: {p.name} ({up.size:,} bytes)")

            if run_btn:
                with st.spinner("쿠팡 광고 CSV 파싱 + 병합 중... (1~2분)"):
                    ok, output = _run_sync_script("sync_coupang_ads_csv.py")
                if ok:
                    st.success("✅ 처리 완료! ads.csv + 캠페인 parquet 갱신됨.")
                else:
                    st.error("❌ 처리 실패 — 아래 로그 확인.")

                with st.expander("📜 실행 로그", expanded=not ok):
                    st.code(output[-3000:] if len(output) > 3000 else output)

                if ok:
                    with st.spinner("프리컴퓨트 갱신 중..."):
                        pc_ok, _ = _run_precompute()
                    if pc_ok:
                        st.success("✅ 광고 분석에 즉시 반영됨!")
                        st.cache_data.clear()

    st.divider()

    existing = _get_existing_files(ADS_UPLOAD_DIR)
    if existing:
        st.markdown(f"#### 📁 기존 업로드된 파일 ({len(existing)}개)")
        for f in existing[:20]:
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%m/%d %H:%M")
            size_kb = f.stat().st_size / 1024
            st.caption(f"• `{f.name}` — {size_kb:,.1f} KB · {mtime}")
        if len(existing) > 20:
            st.caption(f"…외 {len(existing) - 20}개")
    else:
        st.info("아직 업로드된 파일이 없습니다.")


# ----------------------------------------------------------
# 📖 사용 가이드
# ----------------------------------------------------------
with tab_help:
    st.markdown(
        """
        ### 📖 사용 가이드

        #### 🔄 자동 처리 흐름
        1. **파일 업로드** → 자동으로 해당 폴더(`data/coupang_*_upload/`)에 저장
        2. **"저장 + 자동 처리"** 버튼 클릭
            - 백엔드: `scripts/sync_coupang_*_csv.py` 실행
            - CSV/Excel 자동 파싱 → orders.csv / coupang_inbound.csv / ads.csv 누적 병합
            - 같은 (날짜·상품·광고) 키는 새 데이터로 갱신, 과거 데이터 보존
        3. **프리컴퓨트 자동 실행** → 매출 분석/제품 분석/광고 분석에 즉시 반영
        4. **캐시 비움** → 페이지 새로고침 시 최신 데이터 표시

        #### 📤 수동 처리 (CLI 사용 시)
        업로드만 하고 처리는 따로 하고 싶다면:
        ```bash
        # 쿠팡 판매
        .venv\\Scripts\\python.exe scripts\\sync_coupang_sales_csv.py

        # 쿠팡 광고
        .venv\\Scripts\\python.exe scripts\\sync_coupang_ads_csv.py
        ```

        #### ⚠️ Streamlit Cloud 제한
        Cloud 환경에선 **파일 시스템 쓰기 + 스크립트 실행이 제한**되어 동작하지 않습니다.
        반드시 **로컬 PC**에서 실행 중인 대시보드에서 사용하세요.

        #### 📊 결과 확인
        - **쿠팡 판매 CSV** → 📦 제품 분석 → 쿠팡 로켓배송 매출 반영
        - **쿠팡 광고 CSV** → 📣 광고 분석 → 쿠팡 광고 캠페인/지표 반영
        """
    )

    st.divider()

    # sync_log.txt 최근 부분
    if LOG_FILE.exists():
        with st.expander("🪵 최근 sync 로그 (마지막 50줄)", expanded=False):
            try:
                lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
                recent = lines[-50:]
                st.code("\n".join(recent))
            except Exception as e:
                st.caption(f"로그 읽기 실패: {e}")
