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


def _last_upload_info(d: Path) -> tuple[datetime | None, int, str | None]:
    """업로드 폴더 요약: (마지막 업로드 시각, 총 파일 수, 마지막 파일명)."""
    files = _get_existing_files(d)
    if not files:
        return None, 0, None
    last_file = files[0]
    last_mtime = datetime.fromtimestamp(last_file.stat().st_mtime)
    return last_mtime, len(files), last_file.name


def _render_last_upload_card(d: Path, label: str) -> None:
    """탭 상단 마지막 업로드 정보 카드."""
    last_mtime, total, last_name = _last_upload_info(d)
    c1, c2, c3 = st.columns([1.2, 1, 2])
    with c1:
        if last_mtime:
            now = datetime.now()
            delta = now - last_mtime
            if delta.days >= 1:
                rel = f"{delta.days}일 전"
                color = "#dc2626" if delta.days >= 7 else "#ca8a04"
            elif delta.seconds >= 3600:
                rel = f"{delta.seconds // 3600}시간 전"
                color = "#16a34a"
            elif delta.seconds >= 60:
                rel = f"{delta.seconds // 60}분 전"
                color = "#16a34a"
            else:
                rel = "방금 전"
                color = "#16a34a"
            st.markdown(
                f"""
                <div style='padding:10px 14px; background:#f8fafc; border-left:4px solid {color};
                            border-radius:6px;'>
                  <div style='font-size:0.7rem; color:#64748b; font-weight:600;
                              text-transform:uppercase; letter-spacing:0.05em;'>
                    📅 마지막 업로드
                  </div>
                  <div style='font-size:1.05rem; font-weight:700; color:#0f172a;
                              margin-top:2px;'>
                    {last_mtime.strftime('%Y-%m-%d %H:%M')}
                  </div>
                  <div style='font-size:0.75rem; color:{color}; font-weight:600;
                              margin-top:2px;'>
                    {rel}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div style='padding:10px 14px; background:#fef3c7; border-left:4px solid #f59e0b;
                            border-radius:6px;'>
                  <div style='font-size:0.7rem; color:#92400e; font-weight:600;
                              text-transform:uppercase; letter-spacing:0.05em;'>
                    📅 마지막 업로드
                  </div>
                  <div style='font-size:1rem; font-weight:600; color:#92400e;
                              margin-top:4px;'>
                    아직 업로드 없음
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    with c2:
        st.markdown(
            f"""
            <div style='padding:10px 14px; background:#eff6ff; border-left:4px solid #2563eb;
                        border-radius:6px;'>
              <div style='font-size:0.7rem; color:#64748b; font-weight:600;
                          text-transform:uppercase; letter-spacing:0.05em;'>
                📦 총 파일
              </div>
              <div style='font-size:1.4rem; font-weight:700; color:#1e40af;
                          margin-top:2px;'>
                {total}개
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with c3:
        if last_name:
            st.markdown(
                f"""
                <div style='padding:10px 14px; background:#f8fafc; border:1px solid #e2e8f0;
                            border-radius:6px;'>
                  <div style='font-size:0.7rem; color:#64748b; font-weight:600;
                              text-transform:uppercase; letter-spacing:0.05em;'>
                    📄 가장 최근 파일
                  </div>
                  <div style='font-size:0.85rem; font-weight:500; color:#334155;
                              margin-top:2px; word-break:break-all;'>
                    {last_name}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


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

    # 마지막 업로드 정보
    _render_last_upload_card(SALES_UPLOAD_DIR, "쿠팡 판매")
    st.write("")

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

    # 마지막 업로드 정보
    _render_last_upload_card(ADS_UPLOAD_DIR, "쿠팡 광고")
    st.write("")

    with st.expander("📥 어디서 받나요?", expanded=False):
        st.markdown(
            """
            1. 쿠팡 **광고센터** 로그인
            2. **리포트 → 캠페인 리포트** 또는 **광고그룹 리포트**
            3. CSV/Excel 다운로드
            4. 아래에 드롭/업로드

            **권장: 일별 리포트 (`pa_daily_*`)** — 매출/광고 분석에 일별 그래프 반영됨.
            합계 리포트(`pa_total_campaign_*`)도 받으시지만 캠페인 분석 탭에만 반영됩니다.
            """
        )

    st.info(
        "💡 **두 가지 리포트 모두 지원**\n\n"
        "- **`pa_daily_*` 일별 리포트**: ads.csv + 일별 parquet 모두 갱신 → "
        "광고 분석 시계열 차트, ROAS 추이, 매출 분석 등 전체 반영\n"
        "- **`pa_total_campaign_*` 합계 리포트**: 캠페인 합계 parquet 만 갱신 → "
        "광고 분석 → 쿠팡 → 캠페인 분석 탭의 ROAS/지출 합계 반영"
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
