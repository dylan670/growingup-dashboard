"""회의록 — Notion CSV 업로드 표 형식.

운영:
  1. 노션 회의록 DB '...' → 내보내기 → CSV (또는 Markdown & CSV)
  2. 이 페이지에 파일 업로드
  3. CSV 내용 그대로 표로 표시
"""
from __future__ import annotations

import io
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.ui import setup_page


setup_page(
    page_title="회의록",
    page_icon="📝",
    header_title="📝 회의록",
    header_subtitle="Notion 회의록 DB 내보내기 (.csv 또는 .zip) 업로드",
)


ROOT = Path(__file__).parent.parent
MEETINGS_CSV = ROOT / "data" / "meetings.csv"
UPLOAD_DIR = ROOT / "data" / "meetings_upload"


# ==========================================================
# 헬퍼
# ==========================================================
def _load_csv_any_encoding(file_like_or_path) -> pd.DataFrame:
    """CSV 파일 → DataFrame, 인코딩 자동."""
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            if hasattr(file_like_or_path, "read"):
                file_like_or_path.seek(0)
                return pd.read_csv(file_like_or_path, encoding=enc)
            return pd.read_csv(file_like_or_path, encoding=enc)
        except UnicodeDecodeError:
            continue
    # 마지막 시도 — replace
    if hasattr(file_like_or_path, "read"):
        file_like_or_path.seek(0)
        return pd.read_csv(file_like_or_path, encoding="utf-8", encoding_errors="replace")
    return pd.read_csv(file_like_or_path, encoding="utf-8", encoding_errors="replace")


def _extract_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    """ZIP 안의 CSV 추출."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                with zf.open(name) as f:
                    return _load_csv_any_encoding(io.BytesIO(f.read()))
    return pd.DataFrame()


def _save_uploaded(uploaded) -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / uploaded.name
    if target.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = UPLOAD_DIR / f"{Path(uploaded.name).stem}_{ts}{Path(uploaded.name).suffix}"
    target.write_bytes(uploaded.getvalue())
    return target


def _save_meetings(df: pd.DataFrame) -> None:
    MEETINGS_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(MEETINGS_CSV, index=False, encoding="utf-8-sig")


def _load_meetings() -> pd.DataFrame:
    if not MEETINGS_CSV.exists():
        return pd.DataFrame()
    return _load_csv_any_encoding(MEETINGS_CSV)


# ==========================================================
# 업로드 영역
# ==========================================================
with st.expander("📖 Notion 에서 CSV 받는 법", expanded=False):
    st.markdown("""
    1. 노션 **회의록 DB** 페이지 진입
    2. 우상단 **"..."** → **"내보내기"**
    3. 형식: **CSV** 또는 **Markdown & CSV** (둘 다 CSV 들어있음)
    4. 다운로드 → 이 페이지에 업로드
    """)

uploaded = st.file_uploader(
    "📂 회의록 CSV 또는 ZIP 업로드",
    type=["csv", "zip"],
    accept_multiple_files=False,
)

if uploaded is not None:
    col_a, col_b = st.columns([1, 4])
    if col_a.button("💾 저장", type="primary", use_container_width=True):
        try:
            saved = _save_uploaded(uploaded)
            if saved.suffix.lower() == ".zip":
                df = _extract_csv_from_zip(saved)
                if df.empty:
                    st.error("ZIP 안에 CSV 가 없습니다.")
                else:
                    _save_meetings(df)
                    st.success(f"✅ 저장 — {len(df)}개 회의록 / {len(df.columns)}개 컬럼")
                    st.rerun()
            else:
                df = _load_csv_any_encoding(saved)
                _save_meetings(df)
                st.success(f"✅ 저장 — {len(df)}개 회의록 / {len(df.columns)}개 컬럼")
                st.rerun()
        except Exception as e:
            st.error(f"❌ {type(e).__name__}: {e}")


# ==========================================================
# 저장된 CSV 그대로 표시
# ==========================================================
df = _load_meetings()
if df.empty:
    st.info("📭 저장된 회의록이 없습니다. 위에서 CSV 업로드해주세요.")
    st.stop()

st.markdown("---")

# 상단 통계 + 관리 버튼
c1, c2, c3 = st.columns([1, 1, 1])
c1.metric("회의록", f"{len(df)}건")
c2.metric("컬럼", f"{len(df.columns)}개")
with c3:
    if st.button("🗑️ 초기화", use_container_width=True):
        if MEETINGS_CSV.exists():
            MEETINGS_CSV.unlink()
        st.rerun()

st.markdown(f"##### 📋 회의록 ({len(df)}건)")

# CSV 그대로 표 표시
st.dataframe(df, width="stretch", hide_index=True, height=min(600, 60 + len(df) * 36))

# 원본 다운로드 버튼
csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
st.download_button(
    "📥 CSV 다운로드",
    data=csv_bytes,
    file_name=f"meetings_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv",
)
