"""회의록 — Notion-style 카드 뷰.

운영:
  1. 노션 회의록 DB 또는 개별 회의록 페이지 '...' → 내보내기
  2. 형식: 'Markdown & CSV' 선택 (본문 포함 ZIP)
  3. 이 페이지에 ZIP 업로드 → 노션 페이지처럼 렌더링
"""
from __future__ import annotations

import io
import re
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT


setup_page(
    page_title="회의록",
    page_icon="📝",
    header_title="📝 회의록",
    header_subtitle="Notion 회의록 — 'Markdown 및 CSV' 내보내기 (ZIP) 업로드",
)


ROOT = Path(__file__).parent.parent
MEETINGS_DIR = ROOT / "data" / "meetings"
UPLOAD_DIR = ROOT / "data" / "meetings_upload"
CSV_CACHE = MEETINGS_DIR / "_index.csv"
MD_CACHE_DIR = MEETINGS_DIR / "md"


# ==========================================================
# 헬퍼
# ==========================================================
def _load_csv_any(file_like_or_path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            if hasattr(file_like_or_path, "seek"):
                file_like_or_path.seek(0)
            return pd.read_csv(file_like_or_path, encoding=enc)
        except UnicodeDecodeError:
            continue
    if hasattr(file_like_or_path, "seek"):
        file_like_or_path.seek(0)
    return pd.read_csv(file_like_or_path, encoding="utf-8", encoding_errors="replace")


def _save_uploaded(uploaded) -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / uploaded.name
    if target.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = UPLOAD_DIR / f"{Path(uploaded.name).stem}_{ts}{Path(uploaded.name).suffix}"
    target.write_bytes(uploaded.getvalue())
    return target


def _extract_zip(zip_path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """ZIP → (CSV DataFrame, {제목: markdown 본문})."""
    df: pd.DataFrame | None = None
    md_map: dict[str, str] = {}
    uuid_re = re.compile(r"\s+[0-9a-f]{32}$", re.IGNORECASE)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            lower = name.lower()
            if lower.endswith(".csv") and df is None:
                with zf.open(name) as f:
                    df = _load_csv_any(io.BytesIO(f.read()))
            elif lower.endswith(".md"):
                with zf.open(name) as f:
                    text = f.read().decode("utf-8", errors="replace")
                stem = Path(name).stem
                clean = uuid_re.sub("", stem).strip()
                md_map[clean] = text

    if df is None:
        df = pd.DataFrame()
    return df, md_map


def _persist(df: pd.DataFrame, md_map: dict[str, str], merge: bool = True) -> None:
    """CSV index + .md 파일들 디스크 저장 (누적)."""
    MEETINGS_DIR.mkdir(parents=True, exist_ok=True)
    MD_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # md 파일 저장 (제목.md)
    for title, body in md_map.items():
        safe_name = re.sub(r"[\\/:*?\"<>|]", "_", title)[:120]
        (MD_CACHE_DIR / f"{safe_name}.md").write_text(body, encoding="utf-8")

    # CSV 인덱스 누적 병합
    if merge and CSV_CACHE.exists():
        try:
            existing = _load_csv_any(CSV_CACHE)
            # 제목 컬럼 자동 찾기
            title_col = _detect_title_col(df) or _detect_title_col(existing)
            if title_col and title_col in existing.columns:
                existing = existing[~existing[title_col].astype(str).isin(
                    df[title_col].astype(str)
                )]
            df = pd.concat([existing, df], ignore_index=True)
        except Exception:
            pass

    df.to_csv(CSV_CACHE, index=False, encoding="utf-8-sig")


def _detect_title_col(df: pd.DataFrame) -> str | None:
    for c in ("이름", "제목", "Name", "Title"):
        if c in df.columns:
            return c
    return df.columns[0] if len(df.columns) else None


def _detect_date_col(df: pd.DataFrame) -> str | None:
    for c in ("생성일자", "생성 일자", "날짜", "Date", "Created", "Created time"):
        if c in df.columns:
            return c
    return None


def _load_index() -> tuple[pd.DataFrame, dict[str, str]]:
    if not CSV_CACHE.exists():
        return pd.DataFrame(), {}
    df = _load_csv_any(CSV_CACHE)
    md_map: dict[str, str] = {}
    if MD_CACHE_DIR.exists():
        for p in MD_CACHE_DIR.glob("*.md"):
            md_map[p.stem] = p.read_text(encoding="utf-8", errors="replace")
    return df, md_map


def _clear_all() -> None:
    if CSV_CACHE.exists():
        CSV_CACHE.unlink()
    if MD_CACHE_DIR.exists():
        for p in MD_CACHE_DIR.glob("*.md"):
            p.unlink()


def _strip_md_title(text: str, title: str) -> str:
    """markdown 본문 맨 위 H1 제목 + property table 제거 (중복 표시 방지)."""
    lines = text.split("\n")
    # 첫 줄이 # 제목 이면 제거
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    # 빈 줄 제거
    while lines and not lines[0].strip():
        lines = lines[1:]
    # Notion export 의 property dl 블록 (\| key \| value \|) 또는 property 라인 제거
    # 단순화: 본문 그대로 둠
    return "\n".join(lines).strip()


# ==========================================================
# 업로드 영역
# ==========================================================
with st.expander("📖 어떻게 받나요?", expanded=False):
    st.markdown("""
    1. 노션 **회의록 DB** 또는 개별 회의록 페이지 진입
    2. 우상단 **"..."** → **"내보내기"**
    3. **내보내기 형식**: **"Markdown 및 CSV"** ⭐ 선택
    4. **하위 페이지 포함** 체크 (가능하면 — Business 플랜)
    5. 다운로드된 **ZIP** 파일을 아래에 업로드

    *CSV만 받으면 속성(제목/팀/참석자/날짜)만 보이고 본문 X.*
    *ZIP 으로 받으셔야 노션 페이지처럼 본문까지 표시됩니다.*
    """)

uploaded = st.file_uploader(
    "📂 회의록 ZIP 업로드 (Notion 'Markdown 및 CSV' export)",
    type=["zip", "csv"],
    accept_multiple_files=False,
)

if uploaded is not None:
    if st.button("💾 저장", type="primary"):
        try:
            saved = _save_uploaded(uploaded)
            if saved.suffix.lower() == ".zip":
                df_new, md_map_new = _extract_zip(saved)
            else:
                df_new = _load_csv_any(saved)
                md_map_new = {}

            if df_new.empty:
                st.error("CSV 를 찾을 수 없습니다.")
            else:
                _persist(df_new, md_map_new, merge=True)
                msg = f"✅ {len(df_new)}개 회의록 저장"
                if md_map_new:
                    msg += f" (본문 {len(md_map_new)}개 포함)"
                st.success(msg)
                st.rerun()
        except Exception as e:
            st.error(f"❌ {type(e).__name__}: {e}")


# ==========================================================
# 저장된 회의록 표시
# ==========================================================
df, md_map = _load_index()

if df.empty:
    st.info("📭 저장된 회의록이 없습니다. 위에서 ZIP 업로드해주세요.")
    st.stop()

title_col = _detect_title_col(df)
date_col = _detect_date_col(df)

# 정렬: 최신순
if date_col and date_col in df.columns:
    try:
        df = df.copy()
        df["__sort_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values("__sort_date", ascending=False, na_position="last")
        df = df.drop(columns=["__sort_date"])
    except Exception:
        pass

st.markdown("---")

# 통계 + 초기화
c1, c2, c3 = st.columns([1, 1, 1])
c1.metric("회의록", f"{len(df)}건")
c2.metric("본문 포함", f"{len(md_map)}개")
with c3:
    if st.button("🗑️ 초기화", use_container_width=True):
        _clear_all()
        st.rerun()

st.markdown(f"##### 📋 회의록 ({len(df)}건, 최신순)")
st.write("")


# ==========================================================
# Notion-style 카드 렌더링
# ==========================================================
PROPERTY_CHIP_CSS = """
<style>
.notion-card {
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 28px 36px;
  margin-bottom: 20px;
  box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}
.notion-icon {
  font-size: 2.2rem;
  margin-bottom: 12px;
}
.notion-title {
  font-size: 1.9rem;
  font-weight: 800;
  color: #0f172a;
  line-height: 1.2;
  margin: 0 0 22px 0;
}
.notion-prop-row {
  display: flex;
  align-items: flex-start;
  gap: 16px;
  padding: 6px 0;
  font-size: 0.9rem;
}
.notion-prop-key {
  color: #64748b;
  width: 130px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  gap: 6px;
}
.notion-prop-val {
  color: #0f172a;
  flex: 1;
}
.notion-chip {
  display: inline-block;
  padding: 2px 10px;
  background: #fef3c7;
  color: #92400e;
  border-radius: 4px;
  font-size: 0.82rem;
  font-weight: 500;
  margin-right: 4px;
}
.notion-chip-team {
  background: #fce7f3;
  color: #be185d;
}
.notion-chip-person {
  background: #e0e7ff;
  color: #4338ca;
  border-radius: 12px;
}
.notion-body {
  margin-top: 22px;
  padding-top: 20px;
  border-top: 1px solid #f1f5f9;
  color: #1e293b;
  font-size: 0.95rem;
  line-height: 1.7;
}
</style>
"""
st.markdown(PROPERTY_CHIP_CSS, unsafe_allow_html=True)


def _is_blank(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    s = str(v).strip()
    return s == "" or s.lower() == "nan"


def _render_value(val: str, key: str) -> str:
    """속성 value → 칩 형태 HTML."""
    s = str(val).strip()
    # 쉼표 split (multi-select / people)
    items = [x.strip() for x in re.split(r"[,，]\s*", s) if x.strip()]
    chip_class = "notion-chip"
    if "팀" in key:
        chip_class = "notion-chip notion-chip-team"
    elif "참석" in key or "person" in key.lower():
        chip_class = "notion-chip notion-chip-person"

    if len(items) <= 1:
        return f'<span class="notion-chip-val">{s}</span>'
    return "".join(f'<span class="{chip_class}">{i}</span>' for i in items)


for i, row in df.reset_index(drop=True).iterrows():
    title = str(row.get(title_col) or "(제목 없음)").strip()
    # 본문 매칭 (제목 키)
    body_md = md_map.get(re.sub(r"[\\/:*?\"<>|]", "_", title)[:120], "")
    if not body_md:
        body_md = md_map.get(title, "")

    # 속성 dict (title 컬럼 제외)
    props = []
    for c in df.columns:
        if c == title_col:
            continue
        v = row.get(c)
        if _is_blank(v):
            continue
        props.append((c, str(v).strip()))

    # 페이지 카드 HTML
    icon = "🎯"
    prop_html = ""
    for k, v in props:
        prop_html += (
            f'<div class="notion-prop-row">'
            f'<div class="notion-prop-key">{k}</div>'
            f'<div class="notion-prop-val">{_render_value(v, k)}</div>'
            f'</div>'
        )

    body_html = ""
    if body_md:
        stripped = _strip_md_title(body_md, title)
        # markdown → streamlit 의 native markdown 으로 표시
        # HTML 안에 못 넣으니 카드 prop 부분만 unsafe_allow_html 으로 표시 후
        # 본문은 st.markdown 으로 별도 출력
        body_html = ""   # placeholder

    card_html = f"""
<div class="notion-card">
  <div class="notion-icon">{icon}</div>
  <div class="notion-title">{title}</div>
  {prop_html}
</div>
"""
    st.markdown(card_html, unsafe_allow_html=True)

    if body_md:
        stripped = _strip_md_title(body_md, title)
        with st.container():
            st.markdown(
                "<div style='margin: -20px 36px 30px 36px; padding: 20px 0; "
                "background:#ffffff; border-top:1px solid #f1f5f9;'>",
                unsafe_allow_html=True,
            )
            st.markdown(stripped)
            st.markdown("</div>", unsafe_allow_html=True)
