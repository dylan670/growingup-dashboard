"""회의록 — Notion 페이지 그대로 렌더링.

운영:
  노션 → 페이지 또는 DB '...' → '내보내기' → 'Markdown 및 CSV'
  → ZIP 다운로드 → 이 페이지에 업로드 → Notion 페이지처럼 표시
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
    header_subtitle="Notion 'Markdown 및 CSV' export ZIP 업로드 → 페이지 그대로 표시",
)


ROOT = Path(__file__).parent.parent
MEETINGS_DIR = ROOT / "data" / "meetings"
UPLOAD_DIR = ROOT / "data" / "meetings_upload"
INDEX_CSV = MEETINGS_DIR / "_index.csv"
MD_DIR = MEETINGS_DIR / "md"
ATTACH_DIR = MEETINGS_DIR / "attachments"

UUID_RE = re.compile(r"\s+[0-9a-f]{32}", re.IGNORECASE)


# ==========================================================
# 헬퍼
# ==========================================================
def _read_csv_any(stream) -> pd.DataFrame:
    raw = stream.read() if hasattr(stream, "read") else Path(stream).read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(io.BytesIO(raw), encoding="utf-8", encoding_errors="replace")


def _clean_filename(s: str) -> str:
    """Notion export 파일명에서 UUID 제거."""
    return UUID_RE.sub("", s).strip()


def _save_uploaded(uploaded) -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / uploaded.name
    if target.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = UPLOAD_DIR / f"{Path(uploaded.name).stem}_{ts}{Path(uploaded.name).suffix}"
    target.write_bytes(uploaded.getvalue())
    return target


def _extract_zip(zip_path: Path) -> dict:
    """ZIP 내용 풀어서 정리.

    반환: {
        'csv_df': DataFrame (또는 None),
        'md_files': [{title, content_md}],
        'attachments': {filename: bytes},
    }
    """
    MD_DIR.mkdir(parents=True, exist_ok=True)
    ATTACH_DIR.mkdir(parents=True, exist_ok=True)

    csv_df = None
    md_files = []
    attachments_count = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            lower = name.lower()
            stem = Path(name).stem
            ext = Path(name).suffix.lower()

            if ext == ".csv" and csv_df is None:
                with zf.open(name) as f:
                    csv_df = _read_csv_any(f)
            elif ext == ".md":
                with zf.open(name) as f:
                    content = f.read().decode("utf-8", errors="replace")
                title = _clean_filename(stem)
                md_files.append({"title": title, "content_md": content})
                # 디스크 저장
                safe = re.sub(r"[\\/:*?\"<>|]", "_", title)[:120]
                (MD_DIR / f"{safe}.md").write_text(content, encoding="utf-8")
            elif ext in (".pdf", ".png", ".jpg", ".jpeg", ".gif", ".html", ".xlsx", ".docx"):
                with zf.open(name) as f:
                    content = f.read()
                fname = _clean_filename(Path(name).name)
                safe_fname = re.sub(r"[\\/:*?\"<>|]", "_", fname)[:120]
                (ATTACH_DIR / safe_fname).write_bytes(content)
                attachments_count += 1

    return {
        "csv_df": csv_df,
        "md_files": md_files,
        "attachments_count": attachments_count,
    }


def _save_index(df: pd.DataFrame) -> None:
    MEETINGS_DIR.mkdir(parents=True, exist_ok=True)
    if INDEX_CSV.exists():
        try:
            existing = _read_csv_any(INDEX_CSV.open("rb"))
            title_col = _detect_title_col(df) or _detect_title_col(existing)
            if title_col and title_col in existing.columns:
                existing = existing[~existing[title_col].astype(str).isin(
                    df[title_col].astype(str)
                )]
            df = pd.concat([existing, df], ignore_index=True)
        except Exception:
            pass
    df.to_csv(INDEX_CSV, index=False, encoding="utf-8-sig")


def _load_index() -> pd.DataFrame:
    if not INDEX_CSV.exists():
        return pd.DataFrame()
    return _read_csv_any(INDEX_CSV.open("rb"))


def _load_md_for_title(title: str) -> str:
    safe = re.sub(r"[\\/:*?\"<>|]", "_", title)[:120]
    p = MD_DIR / f"{safe}.md"
    if p.exists():
        return p.read_text(encoding="utf-8", errors="replace")
    return ""


def _detect_title_col(df: pd.DataFrame) -> str | None:
    for c in ("이름", "제목", "Title", "Name"):
        if c in df.columns:
            return c
    return df.columns[0] if len(df.columns) else None


def _detect_date_col(df: pd.DataFrame) -> str | None:
    for c in ("생성일자", "생성 일자", "날짜", "Date", "Created", "Created time"):
        if c in df.columns:
            return c
    return None


def _clear_all() -> None:
    for d in (MD_DIR, ATTACH_DIR):
        if d.exists():
            for f in d.iterdir():
                if f.is_file():
                    f.unlink()
    if INDEX_CSV.exists():
        INDEX_CSV.unlink()


def _strip_md_header(md: str) -> str:
    """Notion export 시 본문 첫 줄 # 제목 + property table 제거."""
    lines = md.split("\n")
    # 첫 H1 제거
    if lines and lines[0].lstrip().startswith("# "):
        lines = lines[1:]
    while lines and not lines[0].strip():
        lines = lines[1:]
    # Notion property table (| key | value |) 가 본문 맨 위에 있으면 그것까지 제거
    if lines and lines[0].lstrip().startswith("|") and len(lines) > 1:
        # 다음 빈 줄 또는 |--| 분리선 전까지가 property table
        i = 0
        while i < len(lines) and (lines[i].lstrip().startswith("|") or lines[i].strip() == ""):
            i += 1
            if i < len(lines) and not lines[i].lstrip().startswith("|") and lines[i].strip():
                break
        lines = lines[i:]
        while lines and not lines[0].strip():
            lines = lines[1:]
    return "\n".join(lines).strip()


# ==========================================================
# 업로드 영역
# ==========================================================
with st.expander("📖 사용 방법 — Notion 에서 ZIP 받기", expanded=False):
    st.markdown("""
    **노션 페이지 그대로 표시하려면 ZIP 업로드 필수입니다.**

    1. 노션에서 **회의록 DB** 또는 **개별 회의록 페이지** 진입
    2. 우상단 **"..."** → **"내보내기"** (Export)
    3. **내보내기 형식**: **"Markdown 및 CSV"** ⭐ 선택
    4. **"하위 페이지 포함"** 체크 (가능하면 — Business 플랜 필요)
    5. **"내보내기"** → ZIP 파일 다운로드
    6. 받은 ZIP 을 아래에 업로드

    **ZIP 안 들어있는 것:**
    - CSV: 회의록 목록 + 속성 (제목/팀/참석자/날짜)
    - .md: 각 회의록 본문 (text, list, checkbox, 인용 등)
    - 첨부 PDF/이미지 (있으면)

    **렌더 안 되는 것** (Notion API 필요 — admin 권한 받으면 가능):
    - 댓글 (markdown export 에 미포함)
    - 캘린더 view / 실시간 DB embed
    """)

uploaded = st.file_uploader(
    "📂 회의록 ZIP 업로드",
    type=["zip", "csv"],
    accept_multiple_files=False,
)

if uploaded is not None:
    if st.button("💾 저장 + 처리", type="primary"):
        try:
            saved = _save_uploaded(uploaded)
            if saved.suffix.lower() == ".zip":
                result = _extract_zip(saved)
                csv_df = result["csv_df"]
                md_count = len(result["md_files"])
                att_count = result["attachments_count"]
                if csv_df is None and md_count == 0:
                    st.error("ZIP 에서 CSV/Markdown 을 찾을 수 없습니다.")
                else:
                    if csv_df is not None:
                        _save_index(csv_df)
                    msg = f"✅ 저장 완료"
                    if csv_df is not None:
                        msg += f" — 회의록 {len(csv_df)}건"
                    if md_count:
                        msg += f", 본문 {md_count}개"
                    if att_count:
                        msg += f", 첨부 {att_count}개"
                    st.success(msg)
                    st.rerun()
            else:
                csv_df = _read_csv_any(saved.open("rb"))
                _save_index(csv_df)
                st.success(f"✅ CSV 저장 — {len(csv_df)}건 (본문 X)")
                st.rerun()
        except Exception as e:
            st.error(f"❌ {type(e).__name__}: {e}")


# ==========================================================
# 표시
# ==========================================================
df = _load_index()
md_available = MD_DIR.exists() and any(MD_DIR.glob("*.md"))

if df.empty and not md_available:
    st.info(
        "📭 저장된 회의록 없음.\n\n"
        "위에서 노션 export ZIP 파일을 업로드해주세요."
    )
    st.stop()

# 통계
title_col = _detect_title_col(df) if not df.empty else None
date_col = _detect_date_col(df) if not df.empty else None

# 최신순 정렬
if date_col and date_col in df.columns:
    try:
        df = df.copy()
        df["__sort"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values("__sort", ascending=False, na_position="last").drop(columns=["__sort"])
    except Exception:
        pass

st.markdown("---")

c1, c2, c3 = st.columns([1, 1, 1])
c1.metric("회의록", f"{len(df)}건")
md_count = len(list(MD_DIR.glob("*.md"))) if MD_DIR.exists() else 0
c2.metric("본문", f"{md_count}개")
with c3:
    if st.button("🗑️ 초기화", use_container_width=True):
        _clear_all()
        st.rerun()

st.write("")

# ==========================================================
# Notion 페이지 스타일 카드
# ==========================================================
NOTION_CSS = """
<style>
.notion-page {
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  padding: 36px 56px;
  margin-bottom: 24px;
}
.notion-page h1.notion-title {
  font-size: 2.2rem;
  font-weight: 800;
  color: #0f172a;
  margin: 0 0 28px 0;
  line-height: 1.1;
}
.notion-page .notion-props {
  margin-bottom: 28px;
}
.notion-page .nrow {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  padding: 5px 0;
  font-size: 0.92rem;
}
.notion-page .nkey {
  color: #64748b;
  min-width: 110px;
  display: flex;
  align-items: center;
  gap: 6px;
}
.notion-page .nval {
  color: #0f172a;
  flex: 1;
}
.chip {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 4px;
  font-size: 0.82rem;
  font-weight: 500;
  margin-right: 6px;
  background: #fef3c7;
  color: #92400e;
}
.chip.team { background: #fce7f3; color: #be185d; }
.chip.person {
  background: #e0e7ff;
  color: #4338ca;
  border-radius: 14px;
  padding: 2px 11px;
}
</style>
"""
st.markdown(NOTION_CSS, unsafe_allow_html=True)


def _format_val(val, key: str) -> str:
    """속성 값 → HTML chip."""
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    parts = [x.strip() for x in re.split(r"[,，]\s*", s) if x.strip()]
    cls = "chip"
    if "팀" in key:
        cls = "chip team"
    elif "참석" in key or "person" in key.lower():
        cls = "chip person"
    if len(parts) <= 1:
        return f'<span class="{cls}">{s}</span>'
    return "".join(f'<span class="{cls}">{p}</span>' for p in parts)


# ==========================================================
# 표시 — 가장 최근 회의록 펼친 상태 + 나머지는 collapsible
# ==========================================================
if not df.empty and title_col:
    # 사이드바에 회의록 선택 가능
    titles = df[title_col].astype(str).tolist()
    selected_idx = st.radio(
        "회의록 선택",
        options=list(range(len(titles))),
        format_func=lambda i: titles[i],
        horizontal=False,
        key="meeting_select",
        label_visibility="collapsed",
    ) if len(titles) > 1 else 0

    row = df.iloc[selected_idx]
    title = str(row.get(title_col) or "")
    icon = "🎯"

    # 속성 row HTML 생성
    prop_rows_html = ""
    for col in df.columns:
        if col == title_col:
            continue
        v = row.get(col)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        val_html = _format_val(v, col)
        if not val_html:
            continue
        prop_rows_html += (
            f'<div class="nrow">'
            f'<div class="nkey">{col}</div>'
            f'<div class="nval">{val_html}</div>'
            f'</div>'
        )

    # 페이지 카드 (제목 + 속성)
    st.markdown(
        f"""
<div class="notion-page">
  <div style="font-size:2rem; margin-bottom:14px;">{icon}</div>
  <h1 class="notion-title">{title}</h1>
  <div class="notion-props">{prop_rows_html}</div>
</div>
""",
        unsafe_allow_html=True,
    )

    # 본문 markdown — 카드 아래 별도 블록
    body = _load_md_for_title(title)
    if body:
        stripped = _strip_md_header(body)
        with st.container(border=True):
            st.markdown(stripped)
    else:
        st.info(
            "📭 이 회의록의 본문이 없습니다. "
            "노션 'Markdown 및 CSV' 형식(ZIP) 으로 export 해 업로드해주세요."
        )

    # 첨부 파일 (있으면)
    if ATTACH_DIR.exists():
        attachments = list(ATTACH_DIR.glob("*"))
        if attachments:
            with st.expander(f"📎 첨부 파일 ({len(attachments)}개)"):
                for f in attachments:
                    st.download_button(
                        f"📥 {f.name}",
                        data=f.read_bytes(),
                        file_name=f.name,
                        key=f"dl_{f.name}",
                    )
