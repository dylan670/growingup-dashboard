"""회의록 — Notion 공개 페이지 iframe 임베드.

운영:
  노션 페이지 우상단 '공유' → '웹에 게시' → URL 받기 → 여기에 등록
  → 대시보드에서 노션 페이지 그대로 100% 표시 (실시간 갱신)
"""
from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED


setup_page(
    page_title="회의록",
    page_icon="📝",
    header_title="📝 회의록",
    header_subtitle="Notion 회의록 페이지 — 실시간 임베드 (100% 일치)",
)


ROOT = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "data" / "notion_meetings_url.txt"


# ==========================================================
# URL 저장/로드
# ==========================================================
def _load_url() -> str:
    # 우선순위: 환경변수 → 로컬 파일
    env_url = os.getenv("NOTION_MEETINGS_PUBLIC_URL", "").strip()
    if env_url:
        return env_url
    if CONFIG_FILE.exists():
        try:
            return CONFIG_FILE.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


def _save_url(url: str) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(url.strip(), encoding="utf-8")


def _clear_url() -> None:
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()


# ==========================================================
# URL 입력 / embed
# ==========================================================
notion_url = _load_url()

if not notion_url:
    st.info(
        "📢 노션 회의록 페이지 URL 을 등록하면 대시보드에 그대로 임베드됩니다.\n\n"
        "**준비 — 노션 페이지 공개 발행**\n"
        "1. 노션 회의록 페이지 우상단 **\"공유\"** 클릭\n"
        "2. **\"웹에 게시\"** 토글 활성화\n"
        "3. 옵션: 검색 엔진 표시 끔 / 편집 허용 끔 / 댓글 허용 자유\n"
        "4. **\"링크 복사\"** 클릭\n"
        "5. 복사한 URL (`https://...notion.site/...`) 을 아래에 붙여넣기"
    )

    with st.form("notion_url_setup"):
        url_input = st.text_input(
            "Notion 공개 페이지 URL",
            placeholder="https://openhan.notion.site/...",
        )
        if st.form_submit_button("💾 저장", type="primary"):
            if not url_input.strip().startswith("http"):
                st.error("URL 형식 오류")
            else:
                _save_url(url_input.strip())
                st.success("저장 완료 — 페이지 새로고침")
                st.rerun()
    st.stop()


# ==========================================================
# iframe embed
# ==========================================================
col_a, col_b, col_c = st.columns([4, 1, 1])
with col_a:
    st.markdown(
        f"<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
        f"border-radius:6px; font-size:0.85rem;'>"
        f"🟢 <b>임베드 모드</b> — 노션 페이지 실시간 표시 · "
        f"<a href='{notion_url}' target='_blank' style='color:#2563eb;'>"
        f"🔗 새 탭에서 열기</a>"
        f"</div>",
        unsafe_allow_html=True,
    )
with col_b:
    if st.button("🔄 새로고침", use_container_width=True):
        st.rerun()
with col_c:
    if st.button("⚙️ URL 변경", use_container_width=True):
        _clear_url()
        st.rerun()

st.write("")

# 높이 조절 슬라이더
height = st.slider(
    "iframe 높이 (px)",
    min_value=600,
    max_value=3000,
    value=1400,
    step=100,
    key="iframe_height",
)

# iframe 임베드 — Notion 공개 페이지
components.iframe(notion_url, height=height, scrolling=True)

st.caption(
    "💡 노션에서 페이지 수정하면 새로고침 시 자동 반영됩니다. "
    "댓글/캘린더/DB embed 도 그대로 작동."
)
