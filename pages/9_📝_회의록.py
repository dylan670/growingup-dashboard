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


def _normalize_to_embed_url(url: str) -> str:
    """일반 notion.site URL → 임베드 전용 /ebd/ URL.

    예:
      https://workspace.notion.site/page-title-32hexchars
      https://workspace.notion.site/32hexchars
      → https://workspace.notion.site/ebd/32hexchars
    이미 /ebd/ 형식이면 그대로 (단 // 같은 오타는 교정).
    """
    import re
    s = url.strip().rstrip("/")
    # 슬래시 연속 교정 (https:// 는 보존)
    s = re.sub(r"(?<!:)/+", "/", s)

    # page ID 추출 — URL 마지막 path segment 의 끝부분 32자리 hex
    # 슬러그-pageid 패턴 ('5-3-abc...32hex') 도 정확히 처리
    m = re.search(r"([0-9a-f]{32})(?:[/?#]|$)", s)
    if not m:
        # 하이픈 포함 36자 UUID 패턴 (8-4-4-4-12)
        m2 = re.search(
            r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
            s,
        )
        if not m2:
            return s
        page_id = m2.group(1).replace("-", "")
    else:
        page_id = m.group(1)

    # workspace 추출
    wm = re.match(r"(https?://[^/]+)/", s)
    if not wm:
        return s
    base = wm.group(1)
    return f"{base}/ebd/{page_id}"


def _save_url(url: str) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(_normalize_to_embed_url(url), encoding="utf-8")


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
# 노션 원본 (notion.so) URL — 로그인 상태로 열기용
# ==========================================================
def _to_notion_so_url(embed_url: str) -> str:
    """ebd URL → notion.so URL (사용자 로그인 상태로 열림)."""
    import re
    m = re.search(r"([0-9a-f]{32})", embed_url)
    if not m:
        return embed_url
    page_id = m.group(1)
    # 워크스페이스 추출
    wm = re.match(r"https?://([^.]+)\.notion\.site", embed_url)
    workspace = wm.group(1) if wm else "openhan"
    return f"https://www.notion.so/{workspace}/{page_id}"


notion_so_url = _to_notion_so_url(notion_url)


# ==========================================================
# 상단 — 노션 원본 열기 큼직한 버튼 (로그인 상태로)
# ==========================================================
st.markdown(
    f"""
<a href="{notion_so_url}" target="_blank" style="text-decoration:none;">
<div style="
    background: linear-gradient(135deg, #2563eb 0%, #7c3aed 100%);
    color: white;
    padding: 20px 28px;
    border-radius: 12px;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 2px 8px rgba(37,99,235,0.25);
    cursor: pointer;
    transition: transform 0.1s;
">
  <div>
    <div style="font-size:1.05rem; font-weight:700;">🔗 노션 원본에서 열기 (로그인 상태)</div>
    <div style="font-size:0.82rem; opacity:0.9; margin-top:4px;">
      비공개 DB · 캘린더 · 모든 임베드까지 100% 표시 (새 탭에서 열림)
    </div>
  </div>
  <div style="font-size:1.5rem;">→</div>
</div>
</a>
""",
    unsafe_allow_html=True,
)


# ==========================================================
# iframe embed (공개 부분 미리보기)
# ==========================================================
col_a, col_b, col_c = st.columns([4, 1, 1])
with col_a:
    st.markdown(
        f"<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
        f"border-radius:6px; font-size:0.85rem;'>"
        f"🟢 <b>대시보드 미리보기</b> — 공개된 부분만 (댓글/회의록 OK · "
        f"비공개 DB 는 위 '노션 원본 열기' 사용)"
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
    "💡 iframe 안에서 '사용 권한 없음' 보이는 부분 = 비공개 DB. "
    "전체 보시려면 상단 **'🔗 노션 원본에서 열기'** 클릭하세요 (브라우저 로그인 그대로 사용)."
)
