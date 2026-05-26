"""회의록 — Notion API 자동 연동 + 팀 필터 + iframe 보조.

운영:
  - Notion API token + DB ID 있으면 → 자동 조회 + 카드 + 본문 (기본)
  - 팀 필터: 그로잉업팀만 / 전체 보기 토글
  - 보조: 노션 원본 페이지 열기 버튼 (로그인 상태)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT
from api.notion_meetings import (
    load_meetings, load_page_content, test_connection,
    save_notion_credentials, _get_creds,
)


setup_page(
    page_title="회의록",
    page_icon="📝",
    header_title="📝 회의록",
    header_subtitle="Notion 회의록 DB — API 자동 연동",
)


ROOT = Path(__file__).parent.parent
EMBED_URL_FILE = ROOT / "data" / "notion_meetings_url.txt"


# ==========================================================
# 자격증명 확인
# ==========================================================
token, db_id = _get_creds()

if not token or not db_id:
    st.warning(
        "⚠️ Notion API 자격증명이 없습니다.\n\n"
        "켈리님께 받은 token + DB URL 입력해주세요."
    )
    with st.form("notion_setup"):
        t = st.text_input("Notion Integration Token", type="password",
                          placeholder="ntn_xxxxxxxxxxxx")
        d = st.text_input("회의록 DB URL 또는 ID",
                          placeholder="https://www.notion.so/openhan/...")
        if st.form_submit_button("💾 저장", type="primary"):
            if not t or not d:
                st.error("token + DB URL 모두 입력 필요")
            else:
                # URL 에서 DB ID 자동 추출
                import re
                m = re.search(r"([0-9a-f]{32})", d)
                clean_db = m.group(1) if m else d.replace("-", "").strip()
                save_notion_credentials(t, clean_db)
                st.success("저장 완료 — 페이지 새로고침")
                st.rerun()
    st.stop()


# ==========================================================
# 연결 상태 + 컨트롤
# ==========================================================
ok, msg = test_connection()
if not ok:
    st.error(f"❌ Notion API 오류: {msg}")
    if st.button("⚙️ 재설정"):
        # .env 에서 NOTION_* 만 비우기
        env_path = ROOT / ".env"
        if env_path.exists():
            lines = env_path.read_text(encoding="utf-8").splitlines()
            new_lines = [
                l for l in lines
                if not l.startswith("NOTION_TOKEN=")
                and not l.startswith("NOTION_MEETINGS_DB_ID=")
            ]
            env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        st.rerun()
    st.stop()


# ==========================================================
# 회의록 조회 (캐시)
# ==========================================================
@st.cache_data(ttl=300, show_spinner="📥 Notion 회의록 조회 중...")
def _cached_meetings():
    return load_meetings(max_count=100)


try:
    meetings = _cached_meetings()
except Exception as e:
    st.error(f"조회 실패: {e}")
    st.stop()

if not meetings:
    st.info("📭 회의록 없음")
    st.stop()


# ==========================================================
# 팀 필터
# ==========================================================
# 모든 팀 추출
all_teams = set()
for m in meetings:
    team = m.get("properties", {}).get("팀") or m.get("properties", {}).get("Team", "")
    if team:
        all_teams.add(str(team).strip())

team_options = ["그로잉업"] + sorted(t for t in all_teams if t != "그로잉업")
team_options = ["전체"] + team_options

control_col_a, control_col_b, control_col_c = st.columns([2, 2, 1])
with control_col_a:
    selected_team = st.selectbox(
        "🏷️ 팀 필터",
        team_options,
        index=team_options.index("그로잉업") if "그로잉업" in team_options else 0,
    )
with control_col_b:
    confirm_filter = st.selectbox(
        "✅ 확정 상태",
        ["전체", "confirm 만", "미확정 만"],
        index=0,
    )
with control_col_c:
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# 필터 적용
filtered = []
for m in meetings:
    props = m.get("properties", {})
    team = str(props.get("팀") or props.get("Team", "")).strip()
    if selected_team != "전체" and team != selected_team:
        continue
    confirmed = props.get("confirm")
    if confirm_filter == "confirm 만" and not confirmed:
        continue
    if confirm_filter == "미확정 만" and confirmed:
        continue
    filtered.append(m)


st.markdown(
    f"<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
    f"border-radius:6px; font-size:0.85rem;'>"
    f"🟢 <b>API 모드</b> — {msg} · 전체 {len(meetings)}건 중 "
    f"<b>{len(filtered)}건</b> 표시"
    f"</div>",
    unsafe_allow_html=True,
)
st.write("")


if not filtered:
    st.info("선택한 필터에 맞는 회의록이 없습니다.")
    st.stop()


# ==========================================================
# 헬퍼
# ==========================================================
def _fmt_iso(iso: str) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return (dt + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso[:16]


def _render_blocks(blocks: list[dict]) -> None:
    md_parts: list[str] = []
    for b in blocks:
        btype, text = b.get("type", ""), b.get("text", "")
        if not text and btype != "divider":
            continue
        if btype == "heading_1": md_parts.append(f"### {text}")
        elif btype == "heading_2": md_parts.append(f"#### {text}")
        elif btype == "heading_3": md_parts.append(f"##### {text}")
        elif btype == "bulleted_list_item": md_parts.append(f"- {text}")
        elif btype == "numbered_list_item": md_parts.append(f"1. {text}")
        elif btype == "to_do":
            md_parts.append(f"{'✅' if b.get('checked') else '⬜'} {text}")
        elif btype == "quote": md_parts.append(f"> {text}")
        elif btype == "code": md_parts.append(f"```\n{text}\n```")
        elif btype == "divider": md_parts.append("---")
        elif btype == "callout": md_parts.append(f"> 💡 {text}")
        else: md_parts.append(text)
    if md_parts:
        st.markdown("\n\n".join(md_parts))


# ==========================================================
# Notion 스타일 카드 CSS
# ==========================================================
st.markdown("""
<style>
.nt-card {
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 24px 32px;
  margin-bottom: 16px;
}
.nt-title {
  font-size: 1.5rem;
  font-weight: 800;
  color: #0f172a;
  margin: 0 0 18px 0;
  line-height: 1.2;
}
.nt-row { display: flex; gap: 12px; padding: 4px 0; font-size: 0.88rem; }
.nt-key { color: #64748b; min-width: 100px; }
.nt-val { color: #0f172a; flex: 1; }
.nt-chip {
  display: inline-block; padding: 2px 10px; border-radius: 4px;
  font-size: 0.8rem; font-weight: 500; margin-right: 6px;
  background: #fef3c7; color: #92400e;
}
.nt-chip.team { background: #fce7f3; color: #be185d; }
.nt-chip.person { background: #e0e7ff; color: #4338ca; border-radius: 14px; }
.nt-chip.confirmed { background: #dcfce7; color: #166534; }
</style>
""", unsafe_allow_html=True)


def _format_prop_value(val, key: str) -> str:
    """속성 → HTML chip."""
    if val is None or val == "":
        return ""
    cls = "nt-chip"
    if "팀" in key:
        cls = "nt-chip team"
    elif "참석" in key or "팀장" in key:
        cls = "nt-chip person"

    if isinstance(val, bool):
        if val:
            return '<span class="nt-chip confirmed">✓ 확정</span>'
        return '<span class="nt-chip" style="background:#fef3c7;color:#92400e;">미확정</span>'

    if isinstance(val, list):
        if not val:
            return ""
        return "".join(f'<span class="{cls}">{v}</span>' for v in val)

    s = str(val).strip()
    if not s:
        return ""
    # 날짜 ISO 면 보기 좋게
    if "T" in s and ":" in s and "-" in s:
        s = _fmt_iso(s)
    # 쉼표로 split (multi)
    if "," in s and ("참석" in key or "팀장" in key):
        parts = [p.strip() for p in s.split(",")]
        return "".join(f'<span class="{cls}">{p}</span>' for p in parts)
    return f'<span class="{cls}">{s}</span>'


# ==========================================================
# 회의록 목록 — 카드 (최신순)
# ==========================================================
st.markdown(f"##### 📋 회의록 ({len(filtered)}건, 최신순)")
st.write("")

for i, m in enumerate(filtered):
    title = m.get("title") or "(제목 없음)"
    created = _fmt_iso(m.get("created_at", ""))
    notion_url = m.get("url", "")
    props = m.get("properties", {})

    # 첫 1개만 자동 펼침
    with st.expander(f"📅 **{title}**  ·  {created}", expanded=(i == 0)):
        # 속성 카드
        prop_rows_html = ""
        for k, v in props.items():
            html_val = _format_prop_value(v, k)
            if not html_val:
                continue
            prop_rows_html += (
                f'<div class="nt-row">'
                f'<div class="nt-key">{k}</div>'
                f'<div class="nt-val">{html_val}</div>'
                f'</div>'
            )

        st.markdown(
            f"""
<div class="nt-card">
  {prop_rows_html}
</div>
""",
            unsafe_allow_html=True,
        )

        # 노션 원본 열기 버튼
        if notion_url:
            st.markdown(
                f"<div style='margin: 0 0 12px 0;'>"
                f"<a href='{notion_url}' target='_blank' "
                f"style='font-size:0.82rem; color:#2563eb;'>"
                f"🔗 노션 원본에서 열기 (캘린더/DB embed/댓글 포함 전체 보기)</a>"
                f"</div>",
                unsafe_allow_html=True,
            )

        # 본문 (Notion blocks → markdown)
        try:
            blocks = load_page_content(m["id"])
            if blocks:
                st.divider()
                _render_blocks(blocks)
            else:
                st.caption(":grey[본문 비어있음]")
        except Exception as e:
            st.warning(f"본문 조회 실패: {e}")


# ==========================================================
# 하단 — 보조 도구
# ==========================================================
st.markdown("---")
with st.expander("⚙️ 설정 / 도움말", expanded=False):
    st.markdown(f"""
    **현재 연결**: ✓ Notion API 모드
    **회의록 DB**: `{db_id}`
    **조회**: 매 5분 자동 캐싱 (위 🔄 새로고침 으로 강제 갱신)

    **추가 페이지 연결하고 싶을 때**:
    켈리님께 부탁 → 노션 페이지 우상단 "..." → "연결" →
    "그로잉업팀 대시보드" 추가하면 그 페이지도 API 로 조회 가능.
    """)
    if st.button("🗑️ 자격증명 초기화 (재설정)"):
        env_path = ROOT / ".env"
        if env_path.exists():
            lines = env_path.read_text(encoding="utf-8").splitlines()
            new_lines = [
                l for l in lines
                if not l.startswith("NOTION_TOKEN=")
                and not l.startswith("NOTION_MEETINGS_DB_ID=")
            ]
            env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        st.cache_data.clear()
        st.rerun()
