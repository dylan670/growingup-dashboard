"""회의록 — Notion DB 연동.

매주 작성하는 회의록을 Notion 에서 자동 조회. token + db_id 등록 후
페이지 진입 시 최근순으로 표시. 클릭 시 본문 expander 로 열림.
"""
from __future__ import annotations

import os
from datetime import datetime

import streamlit as st

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT


setup_page(
    page_title="회의록",
    page_icon="📝",
    header_title="📝 회의록",
    header_subtitle="Notion 회의록 DB 자동 연동 — 매주 새 항목 자동 표시",
)


# ==========================================================
# Notion 자격증명 확인
# ==========================================================
from api.notion_meetings import (
    load_meetings, load_page_content, test_connection,
    save_notion_credentials, _get_creds,
)


token, db_id = _get_creds()

if not token or not db_id:
    st.warning(
        "⚠️ Notion 연결이 설정되지 않았습니다.\n\n"
        "아래에서 Integration Token + DB ID 등록해주세요."
    )

    with st.expander("📖 Notion Integration 설정 가이드", expanded=True):
        st.markdown("""
        1. https://www.notion.so/my-integrations → **"New integration"** 클릭
        2. Name 입력 (예: "그로잉업 대시보드") → Workspace 선택 → **Submit**
        3. **Internal Integration Secret** 복사 (`ntn_xxxx...` 또는 `secret_xxxx...`)
        4. 회의록 DB 페이지 우측 상단 **"..."** → **"Connections"** →
           방금 만든 integration 추가
        5. 회의록 DB URL 에서 **DB ID** 복사:
           `https://notion.so/workspace/`**`<32자리 hex>`**`?v=...`
        6. 아래 폼에 입력 → 저장
        """)

    with st.form("notion_creds"):
        new_token = st.text_input(
            "Notion Integration Token",
            placeholder="ntn_xxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            type="password",
        )
        new_db_id = st.text_input(
            "회의록 Database ID",
            placeholder="32자리 hex (또는 하이픈 포함 UUID)",
        )
        col1, col2 = st.columns([1, 4])
        save_btn = col1.form_submit_button("💾 저장", type="primary")

        if save_btn:
            if not new_token or not new_db_id:
                st.error("token + DB ID 모두 입력 필요")
            else:
                # 하이픈 제거 (URL form 호환)
                clean_db = new_db_id.replace("-", "").strip()
                save_notion_credentials(new_token, clean_db)
                st.success("저장 완료 — 페이지 새로고침해주세요.")
                st.rerun()

    st.stop()


# ==========================================================
# 연결 상태 + 갱신 버튼
# ==========================================================
col_a, col_b, col_c = st.columns([2, 1, 1])
with col_a:
    ok, msg = test_connection()
    if ok:
        st.markdown(
            f"<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
            f"border-radius:6px;'>🟢 {msg}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.error(f"❌ {msg}")
        st.stop()
with col_b:
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
with col_c:
    if st.button("⚙️ 재설정", use_container_width=True,
                 help="Notion token / DB 변경"):
        # .env 의 NOTION_* 만 비우기
        from pathlib import Path
        env_path = Path(__file__).parent.parent / ".env"
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


# ==========================================================
# 회의록 목록 + 본문
# ==========================================================
@st.cache_data(ttl=300, show_spinner="📥 Notion 에서 회의록 가져오는 중...")
def _cached_meetings() -> list[dict]:
    return load_meetings(max_count=50)


try:
    meetings = _cached_meetings()
except Exception as e:
    st.error(f"❌ 회의록 조회 실패: {e}")
    st.stop()

if not meetings:
    st.info(
        "📭 회의록이 비어있거나, integration 이 DB 에 연결 안 되었을 수 있어요.\n\n"
        "Notion DB 페이지 우측 상단 **'...'** → **'Connections'** → "
        "본 integration 추가했는지 확인해주세요."
    )
    st.stop()

st.markdown(f"##### 📋 회의록 {len(meetings)}개 (최신순)")
st.write("")


def _format_date(iso: str) -> str:
    """ISO timestamp → 사용자 친화적 표시."""
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        # 한국 시간 (UTC+9)
        from datetime import timedelta
        dt = dt + timedelta(hours=9)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso[:10]


def _render_blocks(blocks: list[dict]) -> None:
    """Notion block → Streamlit markdown."""
    md_parts: list[str] = []
    for b in blocks:
        btype = b.get("type", "")
        text = b.get("text", "")
        if not text and btype not in ("divider",):
            continue
        if btype == "heading_1":
            md_parts.append(f"### {text}")
        elif btype == "heading_2":
            md_parts.append(f"#### {text}")
        elif btype == "heading_3":
            md_parts.append(f"##### {text}")
        elif btype == "bulleted_list_item":
            md_parts.append(f"- {text}")
        elif btype == "numbered_list_item":
            md_parts.append(f"1. {text}")
        elif btype == "to_do":
            checked = "✅" if b.get("checked") else "⬜"
            md_parts.append(f"{checked} {text}")
        elif btype == "quote":
            md_parts.append(f"> {text}")
        elif btype == "code":
            md_parts.append(f"```\n{text}\n```")
        elif btype == "divider":
            md_parts.append("---")
        elif btype == "callout":
            md_parts.append(f"> 💡 {text}")
        else:
            # paragraph + 기타
            md_parts.append(text)
    if md_parts:
        st.markdown("\n\n".join(md_parts))


# 회의록 목록 — 카드 형태
for i, m in enumerate(meetings):
    title = m.get("title") or "(제목 없음)"
    created = _format_date(m.get("created_at", ""))
    last_edited = _format_date(m.get("last_edited_at", ""))
    props = m.get("properties", {})
    notion_url = m.get("url", "")

    # 가장 최근 = 기본 펼침
    expanded = (i == 0)

    with st.expander(f"📅 **{title}**  ·  생성 {created}", expanded=expanded):
        # 메타 정보
        meta_cols = st.columns(4)
        meta_idx = 0

        # property 표시: 팀, 참석자, 날짜 등
        for key, val in props.items():
            if not val:
                continue
            if isinstance(val, list):
                if not val:
                    continue
                display = ", ".join(str(v) for v in val)
            else:
                display = str(val)
            if not display:
                continue
            with meta_cols[meta_idx % 4]:
                st.markdown(
                    f"<div style='font-size:0.72rem; color:{TEXT_FAINT}; "
                    f"text-transform:uppercase; font-weight:600;'>{key}</div>"
                    f"<div style='font-size:0.9rem; color:{TEXT_MAIN}; "
                    f"font-weight:500; margin-top:2px;'>{display}</div>",
                    unsafe_allow_html=True,
                )
            meta_idx += 1

        st.markdown(
            f"<div style='font-size:0.75rem; color:{TEXT_MUTED}; "
            f"margin:10px 0;'>마지막 수정: {last_edited}  ·  "
            f"<a href='{notion_url}' target='_blank' style='color:#2563eb;'>"
            f"🔗 Notion 에서 열기</a></div>",
            unsafe_allow_html=True,
        )

        # 본문 (blocks) — lazy load
        try:
            blocks = load_page_content(m["id"])
            if blocks:
                st.divider()
                _render_blocks(blocks)
            else:
                st.caption(":grey[본문 내용 없음]")
        except Exception as e:
            st.warning(f"본문 조회 실패: {e}")
