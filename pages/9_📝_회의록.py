"""회의록 + 워크스페이스 — Notion API 자동 연동 (다중 DB).

연결된 모든 DB 자동 탐색 후 탭으로 표시:
  📝 회의록 / ✅ 할 일 / 📚 지식 / 🗓 캘린더 / 기타
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT
from api.notion_meetings import (
    list_accessible_databases, query_database,
    load_page_content, test_connection,
    save_notion_credentials, _get_creds,
)


setup_page(
    page_title="회의록 / 워크스페이스",
    page_icon="📝",
    header_title="📝 회의록 · 워크스페이스",
    header_subtitle="Notion 연결된 DB 자동 표시 — API 모드",
)


ROOT = Path(__file__).parent.parent


# ==========================================================
# 자격증명 확인
# ==========================================================
token, db_id = _get_creds()

if not token:
    st.warning("⚠️ Notion API token 이 없습니다.")
    with st.form("notion_setup"):
        t = st.text_input("Notion Integration Token", type="password",
                          placeholder="ntn_xxxxxxxxxxxx")
        d = st.text_input("회의록 DB URL 또는 ID (선택)",
                          placeholder="https://www.notion.so/openhan/...")
        if st.form_submit_button("💾 저장", type="primary"):
            if not t:
                st.error("token 입력 필요")
            else:
                import re
                clean_db = ""
                if d:
                    m = re.search(r"([0-9a-f]{32})", d)
                    clean_db = m.group(1) if m else d.replace("-", "").strip()
                save_notion_credentials(t, clean_db)
                st.success("저장 — 새로고침")
                st.rerun()

    st.markdown("""
    ---
    ### ☁️ Streamlit Cloud 에 secrets 등록 안내
    Cloud 에서도 작동하려면:
    1. https://share.streamlit.io → 본인 앱 → **Settings → Secrets**
    2. 다음 추가:
    ```
    NOTION_TOKEN = "ntn_xxxxxxxxxxxx"
    ```
    3. 저장 → 자동 재배포
    """)
    st.stop()


# ==========================================================
# 연결 테스트 + 모든 DB 탐색
# ==========================================================
@st.cache_data(ttl=300, show_spinner="📥 Notion DB 탐색 중...")
def _cached_databases():
    return list_accessible_databases()


@st.cache_data(ttl=300, show_spinner="📥 DB 행 조회 중...")
def _cached_query(db_id_arg: str):
    return query_database(db_id_arg, max_count=200)


databases = _cached_databases()

if not databases:
    st.error(
        "❌ 접근 가능한 DB 가 없습니다. "
        "노션에서 integration 을 DB 에 연결했는지 확인해주세요."
    )
    if st.button("🔄 다시 시도"):
        st.cache_data.clear()
        st.rerun()
    st.stop()


# ==========================================================
# 상단 상태바
# ==========================================================
header_a, header_b = st.columns([4, 1])
with header_a:
    st.markdown(
        f"<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
        f"border-radius:6px; font-size:0.85rem;'>"
        f"🟢 <b>API 모드</b> — Notion 연결됨 · <b>{len(databases)}개 DB</b> 탐색됨"
        f"</div>",
        unsafe_allow_html=True,
    )
with header_b:
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.write("")


# ==========================================================
# DB 타입 분류 + 그로잉업팀 관련만 필터링
# ==========================================================
def _classify_db(title: str) -> tuple[str, str] | None:
    """DB 제목 → (탭 라벨, 아이콘) 또는 None (관련 없음)."""
    t = title.lower().replace(" ", "")
    if "회의록" in title or "meeting" in t:
        return ("회의록", "📝")
    if ("할 일" in title or "할일" in title or
            "task" in t or "todo" in t or "to-do" in t):
        return ("할 일", "✅")
    if "지식" in title or "knowledge" in t:
        return ("지식", "📚")
    if "캘린더" in title or "calendar" in t or "일정" in title:
        return ("캘린더", "🗓")
    return None   # 관련 없는 DB → 제외


# 키워드 매칭된 DB 만 + 중복 제거 (같은 title 두 개 있으면 첫 번째만)
classified = []
seen_labels: dict[str, str] = {}   # label → db_id (중복 방지)

for db in databases:
    result = _classify_db(db["title"])
    if result is None:
        continue
    label, icon = result
    # "그로잉업팀 캘린더" 같이 그로잉업 keyword 있으면 우선
    is_growingup_specific = "그로잉업" in db["title"]
    existing_id = seen_labels.get(label)
    if existing_id and not is_growingup_specific:
        # 이미 더 관련 있는 것 있으면 skip
        continue
    classified = [c for c in classified if c["label"] != label] + [{
        **db, "label": label, "icon": icon,
    }]
    seen_labels[label] = db["id"]

# 우선순위 정렬: 회의록 → 할 일 → 캘린더 → 지식
order = {"회의록": 0, "할 일": 1, "캘린더": 2, "지식": 3}
classified.sort(key=lambda x: order.get(x["label"], 99))


# ==========================================================
# 탭 생성
# ==========================================================
tab_labels = [f"{c['icon']} {c['label']}" for c in classified]
tabs = st.tabs(tab_labels)


# ==========================================================
# 헬퍼 — 회의록 카드 렌더링
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


def _normalize_prop_value(v):
    """display 용 값 정규화 (list → 쉼표, dict → 무시 등)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "✓" if v else ""
    if isinstance(v, list):
        return ", ".join(str(x) for x in v if x)
    s = str(v).strip()
    if "T" in s and ":" in s and len(s) > 10:
        return _fmt_iso(s)
    return s


# ==========================================================
# 회의록 — 카드 + 본문 (특별 처리)
# ==========================================================
def render_meetings_view(db_id_arg: str):
    rows = _cached_query(db_id_arg)
    if not rows:
        st.info("📭 회의록 없음")
        return

    # 팀 필터
    teams = sorted({
        str(r.get("properties", {}).get("팀") or "").strip()
        for r in rows
        if r.get("properties", {}).get("팀")
    })
    team_options = ["전체"] + (["그로잉업"] if "그로잉업" in teams else []) + \
                   [t for t in teams if t != "그로잉업"]

    col_a, col_b = st.columns([2, 1])
    with col_a:
        selected_team = st.selectbox(
            "🏷️ 팀 필터",
            team_options,
            index=team_options.index("그로잉업") if "그로잉업" in team_options else 0,
            key=f"team_filter_{db_id_arg}",
        )
    with col_b:
        confirm_only = st.checkbox("✅ 확정만", key=f"confirm_{db_id_arg}")

    filtered = [
        r for r in rows
        if (selected_team == "전체" or
            str(r.get("properties", {}).get("팀") or "").strip() == selected_team)
        and (not confirm_only or r.get("properties", {}).get("confirm"))
    ]

    st.caption(f"전체 {len(rows)}건 중 **{len(filtered)}건** 표시")
    st.write("")

    if not filtered:
        st.info("필터 조건에 맞는 회의록 없음")
        return

    # 정렬 — 최신순
    filtered.sort(key=lambda r: r.get("created_at", ""), reverse=True)

    for i, m in enumerate(filtered):
        title = m.get("title") or "(제목 없음)"
        created = _fmt_iso(m.get("created_at", ""))
        notion_url = m.get("url", "")
        props = m.get("properties", {})

        with st.expander(f"📅 **{title}**  ·  {created}", expanded=(i == 0)):
            # 속성 카드
            for k, v in props.items():
                if not v:
                    continue
                norm = _normalize_prop_value(v)
                if not norm:
                    continue
                st.markdown(
                    f"<div style='display:flex; gap:12px; padding:3px 0; font-size:0.88rem;'>"
                    f"<div style='color:{TEXT_FAINT}; min-width:100px;'>{k}</div>"
                    f"<div style='color:{TEXT_MAIN};'>{norm}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            if notion_url:
                st.markdown(
                    f"<div style='margin: 8px 0; font-size:0.82rem;'>"
                    f"<a href='{notion_url}' target='_blank' style='color:#2563eb;'>"
                    f"🔗 노션 원본에서 열기</a></div>",
                    unsafe_allow_html=True,
                )

            try:
                blocks = load_page_content(m["id"])
                if blocks:
                    st.divider()
                    _render_blocks(blocks)
            except Exception as e:
                st.warning(f"본문 조회 실패: {e}")


# ==========================================================
# 일반 DB — 표 형태
# ==========================================================
def render_table_view(db_id_arg: str, label: str):
    rows = _cached_query(db_id_arg)
    if not rows:
        st.info(f"📭 {label} 데이터 없음")
        return

    # 모든 property 키 수집
    all_keys = set()
    for r in rows:
        all_keys.update(r.get("properties", {}).keys())

    # DataFrame 변환
    data = []
    for r in rows:
        record = {"제목": r.get("title") or "(제목 없음)"}
        for k in all_keys:
            v = r.get("properties", {}).get(k)
            record[k] = _normalize_prop_value(v)
        record["_url"] = r.get("url", "")
        data.append(record)

    df = pd.DataFrame(data)

    # 최신순
    if "created_at" not in df.columns and rows:
        df["_created"] = [r.get("created_at", "") for r in rows]
        df = df.sort_values("_created", ascending=False).drop(columns=["_created"])

    # URL 컬럼은 link 로 표시
    if "_url" in df.columns:
        col_cfg = {
            "_url": st.column_config.LinkColumn(
                "노션 열기", display_text="🔗 열기", width="small",
            ),
        }
    else:
        col_cfg = {}

    st.caption(f"{len(df)}건")
    st.dataframe(
        df, width="stretch", hide_index=True,
        column_config=col_cfg,
        height=min(700, 60 + len(df) * 36),
    )


# ==========================================================
# 탭별 렌더링
# ==========================================================
for tab, c in zip(tabs, classified):
    with tab:
        st.markdown(f"##### {c['icon']} {c['title']}")
        if c["label"] == "회의록":
            render_meetings_view(c["id"])
        else:
            render_table_view(c["id"], c["label"])


# ==========================================================
# 하단 — 설정
# ==========================================================
st.markdown("---")
with st.expander("⚙️ 설정 / 도움말", expanded=False):
    st.markdown(f"""
    **연결된 DB ({len(classified)}개)**:
    """)
    for c in classified:
        st.markdown(f"- {c['icon']} **{c['title']}** ([🔗 노션 원본]({c['url']}))")

    st.markdown("""

    **추가 DB 연결**: 노션 페이지 우상단 "..." → "연결" → "그로잉업팀 대시보드" 추가

    **Cloud secrets** (Cloud 에서 작동 필요):
    ```
    NOTION_TOKEN = "ntn_xxxxxxxxxx"
    ```
    https://share.streamlit.io → Settings → Secrets 에 추가.
    """)
    if st.button("🗑️ 자격증명 초기화"):
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
