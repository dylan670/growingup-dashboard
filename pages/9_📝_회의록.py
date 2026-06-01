"""회의록 + 워크스페이스 — Notion API 자동 연동 (다중 DB).

연결된 모든 DB 자동 탐색 후 탭으로 표시:
  📝 회의록 / ✅ 할 일 / 📚 지식 / 🗓 캘린더 / 기타
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT
from api.notion_meetings import (
    list_accessible_databases, query_database,
    load_page_content, test_connection,
    save_notion_credentials, _get_creds,
    _extract_date_full,
    cache_load, cache_save, cache_age_seconds,
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
# 캐시 우선 로드 — 디스크 캐시에서 즉시 표시, 노션 API 호출 없음
# ==========================================================
def _cache_key_for_db(db_id_arg: str) -> str:
    return f"rows_{db_id_arg.replace('-', '')}"


@st.cache_data(ttl=60, show_spinner=False)
def _cached_databases():
    """디스크 캐시 → 노션 API 순서."""
    cached, saved_at = cache_load("databases")
    if cached:
        return cached, saved_at
    # 디스크 캐시 없으면 노션 API
    fresh = list_accessible_databases()
    if fresh:
        cache_save("databases", fresh)
    return fresh, None


@st.cache_data(ttl=60, show_spinner=False)
def _cached_query(db_id_arg: str):
    """일반 쿼리 — 디스크 캐시 우선."""
    cached, _ = cache_load(_cache_key_for_db(db_id_arg))
    if cached is not None:
        return cached
    return query_database(db_id_arg, max_count=200, include_raw=True)


@st.cache_data(ttl=60, show_spinner=False)
def _cached_query_raw(db_id_arg: str):
    """캘린더용 — 디스크 캐시 우선 (raw properties 포함)."""
    cached, _ = cache_load(_cache_key_for_db(db_id_arg))
    if cached is not None:
        return cached
    return query_database(db_id_arg, max_count=300, include_raw=True)


def _sync_now(db_ids: list[str]) -> tuple[int, int, list[str]]:
    """노션에서 강제 sync — (성공, 실패, 에러 메시지)."""
    ok, fail = 0, 0
    errors: list[str] = []

    # databases 갱신
    try:
        fresh_dbs = list_accessible_databases()
        if fresh_dbs:
            # 페이지 코드와 동일한 classify 로직으로 필터링
            cs = []
            seen: dict = {}
            for db in fresh_dbs:
                title = db["title"]
                t = title.lower().replace(" ", "")
                lbl = None
                if "회의록" in title or "meeting" in t: lbl = ("회의록", "📝")
                elif ("할 일" in title or "할일" in title or "task" in t
                      or "todo" in t or "to-do" in t): lbl = ("할 일", "✅")
                elif "지식" in title or "knowledge" in t: lbl = ("지식", "📚")
                elif "캘린더" in title or "calendar" in t or "일정" in title:
                    lbl = ("캘린더", "🗓")
                if not lbl:
                    continue
                label, icon = lbl
                is_g = "그로잉업" in title
                if label in seen and not is_g:
                    continue
                cs = [c for c in cs if c["label"] != label]
                cs.append({**db, "label": label, "icon": icon})
                seen[label] = db["id"]
            order = {"회의록": 0, "할 일": 1, "캘린더": 2, "지식": 3}
            cs.sort(key=lambda x: order.get(x["label"], 99))
            cache_save("databases", cs)
            # 이후 DB id 목록은 cs 사용
            db_ids = [c["id"] for c in cs]
    except Exception as e:
        errors.append(f"DB 목록: {e}")

    for db_id in db_ids:
        try:
            rows = query_database(
                db_id, max_count=300, raise_on_error=True, include_raw=True,
            )
            cache_save(_cache_key_for_db(db_id), rows)
            ok += 1
        except Exception as e:
            fail += 1
            errors.append(f"{db_id[:8]}: {e}")
    return ok, fail, errors


def _diagnose_query(db_id_arg: str) -> str:
    """쿼리 실패 원인 진단."""
    try:
        rows = query_database(db_id_arg, max_count=5, raise_on_error=True)
        return f"✅ 정상 — {len(rows)}건 조회됨"
    except Exception as e:
        return f"❌ {e}"


def _show_empty_diag(db_id_arg: str, label: str = "데이터"):
    """rows 가 비어 있을 때 공통 진단 UI."""
    # 캐시 상태 알리기
    age = cache_age_seconds(_cache_key_for_db(db_id_arg))
    if age is not None:
        st.caption(f"⏱ 로컬 캐시: {age/60:.1f}분 전 저장됨 (비어있음)")

    diag = _diagnose_query(db_id_arg)
    if any(s in diag for s in (
        "일시 장애", "503", "502", "504", "timeout", "Timeout", "ConnectionError",
    )):
        st.error(
            "🌐 **Notion 서버가 일시 장애 상태입니다.** "
            "노션 본체도 같이 안 될 가능성이 큽니다. 잠시 후 다시 시도해주세요."
        )
        st.caption(f"진단: {diag}")
        st.markdown("👉 https://www.notionstatus.com 에서 상태 확인")
    else:
        st.info(f"📭 {label} 없음")
        with st.expander("🔍 진단", expanded=False):
            st.code(f"DB ID: {db_id_arg}\n쿼리 결과: {diag}")
    if st.button("🔄 캐시 비우고 재시도", key=f"recache_{label}_{db_id_arg}"):
        st.cache_data.clear()
        st.rerun()


databases, db_cache_saved_at = _cached_databases()

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
# 상단 상태바 — 캐시 상태 표시 + Sync 버튼
# ==========================================================
# 캐시 나이 계산 (databases 기준)
cache_age = cache_age_seconds("databases")
if cache_age is not None:
    if cache_age < 60:
        age_text = f"{int(cache_age)}초"
        age_color = "#16a34a"
    elif cache_age < 3600:
        age_text = f"{int(cache_age/60)}분"
        age_color = "#16a34a" if cache_age < 1800 else "#f59e0b"
    else:
        age_text = f"{int(cache_age/3600)}시간"
        age_color = "#dc2626"
    cache_status = (
        f"<span style='color:{age_color};'>⚡ 캐시 {age_text} 전</span>"
    )
else:
    cache_status = "<span style='color:#dc2626;'>⚠️ 캐시 없음</span>"

header_a, header_b, header_c = st.columns([3, 1, 1])
with header_a:
    st.markdown(
        f"<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
        f"border-radius:6px; font-size:0.85rem;'>"
        f"🟢 <b>API 모드</b> — <b>{len(databases)}개 DB</b> · {cache_status} "
        f"<span style='color:#6b7280; font-size:0.78rem;'>"
        f"(로컬 캐시에서 즉시 로드)</span>"
        f"</div>",
        unsafe_allow_html=True,
    )
with header_b:
    if st.button("⚡ Sync", use_container_width=True,
                 help="노션에서 최신 데이터 가져와 캐시 갱신"):
        db_ids = [d["id"] for d in databases]
        with st.spinner("📥 노션에서 sync 중..."):
            ok, fail, errs = _sync_now(db_ids)
        if fail == 0:
            st.success(f"✅ {ok}개 DB sync 완료")
        else:
            st.warning(f"⚠️ {ok}개 성공, {fail}개 실패")
            for e in errs:
                st.caption(f"  - {e}")
        st.cache_data.clear()
        time.sleep(0.5)
        st.rerun()
with header_c:
    if st.button("🔄 다시 로드", use_container_width=True,
                 help="streamlit 세션 캐시만 비우고 디스크 캐시는 유지"):
        st.cache_data.clear()
        st.rerun()

st.write("")


# ==========================================================
# DB 타입 분류 — 캐시에 label 이 이미 있으면 그대로, 아니면 동적 분류
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
    return None


# 캐시에서 온 databases 는 이미 분류돼 있을 수 있음 (sync_notion_cache.py)
if databases and "label" in databases[0]:
    classified = list(databases)
else:
    # 동적 분류
    classified = []
    seen_labels: dict[str, str] = {}
    for db in databases:
        result = _classify_db(db.get("title", ""))
        if result is None:
            continue
        label, icon = result
        is_growingup = "그로잉업" in db.get("title", "")
        if label in seen_labels and not is_growingup:
            continue
        classified = [c for c in classified if c["label"] != label]
        classified.append({**db, "label": label, "icon": icon})
        seen_labels[label] = db["id"]

# 우선순위 정렬
order = {"회의록": 0, "할 일": 1, "캘린더": 2, "지식": 3}
classified.sort(key=lambda x: order.get(x.get("label", ""), 99))


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
        _show_empty_diag(db_id_arg, "회의록")
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
        # 옵션 5개 이하면 horizontal radio (한 번에 클릭), 많으면 selectbox
        if len(team_options) <= 5:
            selected_team = st.radio(
                "🏷️ 팀 필터",
                team_options,
                index=team_options.index("그로잉업") if "그로잉업" in team_options else 0,
                horizontal=True,
                key=f"team_filter_{db_id_arg}",
            )
        else:
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

    # ============================================
    # 주차별 그룹화 + 상단 목차 chip
    # ============================================
    def _week_info(iso_dt: str) -> tuple[str, str]:
        """ISO timestamp → (sort_key, display_label)."""
        if not iso_dt:
            return ("0000-00-0", "(날짜 미상)")
        try:
            dt = datetime.fromisoformat(iso_dt.replace("Z", "+00:00"))
            dt = dt + timedelta(hours=9)   # KST
            wm = (dt.day - 1) // 7 + 1
            return (
                f"{dt.year:04d}-{dt.month:02d}-{wm}",
                f"{dt.year}년 {dt.month}월 {wm}주차",
            )
        except Exception:
            return ("0000-00-0", "(날짜 미상)")

    from collections import defaultdict
    weeks: dict = defaultdict(list)
    week_labels: dict = {}
    for m in filtered:
        sk, lbl = _week_info(m.get("created_at", ""))
        weeks[sk].append(m)
        week_labels[sk] = lbl

    sorted_weeks = sorted(weeks.keys(), reverse=True)

    # 상단 목차 chip (주차 1개 이상일 때만)
    if len(sorted_weeks) > 1:
        chips_inner = ""
        for sk in sorted_weeks:
            cnt = len(weeks[sk])
            chips_inner += (
                f'<div style="background:#dbeafe; color:#1e40af; '
                f'border-radius:999px; padding:5px 12px; '
                f'font-size:0.78rem; font-weight:600; '
                f'border:1px solid #bfdbfe;">'
                f'{week_labels[sk]} <span style="color:#60a5fa; '
                f'margin-left:4px;">·</span> {cnt}건</div>'
            )
        st.markdown(
            f'<div style="display:flex; gap:6px; flex-wrap:wrap; '
            f'margin-bottom:14px; padding:10px 12px; background:#f8fafc; '
            f'border-radius:10px; border:1px solid #e2e8f0;">'
            f'<div style="font-size:0.78rem; color:#64748b; '
            f'font-weight:600; align-self:center; margin-right:4px;">'
            f'📑 주차별 목차</div>'
            f'{chips_inner}</div>',
            unsafe_allow_html=True,
        )

    # ============================================
    # 주차별 회의록 렌더링
    # ============================================
    is_first_overall = True
    for sk_idx, sk in enumerate(sorted_weeks):
        st.markdown(
            f'<div style="margin-top:18px; margin-bottom:6px; '
            f'padding:8px 12px; background:linear-gradient(90deg, #eff6ff 0%, transparent 100%); '
            f'border-left:4px solid #2563eb; border-radius:6px;">'
            f'<span style="font-size:0.95rem; font-weight:700; color:#1e40af;">'
            f'📅 {week_labels[sk]}</span>'
            f'<span style="font-size:0.78rem; color:#64748b; margin-left:10px;">'
            f'{len(weeks[sk])}건</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        for m in weeks[sk]:
            title = m.get("title") or "(제목 없음)"
            created = _fmt_iso(m.get("created_at", ""))
            notion_url = m.get("url", "")
            props = m.get("properties", {})

            with st.expander(
                f"📝 **{title}**  ·  {created}",
                expanded=is_first_overall,
            ):
                is_first_overall = False
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
# 캘린더 — 달력 그리드 뷰 (streamlit-calendar)
# ==========================================================
def render_calendar_view(db_id_arg: str):
    """노션 캘린더 DB → FullCalendar 달력 표시."""
    try:
        from streamlit_calendar import calendar
    except ImportError:
        st.warning(
            "streamlit-calendar 가 설치되지 않았습니다. "
            "`pip install streamlit-calendar` 후 재시작."
        )
        return

    rows = _cached_query_raw(db_id_arg)
    if not rows:
        _show_empty_diag(db_id_arg, "캘린더 항목")
        return

    # 담당자별 색상 매핑
    PALETTE = ["#2563eb", "#f59e0b", "#16a34a", "#dc2626", "#8b5cf6",
               "#ec4899", "#0ea5e9", "#14b8a6", "#f97316", "#64748b"]
    assignee_color: dict[str, str] = {}
    idx = 0

    def _color_for(name: str) -> str:
        nonlocal idx
        if name not in assignee_color:
            assignee_color[name] = PALETTE[idx % len(PALETTE)]
            idx += 1
        return assignee_color[name]

    # 날짜 컬럼 자동 탐지 — date 타입 property 우선
    DATE_KEY_PRIORITY = ["날짜", "Date", "date", "마감일시", "마감일", "일자", "Due"]

    def _find_date_dict(raw_props: dict) -> dict | None:
        """raw properties 에서 date 타입 property 우선 탐색."""
        # 우선순위 키
        for k in DATE_KEY_PRIORITY:
            if k in raw_props and raw_props[k].get("type") == "date":
                d = _extract_date_full(raw_props[k])
                if d.get("start"):
                    return d
        # 그래도 없으면 date 타입 첫 것
        for k, v in raw_props.items():
            if v.get("type") == "date":
                d = _extract_date_full(v)
                if d.get("start"):
                    return d
        return None

    # 이벤트 변환
    events = []
    skipped_no_date = 0
    sample_props_seen: set = set()

    for r in rows:
        title = r.get("title") or "(제목 없음)"
        props = r.get("properties", {})
        raw_props = r.get("_raw_properties", {})

        for k in raw_props.keys():
            sample_props_seen.add(k)

        # 1) raw 에서 date 타입 찾기
        date_info = _find_date_dict(raw_props)

        # 2) 없으면 parsed props 의 string 값 fallback
        if not date_info or not date_info.get("start"):
            date_val = (
                props.get("날짜")
                or props.get("Date")
                or props.get("date")
                or props.get("마감일시")
                or props.get("마감일")
            )
            if date_val and isinstance(date_val, str) and date_val.strip():
                date_info = {"start": date_val.strip(), "end": ""}

        # 3) 그래도 없으면 created_at fallback
        if not date_info or not date_info.get("start"):
            ca = r.get("created_at", "")
            if ca:
                date_info = {"start": ca, "end": ""}

        if not date_info or not date_info.get("start"):
            skipped_no_date += 1
            continue

        date_str = date_info["start"]
        end_str = date_info.get("end", "")

        # 담당자
        assignee = props.get("담당자") or props.get("Assignee") or ""
        if isinstance(assignee, list):
            assignee = assignee[0] if assignee else ""
        assignee = str(assignee).strip()

        # 완료여부
        done = (props.get("완료여부") or props.get("보정완료")
                or props.get("Done") or props.get("완료"))
        done_bool = bool(done) and str(done).lower() not in ("no", "false", "0", "")

        # 색상
        color = _color_for(assignee) if assignee else "#94a3b8"
        if done_bool:
            color = color + "88"

        event_title = f"{title}"
        if assignee:
            event_title = f"[{assignee}] {title}"

        event = {
            "title": event_title,
            "start": date_str,
            "backgroundColor": color,
            "borderColor": color,
            "textColor": "#ffffff",
            "extendedProps": {
                "url": r.get("url", ""),
                "assignee": assignee,
                "done": done_bool,
            },
        }
        if end_str:
            event["end"] = end_str
        events.append(event)

    if not events:
        st.warning(
            f"📭 {len(rows)}건 조회됐지만 날짜 정보가 있는 이벤트가 없습니다."
        )
        with st.expander("🔍 진단 — 속성 키 목록", expanded=True):
            st.code(
                f"DB 행 수: {len(rows)}\n"
                f"날짜 없어서 skip: {skipped_no_date}\n"
                f"발견된 속성 키: {sorted(sample_props_seen)}"
            )
            if rows:
                st.markdown("**첫 행 raw 속성 샘플:**")
                st.json({
                    k: v for k, v in
                    (rows[0].get("_raw_properties") or {}).items()
                })
        return

    # 통계
    st.caption(
        f"📊 전체 {len(rows)}건 중 **{len(events)}건** 표시 "
        f"(날짜 없음 {skipped_no_date}건 제외)"
    )

    # 뷰 모드 선택
    view_mode = st.radio(
        "📅 뷰 모드",
        ["월간", "주간", "리스트"],
        horizontal=True,
        key=f"cal_view_{db_id_arg}",
    )
    view_map = {
        "월간": "dayGridMonth",
        "주간": "timeGridWeek",
        "리스트": "listMonth",
    }
    initial_view = view_map[view_mode]

    # 시작 날짜 = 가장 빈번한 월
    from collections import Counter
    months = Counter()
    for ev in events:
        s = ev.get("start", "")[:7]
        if s:
            months[s] += 1
    initial_date = (
        months.most_common(1)[0][0] + "-01" if months
        else datetime.now().strftime("%Y-%m-01")
    )

    calendar_options = {
        "headerToolbar": {
            "left": "today prev,next",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek,listMonth",
        },
        "initialView": initial_view,
        "initialDate": initial_date,
        "selectable": False,
        "editable": False,
        "locale": "ko",
        "buttonText": {
            "today": "오늘",
            "month": "월",
            "week": "주",
            "list": "리스트",
        },
        "height": 720,
        "firstDay": 0,
        "dayMaxEvents": 3,
        "displayEventTime": False,
    }

    custom_css = """
.fc-event-title { font-weight: 500; font-size: 0.78rem; }
.fc-event { cursor: pointer; padding: 2px 4px; border-radius: 4px; }
.fc-toolbar-title { font-size: 1.15rem; font-weight: 700; }
.fc-button { font-size: 0.82rem; padding: 4px 12px; }
.fc-day-today { background: #fef3c7 !important; }
.fc-col-header-cell { background: #f8fafc; font-weight: 600; }
"""

    state = calendar(
        events=events,
        options=calendar_options,
        custom_css=custom_css,
        key=f"calendar_{db_id_arg}",
    )

    # 클릭한 이벤트 정보 표시
    if state and state.get("eventClick"):
        clicked = state["eventClick"].get("event", {})
        ext = clicked.get("extendedProps", {}) or {}
        url = ext.get("url", "")
        st.markdown(
            f"**{clicked.get('title', '')}**  · "
            + (f"[🔗 노션에서 열기]({url})" if url else "")
        )

    # 담당자별 범례
    if assignee_color:
        st.write("")
        legend_html = (
            "<div style='display:flex; gap:10px; flex-wrap:wrap; "
            "font-size:0.82rem; margin-top:8px;'>"
        )
        for name, color in assignee_color.items():
            if name:
                legend_html += (
                    f"<div style='display:flex; align-items:center; gap:6px;'>"
                    f"<div style='width:14px; height:14px; background:{color}; "
                    f"border-radius:3px;'></div>{name}</div>"
                )
        legend_html += "</div>"
        st.markdown(legend_html, unsafe_allow_html=True)


# ==========================================================
# 일반 DB — 표 형태
# ==========================================================
def render_table_view(db_id_arg: str, label: str):
    rows = _cached_query(db_id_arg)
    if not rows:
        _show_empty_diag(db_id_arg, label)
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
        elif c["label"] == "캘린더":
            render_calendar_view(c["id"])
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
