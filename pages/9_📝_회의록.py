"""회의록 — Notion CSV 업로드 + (선택) Notion API 자동 연동.

운영 모드 2가지:
  A) CSV 업로드 (기본 — admin 권한 없을 때) — 매주 Notion 에서 export 후 업로드
  B) Notion API 직결 (admin 권한 받으면) — 자동 갱신
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.ui import setup_page, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT
from api.meetings_csv import (
    parse_csv_file, parse_zip_export, merge_csv_and_md,
    save_meetings, load_meetings, clear_meetings, save_uploaded_file,
    UPLOAD_DIR,
)
from api.notion_meetings import (
    load_meetings as load_notion_meetings,
    load_page_content as load_notion_content,
    test_connection as test_notion,
    _get_creds as get_notion_creds,
)


setup_page(
    page_title="회의록",
    page_icon="📝",
    header_title="📝 회의록",
    header_subtitle="Notion 회의록 — CSV 업로드 또는 API 자동 연동",
)


# ==========================================================
# 모드 자동 감지
# ==========================================================
notion_token, notion_db = get_notion_creds()
use_api = bool(notion_token and notion_db)


# ==========================================================
# 상단 모드 안내 + 업로드 / API 설정 토글
# ==========================================================
mode_col_a, mode_col_b = st.columns([3, 1])
with mode_col_a:
    if use_api:
        st.markdown(
            "<div style='padding:8px 14px; background:#dcfce7; border-left:4px solid #16a34a; "
            "border-radius:6px;'>🟢 <b>Notion API 모드</b> — 자동 갱신 중</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div style='padding:8px 14px; background:#fef3c7; border-left:4px solid #f59e0b; "
            "border-radius:6px;'>📤 <b>CSV 업로드 모드</b> — Notion에서 export 후 업로드</div>",
            unsafe_allow_html=True,
        )

with mode_col_b:
    if not use_api:
        with st.popover("⚙️ Notion API 설정"):
            st.markdown(
                "Admin 권한 받으셨다면 token + DB ID 입력 → 영구 자동 갱신:"
            )
            from api.notion_meetings import save_notion_credentials
            with st.form("notion_setup"):
                t = st.text_input("Notion Token", type="password")
                d = st.text_input("Database ID (32자리)")
                if st.form_submit_button("저장", type="primary"):
                    if t and d:
                        save_notion_credentials(t, d.replace("-", ""))
                        st.success("저장 — 새로고침")
                        st.rerun()


# ==========================================================
# Notion API 모드 — 자동 표시
# ==========================================================
if use_api:
    ok, msg = test_notion()
    if not ok:
        st.error(f"Notion API 오류: {msg}")
        st.stop()

    @st.cache_data(ttl=300, show_spinner="📥 Notion 회의록 조회 중...")
    def _api_meetings():
        return load_notion_meetings(max_count=50)

    refresh_col, _ = st.columns([1, 5])
    if refresh_col.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()

    try:
        meetings = _api_meetings()
    except Exception as e:
        st.error(f"조회 실패: {e}")
        st.stop()

    if not meetings:
        st.info("📭 회의록 없음")
        st.stop()

    st.markdown(f"##### 📋 회의록 {len(meetings)}개 (최신순)")

    def _fmt_iso(iso: str) -> str:
        if not iso:
            return ""
        try:
            from datetime import timedelta
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

    for i, m in enumerate(meetings):
        title = m.get("title") or "(제목 없음)"
        created = _fmt_iso(m.get("created_at", ""))
        last_edit = _fmt_iso(m.get("last_edited_at", ""))
        props = m.get("properties", {})
        notion_url = m.get("url", "")

        with st.expander(f"📅 **{title}**  ·  {created}", expanded=(i == 0)):
            # property 표시
            if props:
                pcols = st.columns(min(4, max(1, len(props))))
                for idx, (k, v) in enumerate(props.items()):
                    if not v:
                        continue
                    display = ", ".join(str(x) for x in v) if isinstance(v, list) else str(v)
                    if not display:
                        continue
                    with pcols[idx % len(pcols)]:
                        st.markdown(
                            f"<div style='font-size:0.72rem; color:{TEXT_FAINT}; "
                            f"text-transform:uppercase; font-weight:600;'>{k}</div>"
                            f"<div style='font-size:0.9rem; color:{TEXT_MAIN}; "
                            f"margin-top:2px;'>{display}</div>",
                            unsafe_allow_html=True,
                        )
            if notion_url:
                st.markdown(
                    f"<div style='font-size:0.75rem; color:{TEXT_MUTED}; margin:10px 0;'>"
                    f"수정 {last_edit}  ·  <a href='{notion_url}' target='_blank' "
                    f"style='color:#2563eb;'>🔗 Notion 열기</a></div>",
                    unsafe_allow_html=True,
                )
            try:
                blocks = load_notion_content(m["id"])
                if blocks:
                    st.divider()
                    _render_blocks(blocks)
            except Exception as e:
                st.warning(f"본문 조회 실패: {e}")

    st.stop()


# ==========================================================
# CSV 업로드 모드
# ==========================================================
with st.expander("📖 Notion 에서 어떻게 내보내나요?", expanded=False):
    st.markdown("""
    1. **노션 회의록 DB** 페이지 진입
    2. 우상단 **"..."** → **"내보내기"** (Export)
    3. **내보내기 형식**:
       - **"Markdown 및 CSV"** ⭐ 추천 — 본문까지 포함 (ZIP 다운로드)
       - **"CSV"** — 속성만 (제목/팀/참석자/날짜 등), 본문 X
    4. **하위 페이지 포함** 체크 권장
    5. 다운로드 받은 파일을 아래에 업로드

    **추천**: ZIP 형식이 본문까지 보여서 좋아요. CSV 만 받으면 회의록 리스트만 보입니다.
    """)

uploaded = st.file_uploader(
    "📂 Notion export 파일 업로드 (.csv 또는 .zip)",
    type=["csv", "zip"],
    accept_multiple_files=False,
    key="meetings_upload",
)

if uploaded is not None:
    btn_col_a, btn_col_b = st.columns([1, 4])
    process = btn_col_a.button("💾 저장 + 처리", type="primary", use_container_width=True)
    if process:
        with st.spinner("처리 중..."):
            saved = save_uploaded_file(uploaded)
            try:
                if saved.suffix.lower() == ".zip":
                    df_csv, md_map = parse_zip_export(saved)
                    if df_csv.empty:
                        st.error("❌ ZIP 안에 CSV 가 없습니다.")
                    else:
                        merged = merge_csv_and_md(df_csv, md_map)
                        save_meetings(merged)
                        n_with_body = (merged["__content_md"].str.len() > 0).sum()
                        st.success(
                            f"✅ {len(merged)}개 회의록 저장 "
                            f"(본문 포함 {int(n_with_body)}개 / {len(md_map)} 본문)"
                        )
                else:
                    df_csv = parse_csv_file(saved)
                    save_meetings(df_csv)
                    st.success(
                        f"✅ {len(df_csv)}개 회의록 저장 (속성만 — 본문은 ZIP 업로드 필요)"
                    )
                st.rerun()
            except Exception as e:
                st.error(f"❌ 파싱 실패: {type(e).__name__}: {e}")


# ==========================================================
# 저장된 회의록 표시
# ==========================================================
meetings_df = load_meetings()
if meetings_df.empty:
    st.info(
        "📭 저장된 회의록이 없습니다.\n\n"
        "노션에서 회의록 DB 를 export → ZIP 또는 CSV 업로드해주세요."
    )
    st.stop()

st.markdown("---")

# 상단 통계
total = len(meetings_df)
with_body = int((meetings_df["__content_md"].astype(str).str.len() > 0).sum())
last_date = meetings_df["__date"].max() if "__date" in meetings_df.columns else ""

c1, c2, c3, c4 = st.columns(4)
c1.metric("총 회의록", f"{total}개")
c2.metric("본문 포함", f"{with_body}개")
c3.metric("최근 회의록", str(last_date)[:10] if last_date else "—")
with c4:
    if st.button("🗑️ 전체 초기화", use_container_width=True,
                 help="저장된 회의록 모두 삭제 (다시 업로드 필요)"):
        clear_meetings()
        st.success("초기화 완료")
        st.rerun()

st.markdown(f"##### 📋 회의록 {total}개 (최신순)")

# 카드 목록
for i, row in meetings_df.iterrows():
    title = row.get("__title") or "(제목 없음)"
    date = str(row.get("__date") or "")[:10]
    body = str(row.get("__content_md") or "")

    with st.expander(f"📅 **{title}**  ·  {date}", expanded=(i == 0)):
        # property 표시 (csv 의 모든 컬럼 중 __로 시작 안 하는 것)
        prop_cols = [c for c in meetings_df.columns if not c.startswith("__")]
        if prop_cols:
            display_props = {}
            for c in prop_cols:
                v = row.get(c)
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    continue
                s = str(v).strip()
                if s and s.lower() != "nan":
                    display_props[c] = s

            if display_props:
                pcols = st.columns(min(4, max(1, len(display_props))))
                for idx, (k, v) in enumerate(display_props.items()):
                    with pcols[idx % len(pcols)]:
                        st.markdown(
                            f"<div style='font-size:0.72rem; color:{TEXT_FAINT}; "
                            f"text-transform:uppercase; font-weight:600;'>{k}</div>"
                            f"<div style='font-size:0.9rem; color:{TEXT_MAIN}; "
                            f"margin-top:2px;'>{v}</div>",
                            unsafe_allow_html=True,
                        )

        if body:
            st.divider()
            st.markdown(body)
        else:
            st.caption(
                ":grey[본문 없음 — CSV 만 업로드 시 본문 X. "
                "Notion 'Markdown 및 CSV' 형식(ZIP)으로 재업로드하면 본문 포함]"
            )
