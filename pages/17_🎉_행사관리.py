"""행사관리 — 팀 일정 + 브랜드 행사 통합 캘린더 (Notion 연동).

📅 팀 일정   : 그로잉업팀 캘린더 (담당자별 업무 — 클레어/딜런/제인 …)
🎉 브랜드 행사: 🕶️ 롤라루 캘린더 (브랜드·판매처별 프로모션/세일)
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.ui import setup_page, BRAND_COLORS, TEXT_MUTED
from api.notion_events import load_events, load_team_schedule


setup_page(
    page_title="행사관리",
    page_icon="🎉",
    header_title="🎉 행사관리",
    header_subtitle="팀 일정(담당자별) · 브랜드 행사를 한 화면에서 — 진행중·예정 추적",
)


def _flatten_html(html: str) -> str:
    return "".join(ln.strip() for ln in html.strip().split("\n"))


_today = pd.Timestamp(datetime.now().date())
_WD = "월화수목금토일"

# 담당자 색상 팔레트 (이름순 고정 매핑 → 색 일관성)
_PERSON_PALETTE = [
    "#6366f1", "#ec4899", "#14b8a6", "#f59e0b", "#8b5cf6",
    "#ef4444", "#0ea5e9", "#84cc16", "#f43f5e", "#06b6d4",
]


def _person_colors(names: list[str]) -> dict[str, str]:
    return {n: _PERSON_PALETTE[i % len(_PERSON_PALETTE)]
            for i, n in enumerate(sorted(names))}


# ============================================================
# 데이터 로드
# ============================================================
@st.cache_data(ttl=600)
def _load_team_df() -> pd.DataFrame:
    rows = load_team_schedule()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["start"] = pd.to_datetime(df["date_start"], errors="coerce", utc=True)
    df["start"] = df["start"].dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    df = df.dropna(subset=["start"]).reset_index(drop=True)
    df["done"] = df["done"].fillna(False).astype(bool)
    df["name"] = df["name"].fillna("(이름 없음)").replace("", "(이름 없음)")
    df["assignees"] = df["assignees"].apply(
        lambda a: a if isinstance(a, list) and a else ["미지정"]
    )
    return df


@st.cache_data(ttl=600)
def _load_events_df() -> pd.DataFrame:
    rows = load_events()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["start"] = pd.to_datetime(df["date_start"], errors="coerce")
    df["end"] = pd.to_datetime(df["date_end"], errors="coerce")
    df["end"] = df["end"].fillna(df["start"])
    same = df["end"] <= df["start"]
    df.loc[same, "end"] = df.loc[same, "start"] + pd.Timedelta(days=1)
    df = df.dropna(subset=["start"]).reset_index(drop=True)
    df["name"] = df["name"].fillna("(이름 없음)").replace("", "(이름 없음)")
    df["brand"] = df["brand"].fillna("기타").replace("", "기타")
    df["channel"] = df["channel"].fillna("기타").replace("", "기타")
    return df


tab_team, tab_brand = st.tabs(["📅 팀 일정", "🎉 브랜드 행사"])


# ============================================================
# TAB 1 — 팀 일정 (그로잉업팀 캘린더)
# ============================================================
with tab_team:
    tdf = _load_team_df()

    if tdf.empty:
        st.markdown(
            _flatten_html("""
<div style="background:#eef2ff; border:1px solid #c7d2fe; border-radius:12px; padding:20px 24px; margin-top:16px;">
    <div style="font-size:1.02rem; font-weight:700; color:#3730a3;">📭 팀 일정 데이터 없음</div>
    <div style="font-size:0.88rem; color:#4338ca; margin-top:8px; line-height:1.6;">
        그로잉업팀 캘린더를 불러오지 못했습니다. Notion Integration 연결 또는
        최근 일정(날짜) 입력 여부를 확인해 주세요.
    </div>
</div>
            """),
            unsafe_allow_html=True,
        )
    else:
        all_people = sorted({a for lst in tdf["assignees"] for a in lst})
        pcolors = _person_colors(all_people)

        fc1, fc2 = st.columns([3, 1])
        with fc1:
            sel_people = st.multiselect(
                "👥 담당자", all_people, default=all_people,
            )
        with fc2:
            only_open = st.toggle("⏳ 미완료만", value=False)

        view = tdf[tdf["assignees"].apply(
            lambda a: any(x in sel_people for x in a)
        )].copy()
        if only_open:
            view = view[~view["done"]]

        if view.empty:
            st.info("선택한 조건에 해당하는 일정이 없습니다.")
        else:
            # KPI
            day0 = view["start"].dt.normalize()
            today_cnt = int((day0 == _today).sum())
            wk_cnt = int(((day0 >= _today) &
                          (day0 < _today + pd.Timedelta(days=7))).sum())
            open_cnt = int((~view["done"]).sum())
            present = sorted({a for lst in view["assignees"] for a in lst})

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("📌 오늘", f"{today_cnt}건")
            k2.metric("🗓 향후 7일", f"{wk_cnt}건")
            k3.metric("⏳ 미완료", f"{open_cnt}건")
            k4.metric("👥 담당자", f"{len(present)}명")

            # 담당자별 건수 막대
            cnt = pd.Series(
                [a for lst in view["assignees"] for a in lst]
            ).value_counts()
            if not cnt.empty:
                fig = px.bar(
                    x=cnt.values.tolist(), y=cnt.index.tolist(),
                    orientation="h",
                    color=cnt.index.tolist(),
                    color_discrete_map=pcolors,
                )
                fig.update_layout(
                    height=max(150, 38 * len(cnt) + 40),
                    margin=dict(l=10, r=10, t=6, b=6),
                    showlegend=False, plot_bgcolor="white",
                    xaxis=dict(title="", showgrid=True, gridcolor="#f1f5f9"),
                    yaxis=dict(title="", autorange="reversed"),
                )
                fig.update_traces(
                    texttemplate="%{x}건", textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

            # 날짜별 그룹 리스트
            view = view.sort_values("start")
            view["day"] = view["start"].dt.normalize()
            for day, grp in view.groupby("day"):
                delta = (day - _today).days
                wd = _WD[int(day.weekday())]
                md = f"{day.month}/{day.day}({wd})"
                if delta == 0:
                    lab, lc = f"오늘 · {md}", "#4f46e5"
                elif delta == 1:
                    lab, lc = f"내일 · {md}", "#0891b2"
                elif delta == -1:
                    lab, lc = f"어제 · {md}", "#94a3b8"
                else:
                    lab = f"{md}"
                    lc = "#94a3b8" if delta < 0 else "#0f172a"

                rows_html = ""
                for _, t in grp.iterrows():
                    tm = t["start"].strftime("%H:%M")
                    done = bool(t["done"])
                    nm = t["name"]
                    nm_style = (
                        "color:#94a3b8; text-decoration:line-through;"
                        if done else "color:#0f172a;"
                    )
                    mark = "✅" if done else "▫️"
                    chips = ""
                    for a in t["assignees"]:
                        c = pcolors.get(a, "#64748b")
                        chips += (
                            f'<span style="background:{c}1a; color:{c}; '
                            f'font-size:0.68rem; font-weight:700; padding:2px 8px; '
                            f'border-radius:999px; margin-left:4px;">{a}</span>'
                        )
                    rows_html += (
                        f'<div style="display:flex; align-items:center; gap:8px; '
                        f'padding:7px 12px; border-bottom:1px solid #f1f5f9;">'
                        f'<span style="font-size:0.72rem; color:#94a3b8; '
                        f'width:42px; flex-shrink:0;">{tm}</span>'
                        f'<span style="flex:1; font-size:0.84rem; {nm_style}">'
                        f'{mark} {nm}</span>'
                        f'<span style="white-space:nowrap;">{chips}</span>'
                        f'</div>'
                    )

                st.markdown(
                    _flatten_html(f"""
<div style="margin-bottom:14px;">
    <div style="font-size:0.82rem; font-weight:700; color:{lc}; margin-bottom:4px;">
        {lab} <span style="color:#cbd5e1; font-weight:500;">· {len(grp)}건</span>
    </div>
    <div style="background:white; border:1px solid #e2e8f0; border-radius:10px; overflow:hidden;">
        {rows_html}
    </div>
</div>
                    """),
                    unsafe_allow_html=True,
                )


# ============================================================
# TAB 2 — 브랜드 행사 (롤라루 캘린더)
# ============================================================
with tab_brand:
    edf = _load_events_df()

    if edf.empty:
        st.markdown(
            _flatten_html("""
<div style="background:#fef3c7; border:1px solid #fcd34d; border-radius:12px; padding:22px 26px; margin-top:16px;">
    <div style="font-size:1.05rem; font-weight:700; color:#78350f;">📭 브랜드 행사 데이터 없음</div>
    <div style="font-size:0.88rem; color:#92400e; margin-top:10px; line-height:1.7;">
        Notion <b>🕶️ 롤라루 캘린더</b> 를 불러오지 못했습니다.<br>
        <b>롤라루 캘린더</b> 페이지를 대시보드 Integration(<b>그로잉업팀 대시보드</b>)에 공유해 주세요:
        <ol style="margin-top:8px; padding-left:20px;">
            <li>Notion <b>🕶️ 롤라루 캘린더</b> 페이지 → 우측 상단 <code>···</code></li>
            <li><b>연결(Connections)</b> → <b>그로잉업팀 대시보드</b> 추가 (⚠️ Claude 말고)</li>
        </ol>
        연결 후 새로고침하면 자동 반영됩니다.
    </div>
</div>
            """),
            unsafe_allow_html=True,
        )
    else:
        def _status(row) -> str:
            if row["start"] > _today:
                return "예정"
            if row["end"].normalize() < _today:
                return "종료"
            return "진행중"

        edf["status"] = edf.apply(_status, axis=1)

        fc1, fc2 = st.columns(2)
        with fc1:
            brands = sorted(edf["brand"].unique().tolist())
            sel_brands = st.multiselect("🏷 브랜드", brands, default=brands)
        with fc2:
            channels = sorted(edf["channel"].unique().tolist())
            sel_channels = st.multiselect("🛒 판매처", channels, default=channels)

        view = edf[edf["brand"].isin(sel_brands) &
                   edf["channel"].isin(sel_channels)].copy()

        if view.empty:
            st.info("선택한 필터에 해당하는 행사가 없습니다.")
        else:
            this_month = _today.to_period("M")
            ongoing = int((view["status"] == "진행중").sum())
            upcoming = int((view["status"] == "예정").sum())
            tm_cnt = int(view["start"].dt.to_period("M").eq(this_month).sum())

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("🔴 진행중", f"{ongoing}건")
            k2.metric("🟡 예정", f"{upcoming}건")
            k3.metric("📅 이번 달", f"{tm_cnt}건")
            k4.metric("📊 전체", f"{len(view)}건")

            st.markdown("---")
            st.markdown("### 🗓 행사 타임라인")

            gantt = view.sort_values("start").copy()
            bcolor = {
                b: BRAND_COLORS.get(b, {}).get("primary", "#94a3b8")
                for b in gantt["brand"].unique()
            }
            fig = px.timeline(
                gantt, x_start="start", x_end="end", y="name",
                color="brand", color_discrete_map=bcolor,
                hover_data={"channel": True, "status": True, "name": False},
            )
            fig.update_yaxes(autorange="reversed", title="")
            fig.update_xaxes(title="", tickformat="%m/%d",
                             showgrid=True, gridcolor="#f1f5f9")
            fig.add_vline(
                x=_today.timestamp() * 1000,
                line_width=2, line_dash="dash", line_color="#ef4444",
                annotation_text="오늘", annotation_position="top",
            )
            fig.update_layout(
                height=max(280, 42 * len(gantt) + 80),
                margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor="white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02,
                            xanchor="left", x=0, title=""),
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            st.markdown("### 📋 행사 목록")

            _rank = {"진행중": 0, "예정": 1, "종료": 2}
            _sty = {
                "진행중": ("#dcfce7", "#166534", "🔴 진행중"),
                "예정": ("#fef9c3", "#854d0e", "🟡 예정"),
                "종료": ("#f1f5f9", "#64748b", "⚪ 종료"),
            }
            lst = view.copy()
            lst["_r"] = lst["status"].map(_rank)
            lst = lst.sort_values(["_r", "start"], ascending=[True, True])

            for _, ev in lst.iterrows():
                bc = BRAND_COLORS.get(ev["brand"], {})
                pri = bc.get("primary", "#64748b")
                soft = bc.get("bg_soft", "#f8fafc")
                txtc = bc.get("text", "#0f172a")
                bg, fg, badge = _sty.get(ev["status"], _sty["종료"])

                s_str = ev["start"].strftime("%Y.%m.%d")
                if (ev["end"] - ev["start"]).days == 1 and not ev["date_end"]:
                    e_str = s_str
                else:
                    e_str = ev["end"].strftime("%Y.%m.%d")
                period = s_str if e_str == s_str else f"{s_str} → {e_str}"

                if ev["status"] == "예정":
                    d = (ev["start"].normalize() - _today).days
                    dtxt, dcol = (f"D-{d}" if d > 0 else "D-DAY"), "#d97706"
                elif ev["status"] == "진행중":
                    left = (ev["end"].normalize() - _today).days
                    dtxt = f"종료까지 {left}일" if left > 0 else "오늘 종료"
                    dcol = "#16a34a"
                else:
                    ago = (_today - ev["end"].normalize()).days
                    dtxt, dcol = f"{ago}일 전 종료", "#94a3b8"

                st.markdown(
                    _flatten_html(f"""
<div style="background:white; border:1px solid #e2e8f0; border-left:4px solid {pri};
            border-radius:10px; padding:14px 18px; margin-bottom:10px;
            box-shadow:0 1px 3px rgba(15,23,42,0.04);">
    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
        <div style="flex:1;">
            <span style="background:{bg}; color:{fg}; font-size:0.7rem; font-weight:700;
                         padding:2px 9px; border-radius:999px;">{badge}</span>
            <span style="font-size:1rem; font-weight:700; color:#0f172a; margin-left:8px;">{ev['name']}</span>
            <div style="margin-top:7px;">
                <span style="background:{soft}; color:{txtc}; font-size:0.72rem; font-weight:600;
                             padding:2px 9px; border-radius:6px;">{ev['brand']}</span>
                <span style="background:#f1f5f9; color:#475569; font-size:0.72rem; font-weight:600;
                             padding:2px 9px; border-radius:6px; margin-left:5px;">🛒 {ev['channel']}</span>
                <span style="color:{TEXT_MUTED}; font-size:0.78rem; margin-left:8px;">📅 {period}</span>
            </div>
        </div>
        <div style="text-align:right; white-space:nowrap; margin-left:14px;">
            <span style="color:{dcol}; font-size:0.86rem; font-weight:700;">{dtxt}</span>
        </div>
    </div>
</div>
                    """),
                    unsafe_allow_html=True,
                )
