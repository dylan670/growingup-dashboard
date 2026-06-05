"""행사관리 — 채널별 행사/프로모션 캘린더 (Notion 무신사 캘린더 연동).

Notion '🕶️ 무신사 캘린더' DB → 행사 일정을 간트 차트 + 카드로 시각화.
  - 진행중 / 예정 / 종료 상태 자동 분류
  - 브랜드 · 판매처 필터
  - D-day 카운트다운
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.ui import setup_page, BRAND_COLORS, TEXT_MUTED
from api.notion_events import load_events


setup_page(
    page_title="행사관리",
    page_icon="🎉",
    header_title="🎉 행사관리",
    header_subtitle="채널별 행사·프로모션 일정을 한 눈에 — 진행중·예정·종료 추적",
)


def _flatten_html(html: str) -> str:
    return "".join(ln.strip() for ln in html.strip().split("\n"))


# ============================================================
# 데이터 로드
# ============================================================
@st.cache_data(ttl=600)
def _load() -> pd.DataFrame:
    rows = load_events()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["start"] = pd.to_datetime(df["date_start"], errors="coerce")
    # 종료일 없으면 시작일과 동일 처리, 간트 표시를 위해 +1일
    df["end"] = pd.to_datetime(df["date_end"], errors="coerce")
    df["end"] = df["end"].fillna(df["start"])
    # end <= start 인 경우 간트 막대가 안 보이므로 하루 더함
    same = df["end"] <= df["start"]
    df.loc[same, "end"] = df.loc[same, "start"] + pd.Timedelta(days=1)
    df = df.dropna(subset=["start"]).reset_index(drop=True)
    df["name"] = df["name"].fillna("(이름 없음)").replace("", "(이름 없음)")
    df["brand"] = df["brand"].fillna("기타").replace("", "기타")
    df["channel"] = df["channel"].fillna("기타").replace("", "기타")
    return df


df = _load()


# ============================================================
# 빈 상태 — Integration 미연결 / 데이터 없음 안내
# ============================================================
if df.empty:
    st.markdown(
        _flatten_html("""
<div style="background:#fef3c7; border:1px solid #fcd34d; border-radius:12px; padding:22px 26px; margin-top:18px;">
    <div style="font-size:1.05rem; font-weight:700; color:#78350f;">📭 행사 데이터 없음</div>
    <div style="font-size:0.88rem; color:#92400e; margin-top:10px; line-height:1.7;">
        Notion <b>🕶️ 무신사 캘린더</b> DB 에서 행사를 불러오지 못했습니다.<br>
        아래를 확인해 주세요:
        <ol style="margin-top:8px; padding-left:20px;">
            <li><b>Integration 연결</b> — Notion 에서 무신사 캘린더 페이지 우측 상단
                <code>···</code> → <b>연결(Connections)</b> → 대시보드 Integration 추가</li>
            <li><b>행사 입력</b> — 캘린더 DB 에 이름·날짜·브랜드·판매처 행 추가</li>
        </ol>
        연결 후 페이지를 새로고침하면 자동 반영됩니다.
    </div>
</div>
        """),
        unsafe_allow_html=True,
    )
    st.stop()


# ============================================================
# 상태 분류 (진행중 / 예정 / 종료)
# ============================================================
_today = pd.Timestamp(datetime.now().date())


def _status(row) -> str:
    if row["start"] > _today:
        return "예정"
    if row["end"].normalize() < _today:
        return "종료"
    return "진행중"


df["status"] = df.apply(_status, axis=1)


# ============================================================
# 필터 — 브랜드 · 판매처
# ============================================================
fc1, fc2 = st.columns(2)
with fc1:
    brands = sorted(df["brand"].unique().tolist())
    sel_brands = st.multiselect("🏷 브랜드", brands, default=brands)
with fc2:
    channels = sorted(df["channel"].unique().tolist())
    sel_channels = st.multiselect("🛒 판매처", channels, default=channels)

view = df[df["brand"].isin(sel_brands) & df["channel"].isin(sel_channels)].copy()

if view.empty:
    st.info("선택한 필터에 해당하는 행사가 없습니다.")
    st.stop()


# ============================================================
# 상단 KPI
# ============================================================
_this_month = _today.to_period("M")
ongoing = int((view["status"] == "진행중").sum())
upcoming = int((view["status"] == "예정").sum())
this_month_cnt = int(
    view["start"].dt.to_period("M").eq(_this_month).sum()
)
total_cnt = len(view)

k1, k2, k3, k4 = st.columns(4)
k1.metric("🔴 진행중", f"{ongoing}건")
k2.metric("🟡 예정", f"{upcoming}건")
k3.metric("📅 이번 달", f"{this_month_cnt}건")
k4.metric("📊 전체", f"{total_cnt}건")

st.markdown("---")


# ============================================================
# 간트 차트 — 행사 타임라인
# ============================================================
st.markdown("### 🗓 행사 타임라인")

gantt = view.sort_values("start").copy()
# 브랜드별 색상
brand_color_map = {
    b: BRAND_COLORS.get(b, {}).get("primary", "#94a3b8")
    for b in gantt["brand"].unique()
}

fig = px.timeline(
    gantt,
    x_start="start",
    x_end="end",
    y="name",
    color="brand",
    color_discrete_map=brand_color_map,
    hover_data={"channel": True, "status": True, "start": "|%Y-%m-%d",
                "end": "|%Y-%m-%d", "name": False},
)
fig.update_yaxes(autorange="reversed", title="")
fig.update_xaxes(title="", tickformat="%m/%d", showgrid=True,
                 gridcolor="#f1f5f9")
fig.add_vline(
    x=_today.timestamp() * 1000,
    line_width=2, line_dash="dash", line_color="#ef4444",
    annotation_text="오늘", annotation_position="top",
)
fig.update_layout(
    height=max(280, 42 * len(gantt) + 80),
    margin=dict(l=10, r=10, t=10, b=10),
    plot_bgcolor="white",
    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                xanchor="left", x=0, title=""),
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ============================================================
# 행사 리스트 — 카드 (진행중 → 예정 → 종료 순)
# ============================================================
st.markdown("### 📋 행사 목록")

_status_rank = {"진행중": 0, "예정": 1, "종료": 2}
_status_style = {
    "진행중": ("#dcfce7", "#166534", "🔴 진행중"),
    "예정": ("#fef9c3", "#854d0e", "🟡 예정"),
    "종료": ("#f1f5f9", "#64748b", "⚪ 종료"),
}

listing = view.copy()
listing["_rank"] = listing["status"].map(_status_rank)
# 진행중·예정은 시작 가까운 순, 종료는 최근 종료 순
listing = listing.sort_values(
    ["_rank", "start"], ascending=[True, True]
).reset_index(drop=True)

for _, ev in listing.iterrows():
    bc = BRAND_COLORS.get(ev["brand"], {})
    pri = bc.get("primary", "#64748b")
    soft = bc.get("bg_soft", "#f8fafc")
    txtc = bc.get("text", "#0f172a")
    bg, fg, badge = _status_style.get(ev["status"], _status_style["종료"])

    s_str = ev["start"].strftime("%Y.%m.%d")
    # 표시용 종료일은 +1 보정 되돌려서 자연스럽게
    e_disp = ev["end"]
    if (e_disp - ev["start"]).days == 1 and not ev["date_end"]:
        e_str = s_str
    else:
        e_str = e_disp.strftime("%Y.%m.%d")
    period = s_str if e_str == s_str else f"{s_str} → {e_str}"

    # D-day
    if ev["status"] == "예정":
        dday = (ev["start"].normalize() - _today).days
        dday_txt = f"D-{dday}" if dday > 0 else "D-DAY"
        dday_color = "#d97706"
    elif ev["status"] == "진행중":
        left = (ev["end"].normalize() - _today).days
        dday_txt = f"종료까지 {left}일" if left > 0 else "오늘 종료"
        dday_color = "#16a34a"
    else:
        ago = (_today - ev["end"].normalize()).days
        dday_txt = f"{ago}일 전 종료"
        dday_color = "#94a3b8"

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
            <span style="color:{dday_color}; font-size:0.86rem; font-weight:700;">{dday_txt}</span>
        </div>
    </div>
</div>
        """),
        unsafe_allow_html=True,
    )
