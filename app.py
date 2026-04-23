"""그로잉업팀 통합 마케팅 대시보드 — 홈 (Overview).

3개 브랜드:
  - 🍙 똑똑연구소 (유아식)     : 네이버 스마트스토어 · 자사몰 · 쿠팡 로켓그로스
  - 🧳 롤라루 (여행용품)        : 네이버 · 자사몰 · 쿠팡 로켓배송 · 무신사 · 오프라인 · 이지웰 · 오늘의집
  - 👟 루티니스트 (신규)        : 자사몰 · 네이버 스마트스토어 (시트 기반)

홈 구성 (V팀 대시보드 참고):
  1. 상단 KPI 4~6개 (시트 기반 — 목표·달성)
  2. 월별 누적 매출 추이 (목표 vs 실적 막대)
  3. 브랜드별 진도 3개 카드 (진도 바)
  4. 오늘의 성과 하이라이트
  5. 의사결정 Q1~Q3 (기존)
  6. 실시간 알림 피드
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.data import load_ads, load_orders, load_reviews
from utils.metrics import (
    TARGET_ROAS,
    calc_channel_metrics,
    calc_total_metrics,
    calc_repurchase,
)
from utils.products import (
    BRAND_MONTHLY_TARGETS,
    filter_orders_by_brand,
    filter_ads_by_brand,
)
from utils.alerts import check_all_alerts
from utils.ui import (
    setup_page, BRAND_PRIMARY, TEXT_MAIN, TEXT_MUTED,
    BORDER_SUBTLE, BG_CARD, BRAND_BADGES,
    format_won_compact, kpi_card, icon_card,
    status_color, status_badge,
)


def _flatten_html(html: str) -> str:
    """Markdown 코드블록 오인 방지: 모든 들여쓰기 제거 후 한 줄로."""
    return "".join(ln.strip() for ln in html.strip().split("\n"))
from api.google_sheets import load_sheet_daily_sales
from utils.insights import generate_insights, SEVERITY_STYLES


# ============================================================
# 페이지 설정
# ============================================================
setup_page(
    page_title="그로잉업팀 대시보드",
    page_icon="📊",
    header_title="📊 대시보드",
    header_subtitle="그로잉업팀 3개 브랜드 통합 — 똑똑연구소 · 롤라루 · 루티니스트",
)


# ============================================================
# 데이터 로드
# ============================================================
ads = load_ads()
orders = load_orders()
reviews = load_reviews()


@st.cache_data(ttl=300, show_spinner="구글 시트 매출 로드 중…")
def _cached_sheet() -> pd.DataFrame:
    try:
        df = load_sheet_daily_sales()
        # date 컬럼 datetime 보장
        if not df.empty and "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        st.warning(f"시트 로드 실패: {e}")
        # 빈 DF 도 올바른 dtype 으로 생성 (.dt accessor 에러 방지)
        return pd.DataFrame({
            "date": pd.Series([], dtype="datetime64[ns]"),
            "brand": pd.Series([], dtype="object"),
            "channel": pd.Series([], dtype="object"),
            "target": pd.Series([], dtype="int64"),
            "actual": pd.Series([], dtype="int64"),
        })


sheet_df = _cached_sheet()


# ============================================================
# 기간 필터 — 대시보드 제목 바로 아래 (인라인)
# ============================================================
today_real = date.today()
orders_max = orders["date"].max().date() if not orders.empty else today_real
orders_min = orders["date"].min().date() if not orders.empty else date(today_real.year, 1, 1)
min_allowed = min(orders_min, date(today_real.year, 1, 1))

hc1, hc2, hc3, _ = st.columns([1.3, 1.3, 1.3, 2])
with hc1:
    period = st.selectbox(
        "🗓️ 조회 기간",
        ["이번 달", "지난 7일", "지난 14일", "지난 30일",
         "지난 90일", "올해 누적", "사용자 지정"],
        index=0,
    )
with hc2:
    end_date_picked = st.date_input(
        "종료일",
        value=orders_max,
        min_value=min_allowed,
        max_value=today_real,
        help="실제 오늘까지 선택 가능. 매일 10시 자동 sync 후 업데이트.",
    )
today = end_date_picked

if period == "이번 달":
    start_date = pd.Timestamp(today.replace(day=1))
    end_date = pd.Timestamp(today)
    days = (end_date - start_date).days + 1
elif period == "올해 누적":
    start_date = pd.Timestamp(date(today.year, 1, 1))
    end_date = pd.Timestamp(today)
    days = (end_date - start_date).days + 1
elif period == "사용자 지정":
    with hc3:
        start_date_picked = st.date_input(
            "시작일",
            value=today - timedelta(days=6),
            min_value=min_allowed,
            max_value=today,
        )
    start_date = pd.Timestamp(start_date_picked)
    end_date = pd.Timestamp(today)
    days = (end_date - start_date).days + 1
else:
    days_map = {"지난 7일": 7, "지난 14일": 14, "지난 30일": 30, "지난 90일": 90}
    days = days_map[period]
    end_date = pd.Timestamp(today)
    start_date = end_date - pd.Timedelta(days=days - 1)

st.caption(f"📅 **{start_date.date()} ~ {end_date.date()}** ({days}일)")

# 사이드바 — 기준값 참조 (목표 ROAS / 월 매출 목표)
with st.sidebar:
    st.markdown("### 🎯 목표 ROAS")
    for ch, val in TARGET_ROAS.items():
        st.caption(f"• **{ch}**: {val * 100:.0f}%")

    st.divider()
    st.markdown("### 💰 월 매출 목표")
    total_target = sum(BRAND_MONTHLY_TARGETS.values())
    st.caption(f"• **전체**: {total_target:,}원")
    for b, t in BRAND_MONTHLY_TARGETS.items():
        st.caption(f"• **{b}**: {t:,}원")


# ============================================================
# 시트 기반 기간별 요약 계산
# ============================================================
def _sheet_period_summary(
    df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp,
    brand: str | None = None,
) -> dict:
    """선택 기간 · 브랜드의 시트 기반 요약 (target, actual, daily 등)."""
    sub = df[(df["date"] >= start) & (df["date"] <= end)]
    if brand:
        sub = sub[sub["brand"] == brand]

    total_target = int(sub["target"].sum())
    total_actual = int(sub["actual"].sum())
    achievement = (total_actual / total_target * 100) if total_target else 0

    # 일별 총합
    daily = (
        sub.groupby(sub["date"].dt.date)
        .agg(actual=("actual", "sum"), target=("target", "sum"))
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"])

    if not daily.empty:
        best = daily.loc[daily["actual"].idxmax()]
        best_day_date = best["date"].date()
        best_day_revenue = int(best["actual"])
        avg_daily = int(daily["actual"].mean())
        # 목표 도달 일수
        days_achieved = int((daily["actual"] >= daily["target"]).sum())
        total_days = len(daily)
    else:
        best_day_date = None
        best_day_revenue = 0
        avg_daily = 0
        days_achieved = 0
        total_days = 0

    return {
        "total_target": total_target,
        "total_actual": total_actual,
        "achievement_pct": achievement,
        "daily": daily,
        "best_day_date": best_day_date,
        "best_day_revenue": best_day_revenue,
        "avg_daily": avg_daily,
        "days_achieved": days_achieved,
        "total_days": total_days,
    }


overall = _sheet_period_summary(sheet_df, start_date, end_date)


# ============================================================
# 🎯 오늘의 하이라이트 — 최우선 확인 3개 카드 (홈 최상단)
# ============================================================
def _render_today_highlights() -> None:
    """홈 최상단 '오늘 이것만 확인하세요' 섹션 — 3개 액션 카드."""
    from utils.ui import render_insight_card
    from datetime import timedelta as _td

    highlights: list[str] = []   # (severity, title, detail, metric, metric_label, action)

    # 1) 브랜드별 달성률 체크 — 이번 달
    try:
        month_start = pd.Timestamp(today.replace(day=1))
        month_end = pd.Timestamp(today)
        brand_summary = []
        for b in ["똑똑연구소", "롤라루", "루티니스트"]:
            s = _sheet_period_summary(sheet_df, month_start, month_end, brand=b)
            pct = s["achievement_pct"]
            brand_summary.append((b, pct, s["total_actual"], s["total_target"]))

        # 가장 위험한/기회 큰 브랜드 선택
        sorted_by_pct = sorted(brand_summary, key=lambda x: x[1])
        worst_b, worst_pct, worst_actual, worst_target = sorted_by_pct[0]
        best_b, best_pct, best_actual, best_target = sorted_by_pct[-1]

        if worst_target > 0:
            if worst_pct < 60:
                gap = worst_target - worst_actual
                highlights.append(render_insight_card(
                    severity="critical",
                    title=f"{worst_b} 이달 달성률 {worst_pct:.0f}%",
                    detail=(
                        f"목표까지 <b>{gap:,}원</b> 부족. "
                        f"남은 기간 광고 강화 또는 프로모션 긴급 검토."
                    ),
                    metric_value=f"{worst_pct:.0f}%",
                    metric_label="달성률",
                    action_hint="💰 매출 분석에서 확인",
                ))
            elif worst_pct < 85:
                highlights.append(render_insight_card(
                    severity="warning",
                    title=f"{worst_b} 이달 달성률 {worst_pct:.0f}%",
                    detail=(
                        f"목표 대비 {100 - worst_pct:.0f}%p 미달. "
                        f"마감까지 전환 효율 점검 필요."
                    ),
                    metric_value=f"{worst_pct:.0f}%",
                    metric_label="달성률",
                    action_hint="💰 매출 분석에서 확인",
                ))
        if best_pct >= 100 and best_b != worst_b:
            highlights.append(render_insight_card(
                severity="success",
                title=f"{best_b} 목표 초과 {best_pct:.0f}%",
                detail=(
                    f"이달 목표 대비 <b>+{best_pct - 100:.0f}%p</b> 초과. "
                    f"성장 동력 유지 — 재고/마케팅 점검."
                ),
                metric_value=f"+{best_actual - best_target:,}",
                metric_label="추가 매출",
                action_hint="💰 매출 분석에서 확인",
            ))
    except Exception:
        pass

    # 2) 광고 효율 — 최근 7일 저효율 채널
    try:
        recent_ads = ads[
            (ads["date"] >= pd.Timestamp(today) - pd.Timedelta(days=6))
            & (ads["date"] <= pd.Timestamp(today))
        ]
        if not recent_ads.empty:
            by_ch = recent_ads.groupby("channel").agg(
                spend=("spend", "sum"),
                revenue=("revenue", "sum"),
            ).reset_index()
            by_ch = by_ch[by_ch["spend"] >= 50_000]   # 유의미 광고비 기준
            if not by_ch.empty:
                by_ch["roas"] = by_ch["revenue"] / by_ch["spend"] * 100
                worst_ch = by_ch.sort_values("roas").iloc[0]
                worst_ch_name = worst_ch["channel"]
                worst_ch_roas = worst_ch["roas"]
                target = TARGET_ROAS.get(worst_ch_name, 3.0) * 100
                if worst_ch_roas < target * 0.7 and len(highlights) < 3:
                    highlights.append(render_insight_card(
                        severity="warning",
                        title=f"{worst_ch_name} ROAS {worst_ch_roas:.0f}%",
                        detail=(
                            f"최근 7일 목표({target:.0f}%) 대비 저조. "
                            f"광고비 <b>{int(worst_ch['spend']):,}원</b> 투입 대비 "
                            f"매출 {int(worst_ch['revenue']):,}원. "
                            f"소재/타겟 재검토."
                        ),
                        metric_value=f"{worst_ch_roas:.0f}%",
                        metric_label="ROAS · 7일",
                        action_hint="📣 광고 분석에서 캠페인 확인",
                    ))
    except Exception:
        pass

    # 3) 데이터 신선도 — 마지막 sync 체크
    try:
        from utils.precomputed import get_last_updated
        last = get_last_updated()
        if last:
            now = datetime.now()
            hours_ago = int((now - last).total_seconds() / 3600)
            if hours_ago >= 26 and len(highlights) < 3:
                highlights.append(render_insight_card(
                    severity="caution",
                    title=f"데이터 {hours_ago}시간 전 동기화",
                    detail=(
                        "마지막 sync 가 하루를 넘겼습니다. "
                        "PC 전원 + 절전 설정 점검 또는 수동으로 "
                        "<code>sync_all.bat</code> 실행 권장."
                    ),
                    action_hint="⚙️ 설정에서 sync 로그 확인",
                ))
    except Exception:
        pass

    # 4) Fallback — 데이터 없을 때 전반 요약 제시
    if not highlights:
        highlights.append(render_insight_card(
            severity="info",
            title="오늘도 모든 지표 안정",
            detail=(
                "이번 달 주요 브랜드가 목표 범위 내에서 움직이고 있습니다. "
                "각 브랜드 탭에서 세부 성과를 확인하세요."
            ),
            action_hint="📊 전체 브랜드 성과 보기",
        ))

    if not highlights:
        return

    st.markdown(
        f"<h3 style='margin:0 0 12px 0; font-size:1.15rem; color:{TEXT_MAIN}; "
        f"font-weight:700; letter-spacing:-0.02em;'>🎯 오늘 이것만 확인하세요</h3>",
        unsafe_allow_html=True,
    )
    # 최대 3개
    cards = highlights[:3]
    cols = st.columns(len(cards))
    for col, card_html in zip(cols, cards):
        col.markdown(card_html, unsafe_allow_html=True)
    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)


_render_today_highlights()


# ============================================================
# 1. 상단 핵심 KPI 6개 — 커스텀 대형 카드
# ============================================================
st.markdown("### 📈 핵심 지표 (전체)")
st.caption(f"선택 기간: {start_date.date()} ~ {end_date.date()} ({days}일)")


k1, k2, k3, k4, k5, k6 = st.columns(6)

achievement = overall["achievement_pct"]
if achievement >= 100:
    pct_color, pct_status = "#16a34a", "목표 초과 달성"
elif achievement >= 80:
    pct_color, pct_status = "#ca8a04", "목표 근접"
elif achievement >= 50:
    pct_color, pct_status = "#ea580c", "목표 미달"
else:
    pct_color, pct_status = "#dc2626", "큰 폭 미달"

k1.markdown(
    kpi_card(
        "기간 매출",
        format_won_compact(overall["total_actual"]),
        sub=f"{overall['total_actual']:,}원",
        value_color="#2563eb",
    ),
    unsafe_allow_html=True,
)
k2.markdown(
    kpi_card(
        "기간 목표",
        format_won_compact(overall["total_target"]),
        sub=f"{overall['total_target']:,}원",
    ),
    unsafe_allow_html=True,
)
k3.markdown(
    kpi_card(
        "달성률",
        f"{achievement:.0f}%",
        sub=pct_status,
        value_color=pct_color,
    ),
    unsafe_allow_html=True,
)
k4.markdown(
    kpi_card(
        "일 평균 매출",
        format_won_compact(overall["avg_daily"]),
        sub=f"1일당 평균",
    ),
    unsafe_allow_html=True,
)
best_sub = (
    overall["best_day_date"].strftime("%m/%d")
    if overall["best_day_date"] else "—"
)
k5.markdown(
    kpi_card(
        "최고 매출일",
        format_won_compact(overall["best_day_revenue"]),
        sub=best_sub,
    ),
    unsafe_allow_html=True,
)
achieved_ratio = (
    overall["days_achieved"] / overall["total_days"] * 100
    if overall["total_days"] else 0
)
k6.markdown(
    kpi_card(
        "목표 도달일",
        f"{overall['days_achieved']}/{overall['total_days']}일",
        sub=f"{achieved_ratio:.0f}% 달성일 비율",
    ),
    unsafe_allow_html=True,
)


# ============================================================
# 2. 월별 매출 추이 — 목표 vs 실적 (V팀 참고 스타일)
# ============================================================
st.divider()
st.markdown("### 📊 월별 매출 추이 (목표 vs 실적)")

if not sheet_df.empty:
    year_data = sheet_df[sheet_df["date"].dt.year == today.year].copy()
    year_data["month"] = year_data["date"].dt.month

    monthly = (
        year_data.groupby("month")
        .agg(target=("target", "sum"), actual=("actual", "sum"))
        .reset_index()
    )
    monthly["diff"] = monthly["actual"] - monthly["target"]
    monthly["pct"] = (monthly["actual"] / monthly["target"].replace(0, pd.NA) * 100).fillna(0)
    monthly["has_actual"] = monthly["actual"] > 0

    # ---------- 상단 요약 KPI (차트 위) ----------
    completed = monthly[monthly["has_actual"]]
    if not completed.empty:
        ytd_target = int(completed["target"].sum())
        ytd_actual = int(completed["actual"].sum())
        ytd_pct = ytd_actual / ytd_target * 100 if ytd_target else 0
        diff_total = ytd_actual - ytd_target
        n_months = len(completed)

        summary_c1, summary_c2, summary_c3, summary_c4 = st.columns(4)
        with summary_c1:
            st.markdown(
                kpi_card(
                    "올해 누계 실적",
                    format_won_compact(ytd_actual),
                    sub=f"{n_months}개월치",
                    value_color="#2563eb",
                ),
                unsafe_allow_html=True,
            )
        with summary_c2:
            st.markdown(
                kpi_card(
                    "올해 누계 목표",
                    format_won_compact(ytd_target),
                    sub=f"{n_months}개월치",
                ),
                unsafe_allow_html=True,
            )
        with summary_c3:
            pct_color_now, _, _ = status_color(ytd_pct)
            st.markdown(
                kpi_card(
                    "누계 달성률",
                    f"{ytd_pct:.0f}%",
                    sub=f"{'+' if diff_total >= 0 else ''}{diff_total/100_000_000:.1f}억 차이",
                    value_color=pct_color_now,
                ),
                unsafe_allow_html=True,
            )
        with summary_c4:
            best_month = completed.loc[completed["pct"].idxmax()]
            st.markdown(
                kpi_card(
                    "최고 달성월",
                    f"{int(best_month['month'])}월 · {best_month['pct']:.0f}%",
                    sub=f"{format_won_compact(best_month['actual'])} 달성",
                    value_color="#16a34a",
                ),
                unsafe_allow_html=True,
            )
        st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

    # ---------- 월별 비교 차트 ----------
    fig = go.Figure()

    # X축: "1월" ~ "12월"
    month_labels = [f"{m}월" for m in monthly["month"]]

    # 목표 (연회색) — 전체 월
    fig.add_trace(
        go.Bar(
            name="월 목표",
            x=month_labels,
            y=monthly["target"],
            marker=dict(color="#e2e8f0", line=dict(width=0)),
            hovertemplate="<b>%{x}</b><br>목표 %{y:,.0f}원<extra></extra>",
            width=0.4,
        )
    )
    # 실적 (브랜드 블루) — 실적 있는 월만 색 진하게, 없는 월은 투명
    actual_colors = [
        BRAND_PRIMARY if a > 0 else "rgba(0,0,0,0)"
        for a in monthly["actual"]
    ]
    fig.add_trace(
        go.Bar(
            name="월 실적",
            x=month_labels,
            y=monthly["actual"],
            marker=dict(color=actual_colors, line=dict(width=0)),
            hovertemplate="<b>%{x}</b><br>실적 %{y:,.0f}원<extra></extra>",
            width=0.4,
        )
    )

    # 차이 주석 (실적이 있는 월만) — 큰 텍스트 + 배경 배지
    for _, row in monthly.iterrows():
        if row["has_actual"]:
            diff = row["diff"]
            color = "#16a34a" if diff >= 0 else "#dc2626"
            bg = "#dcfce7" if diff >= 0 else "#fee2e2"
            sign = "+" if diff >= 0 else ""
            if abs(diff) >= 100_000_000:
                diff_str = f"{sign}{diff / 100_000_000:.1f}억"
            elif abs(diff) >= 10_000:
                diff_str = f"{sign}{int(diff / 10_000):,}만"
            else:
                diff_str = f"{sign}{int(diff):,}"
            fig.add_annotation(
                x=f"{row['month']}월",
                y=max(row["target"], row["actual"]),
                text=f"<b>{diff_str}</b>",
                showarrow=False,
                yshift=22,
                font=dict(size=13, color=color, family="sans-serif"),
                bgcolor=bg,
                borderpad=4,
                bordercolor=color,
                borderwidth=0,
            )

    fig.update_layout(
        barmode="group",
        bargap=0.18,
        bargroupgap=0.05,
        height=380,
        margin=dict(l=10, r=10, t=50, b=10),
        legend=dict(
            orientation="h", y=-0.14, x=0.5, xanchor="center",
            font=dict(size=12),
        ),
        hovermode="x unified",
        plot_bgcolor="white",
        xaxis=dict(
            showgrid=False,
            tickfont=dict(size=12, color=TEXT_MAIN),
        ),
        yaxis=dict(
            gridcolor="#f1f5f9",
            tickformat=",",
            title=dict(text="매출(원)", font=dict(size=11, color=TEXT_MUTED)),
            tickfont=dict(size=11, color=TEXT_MUTED),
        ),
    )
    st.plotly_chart(fig, width="stretch", key="monthly_trend")
else:
    st.info("시트 데이터 없음 — 구글 시트 연동 확인 필요.")


# ============================================================
# 3. 브랜드별 진도 카드 (3개) — 가독성 강화
# ============================================================
st.divider()
st.markdown("### 🏷️ 브랜드별 진도")
st.caption(f"선택 기간: {start_date.date()} ~ {end_date.date()} · 구글 시트 실시간 기준")

brand_cols = st.columns(3)


def _fmt_won(value: float) -> str:
    """매출 금액을 가독성 있게 포맷: 1.2억 / 5,400만 / 1,234원"""
    v = int(value)
    if v >= 100_000_000:
        return f"{v / 100_000_000:.1f}억원"
    if v >= 10_000:
        return f"{v / 10_000:,.0f}만원"
    return f"{v:,}원"


def _status_info(pct: float) -> tuple[str, str, str]:
    """달성률 → (컬러, 이모지, 상태 라벨)."""
    if pct >= 100:
        return "#16a34a", "✓", "목표 달성"
    if pct >= 85:
        return "#16a34a", "↗", "목표 근접"
    if pct >= 60:
        return "#f59e0b", "▲", "노력 필요"
    return "#dc2626", "▼", "미달 주의"


for i, brand in enumerate(["똑똑연구소", "롤라루", "루티니스트"]):
    summary = _sheet_period_summary(sheet_df, start_date, end_date, brand=brand)
    cfg = BRAND_BADGES[brand]

    with brand_cols[i]:
        pct = summary["achievement_pct"]
        bar_pct = min(pct, 120) / 120 * 100  # 120%를 full bar 기준
        status_c, status_icon, status_label = _status_info(pct)

        actual_str = _fmt_won(summary["total_actual"])
        target_str = _fmt_won(summary["total_target"])
        days_info = (
            f"{summary['days_achieved']}/{summary['total_days']}일 도달"
            if summary["total_days"] > 0 else "데이터 없음"
        )

        card_html = f"""
<div style="background:{BG_CARD}; border:1px solid {BORDER_SUBTLE}; border-radius:16px; padding:22px 24px; box-shadow: 0 1px 3px rgba(15,23,42,0.04); position:relative; overflow:hidden;">
<div style="position:absolute; top:0; left:0; right:0; height:4px; background:{cfg['border']};"></div>
<div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:16px;">
<div style="display:flex; align-items:center; gap:10px;">
<div style="font-size:1.6rem;">{cfg['icon']}</div>
<div style="font-weight:700; color:{TEXT_MAIN}; font-size:1.05rem;">{brand}</div>
</div>
<div style="background:{status_c}15; color:{status_c}; padding:4px 10px; border-radius:20px; font-size:0.75rem; font-weight:600; display:flex; align-items:center; gap:4px;">
<span>{status_icon}</span><span>{status_label}</span>
</div>
</div>
<div style="display:flex; align-items:baseline; gap:8px; margin-bottom:6px;">
<div style="font-size:2.6rem; font-weight:800; color:{status_c}; letter-spacing:-0.03em; line-height:1;">{pct:.0f}</div>
<div style="font-size:1.2rem; font-weight:600; color:{status_c};">%</div>
</div>
<div style="color:{TEXT_MUTED}; font-size:0.82rem; margin-bottom:16px;">목표 달성률 · {days_info}</div>
<div style="background:#f1f5f9; border-radius:8px; height:14px; overflow:hidden; position:relative;">
<div style="width:{bar_pct}%; height:100%; background:linear-gradient(90deg, {status_c}, {status_c}cc); transition:width 0.3s;"></div>
<div style="position:absolute; left:83.3%; top:0; bottom:0; width:2px; background:rgba(15,23,42,0.15);"></div>
</div>
<div style="display:flex; justify-content:space-between; margin-top:6px; font-size:0.7rem; color:{TEXT_MUTED};">
<span>0%</span><span style="margin-left:auto;">100%</span><span style="width:17%; text-align:right;">120%+</span>
</div>
<div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:18px; padding-top:16px; border-top:1px solid {BORDER_SUBTLE};">
<div>
<div style="color:{TEXT_MUTED}; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.04em; font-weight:600; margin-bottom:4px;">실적</div>
<div style="color:{TEXT_MAIN}; font-size:1.05rem; font-weight:700;">{actual_str}</div>
</div>
<div>
<div style="color:{TEXT_MUTED}; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.04em; font-weight:600; margin-bottom:4px;">목표</div>
<div style="color:{TEXT_MAIN}; font-size:1.05rem; font-weight:700;">{target_str}</div>
</div>
</div>
</div>
"""
        st.markdown(_flatten_html(card_html), unsafe_allow_html=True)


# ============================================================
# 3.5. 💡 오늘의 인사이트 (룰 기반 자동 생성)
# ============================================================
st.divider()
st.markdown("### 💡 오늘의 인사이트")
st.caption(
    "데이터에서 바로 드러나는 사실 기반 자동 요약 · 우선순위 순 정렬"
)

insights = generate_insights(
    sheet_df, ads, orders, start_date, end_date, max_count=6,
)

if not insights:
    st.info("감지된 인사이트가 없습니다. (기간을 늘려서 다시 확인해보세요)")
else:
    # 2열 그리드로 표시
    for i in range(0, len(insights), 2):
        cols = st.columns(2)
        for j, insight in enumerate(insights[i:i + 2]):
            style = SEVERITY_STYLES.get(insight["severity"], SEVERITY_STYLES["info"])
            # 백슬래시 피하기 위해 변수로 먼저 추출
            s_bg = style["bg"]
            s_border = style["border"]
            s_label_bg = style["label_bg"]
            s_label_color = style["label_color"]
            s_label_text = style["label_text"]
            ins_icon = insight["icon"]
            ins_title = insight["title"]
            ins_detail = insight["detail"]

            with cols[j]:
                card_html = (
                    f"<div style='background:{s_bg}; "
                    f"border-left:4px solid {s_border}; "
                    f"border-radius:10px; padding:16px 20px; margin-bottom:12px; "
                    f"height:calc(100% - 12px);'>"
                    f"<div style='display:flex; align-items:center; gap:8px; margin-bottom:8px;'>"
                    f"<span style='font-size:1.2rem;'>{ins_icon}</span>"
                    f"<span style='background:{s_label_bg}; color:{s_label_color}; "
                    f"padding:2px 8px; border-radius:6px; font-size:0.7rem; "
                    f"font-weight:700; letter-spacing:0.02em;'>{s_label_text}</span>"
                    f"</div>"
                    f"<div style='font-weight:700; color:{TEXT_MAIN}; font-size:1rem; "
                    f"line-height:1.4; margin-bottom:6px;'>{ins_title}</div>"
                    f"<div style='color:{TEXT_MUTED}; font-size:0.85rem; line-height:1.5;'>"
                    f"{ins_detail}</div>"
                    f"</div>"
                )
                st.markdown(card_html, unsafe_allow_html=True)


# ============================================================
# 4. 오늘의 성과 하이라이트 — 커스텀 카드
# ============================================================
st.divider()
st.markdown("### ⚡ 오늘의 성과 하이라이트")
st.caption(f"기준일: {end_date.date()} (가장 최근 데이터)")

# 어제 기준
yesterday = end_date.date()
yesterday_orders = orders[orders["date"].dt.date == yesterday]
yesterday_ads = ads[ads["date"].dt.date == yesterday]

all_alerts = check_all_alerts(ads, orders)
urgent = sum(1 for a in all_alerts if a["severity"] == "urgent")
opportunity = sum(1 for a in all_alerts if a["severity"] == "opportunity")

cust = calc_repurchase(orders, cycle_days=30)
reached = cust[cust["cycle_reached"]]


h1, h2, h3, h4 = st.columns(4)

# 1) 어제 최고 매출 채널
with h1:
    if not yesterday_orders.empty:
        ch_rev = yesterday_orders.groupby("channel")["revenue"].sum().sort_values(ascending=False)
        top_ch = ch_rev.index[0]
        top_rev = int(ch_rev.iloc[0])
        st.markdown(
            icon_card(
                "🏆", "최고 매출 채널",
                top_ch,
                f"{top_rev:,}원 ({yesterday.strftime('%m/%d')})",
                main_color="#2563eb",
                accent_color="#dbeafe",
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            icon_card("🏆", "최고 매출 채널", "—", "데이터 없음"),
            unsafe_allow_html=True,
        )

# 2) 어제 최고 ROAS 채널
with h2:
    if not yesterday_ads.empty:
        by_ch = (
            yesterday_ads.groupby("channel")
            .agg(spend=("spend", "sum"), revenue=("revenue", "sum"))
            .reset_index()
        )
        by_ch["roas"] = (by_ch["revenue"] / by_ch["spend"].replace(0, pd.NA) * 100).fillna(0)
        by_ch = by_ch[by_ch["spend"] > 0].sort_values("roas", ascending=False)
        if not by_ch.empty:
            top = by_ch.iloc[0]
            st.markdown(
                icon_card(
                    "🎯", "최고 ROAS 채널",
                    f"{top['channel']} · {top['roas']:.0f}%",
                    f"광고비 {int(top['spend']):,}원 → 매출 {int(top['revenue']):,}원",
                    main_color="#16a34a",
                    accent_color="#dcfce7",
                ),
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                icon_card("🎯", "최고 ROAS 채널", "—", "광고 지출 없음"),
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            icon_card("🎯", "최고 ROAS 채널", "—", "데이터 없음"),
            unsafe_allow_html=True,
        )

# 3) 알림 건수
with h3:
    urgent_color = "#dc2626" if urgent > 0 else TEXT_MAIN
    urgent_accent = "#fee2e2" if urgent > 0 else "#f1f5f9"
    st.markdown(
        icon_card(
            "🚨", "긴급 알림",
            f"{urgent}건" if urgent > 0 else "없음",
            f"기회 포착 {opportunity}건",
            main_color=urgent_color,
            accent_color=urgent_accent,
        ),
        unsafe_allow_html=True,
    )

# 4) CRM 재구매 타이밍
with h4:
    st.markdown(
        icon_card(
            "👥", "재구매 타이밍",
            f"{len(reached):,}명",
            f"최근 주문 30~60일 경과",
            main_color="#f59e0b",
            accent_color="#fef3c7",
        ),
        unsafe_allow_html=True,
    )


# ============================================================
# 5. 의사결정 Q1~Q3 (기존 유지, 섹션 축소)
# ============================================================
st.divider()
st.markdown("### 🎯 의사결정 카드")

q_col1, q_col2 = st.columns([1, 1])

# Q1: 최고 효율 채널
with q_col1:
    with st.container(border=True):
        channel_metrics = calc_channel_metrics(ads, orders, start_date, end_date)
        if not channel_metrics.empty:
            sorted_ch = channel_metrics.sort_values("roas_gap", ascending=False)
            best = sorted_ch.iloc[0]
            st.markdown(
                f"<div style='color:{TEXT_MUTED}; font-size:0.82rem; margin-bottom:4px;'>"
                "Q1. 최고 효율 채널</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:1.3rem; font-weight:700; color:{TEXT_MAIN};'>"
                f"{best['channel']} · ROAS {best['roas']:.0f}%</div>",
                unsafe_allow_html=True,
            )
            st.caption(
                f"목표 대비 {best['roas_gap']:+.0f}%p · "
                f"광고비 {int(best['spend']):,}원 → 매출 {int(best['revenue']):,}원"
            )

# Q2: 재구매 타이밍
with q_col2:
    with st.container(border=True):
        st.markdown(
            f"<div style='color:{TEXT_MUTED}; font-size:0.82rem; margin-bottom:4px;'>"
            "Q2. 이번 주 CRM 대상</div>",
            unsafe_allow_html=True,
        )
        if len(reached) > 0:
            expected = int(reached["total_revenue"].mean() * len(reached) * 0.3)
            st.markdown(
                f"<div style='font-size:1.3rem; font-weight:700; color:{TEXT_MAIN};'>"
                f"{len(reached):,}명 · 예상 {expected:,}원</div>",
                unsafe_allow_html=True,
            )
            st.caption("30% 전환 가정 · '👥 CRM' 페이지에서 CSV 다운로드")
        else:
            st.markdown("**대상 없음**")
            st.caption("이번 주 리마인더 보낼 고객이 없습니다.")


# ============================================================
# 6. 실시간 알림 피드
# ============================================================
st.divider()
st.markdown("### 🚦 실시간 알림 피드")
st.caption("자동 감지 신호. 전체는 🚦 알림 센터 페이지 참조.")

if not all_alerts:
    st.info("감지된 알림이 없습니다.")
else:
    # 최대 5건까지만 홈에 표시
    for alert in all_alerts[:5]:
        body = (
            f"**{alert['title']}**  \n"
            f"{alert['message']}  \n"
            f"*근거: {alert['evidence']}*"
        )
        if alert["severity"] == "urgent":
            st.error(body)
        elif alert["severity"] == "opportunity":
            st.success(body)
        else:
            st.info(body)
    if len(all_alerts) > 5:
        st.caption(f"외 {len(all_alerts) - 5}건 — 🚦 알림 센터에서 전체 확인")


# ============================================================
# 푸터
# ============================================================
st.divider()
st.caption(
    "📊 그로잉업팀 (OZKIZ) · 5개 API 자동 수집 + 구글 시트 실시간 연동 · "
    "매일 오전 10시 자동 갱신"
)
