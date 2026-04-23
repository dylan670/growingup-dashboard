"""광고 통합 분석 — 브랜드 탭(전체/똑똑연구소/롤라루) × 채널별 성과 및 액션."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from plotly.subplots import make_subplots

from utils.data import load_ads
from utils.metrics import TARGET_ROAS, calc_daily_trend
from utils.actions import ad_channel_actions, THRESHOLDS
from utils.products import filter_ads_by_brand, BRAND_AD_STORES
from utils.ui import (
    setup_page, render_brand_banner,
    format_won_compact, kpi_card, status_color, TEXT_MUTED,
)
from api.meta_ads import load_meta_client
from api.naver_searchad import load_client_from_env as load_naver_client


setup_page(
    page_title="광고 분석",
    page_icon="📣",
    header_title="📣 광고 분석",
    header_subtitle="네이버 검색광고 · 쿠팡 광고 · 메타 광고 — 브랜드별 ROAS·효율·피로도",
)

# ==========================================================
# 데이터 로드 + 기간 선택
# ==========================================================
ads = load_ads()
from datetime import date as _today_func
today_real = _today_func.today()
ads_max = ads["date"].max().date() if not ads.empty else today_real
ads_min = ads["date"].min().date() if not ads.empty else _today_func(today_real.year - 1, 1, 1)

c1, c2, _ = st.columns([1, 1, 2])
with c1:
    period = st.selectbox(
        "비교 기간",
        ["최근 7일", "최근 14일", "최근 30일"], index=0,
    )
with c2:
    end_date = st.date_input(
        "종료일", value=ads_max,
        min_value=ads_min, max_value=today_real,
        help="실제 오늘까지 선택 가능.",
    )

days = {"최근 7일": 7, "최근 14일": 14, "최근 30일": 30}[period]
start_date = pd.Timestamp(end_date) - pd.Timedelta(days=days - 1)
st.caption(f"분석 기간: **{start_date.date()} ~ {end_date}** ({days}일)")


# ==========================================================
# 렌더링 함수 — 브랜드별 광고 분석
# ==========================================================
CHANNEL_LABELS = {
    "네이버": "네이버 검색광고",
    "쿠팡":   "쿠팡 광고",
    "자사몰": "메타 광고 (자사몰)",
}

CHANNEL_STATUS = {
    "네이버": ("tracked", "실 API 연동 · 매일 자동 갱신"),
    "쿠팡":   ("untracked", "쿠팡 광고비 **미집계** — 공식 API 미공개. advertising.coupang.com 에서 수동 확인"),
    "자사몰": ("tracked", "Meta Marketing API 연동 · 매일 자동 갱신"),
}


def render_untracked_card(ch_key: str, status_msg: str):
    with st.container(border=True):
        col_info, col_action = st.columns([2, 3])
        with col_info:
            st.markdown(f"### {CHANNEL_LABELS[ch_key]}")
            st.markdown(":grey[미집계]")
            st.caption(status_msg)
        with col_action:
            st.info(
                f"**{ch_key} 광고 데이터 없음**  \n"
                "이 채널은 자동 집계되지 않으므로 지표 계산에서 제외됩니다. "
                "수동 리포트는 각 플랫폼 광고 관리자에서 확인하세요."
            )


@st.cache_data(ttl=600, show_spinner="🔍 Meta 캠페인 로드 중...")
def _cached_meta_campaigns(brand: str, since_iso: str, until_iso: str):
    """Meta 캠페인 — 프리컴퓨트 우선, 없으면 live API."""
    # 프리컴퓨트 시도 (매일 10시 precompute.py 가 저장)
    try:
        from utils.precomputed import load_precomputed_parquet
        df = load_precomputed_parquet(f"meta_campaigns_{brand}.parquet")
        if not df.empty:
            return df
    except Exception:
        pass

    # Fallback: live API
    from datetime import date as _date
    client = load_meta_client(brand)
    if client is None:
        return None
    return client.fetch_campaigns_df(
        _date.fromisoformat(since_iso),
        _date.fromisoformat(until_iso),
    )


@st.cache_data(ttl=600, show_spinner="🔍 네이버 캠페인 로드 중...")
def _cached_naver_campaigns(since_iso: str, until_iso: str):
    """네이버 검색광고 캠페인 — 프리컴퓨트 우선."""
    try:
        from utils.precomputed import load_precomputed_parquet
        df = load_precomputed_parquet("naver_campaigns.parquet")
        if not df.empty:
            return df
    except Exception:
        pass

    from datetime import date as _date
    client = load_naver_client()
    if client is None:
        return None
    return client.fetch_campaigns_df(
        _date.fromisoformat(since_iso),
        _date.fromisoformat(until_iso),
    )


def _render_campaign_table(df: pd.DataFrame, target_roas_pct: float, key: str):
    """캠페인 테이블 — ROAS/광고비 기반 색상 행 표시."""
    if df.empty:
        st.info("캠페인 데이터 없음.")
        return

    # 문제 캠페인 플래그: 광고비 50만원+ 면서 ROAS < target × 0.5
    def _flag(row):
        if row["spend"] >= 500_000 and row["roas_pct"] < target_roas_pct * 0.5:
            return "🚨 낭비"
        if row["roas_pct"] >= target_roas_pct * 1.3 and row["spend"] > 0:
            return "✨ 우수"
        if row["spend"] >= 100_000 and row["roas_pct"] < target_roas_pct * 0.8:
            return "⚠️ 부진"
        if row["spend"] == 0:
            return "⏸ 정지"
        return ""

    display = df.copy()
    display["상태"] = display.apply(_flag, axis=1)
    display = display.rename(columns={
        "campaign_name": "캠페인",
        "spend": "광고비",
        "revenue": "매출",
        "roas_pct": "ROAS(%)",
        "impressions": "노출",
        "clicks": "클릭",
        "ctr_pct": "CTR(%)",
        "cpc": "CPC",
        "conversions": "전환",
    })

    display = display[[
        "상태", "캠페인", "광고비", "매출", "ROAS(%)",
        "전환", "클릭", "노출", "CTR(%)", "CPC",
    ]]

    # 합계 행 (요약)
    total_spend = int(df["spend"].sum())
    total_rev = int(df["revenue"].sum())
    total_conv = int(df["conversions"].sum())
    total_roas = (total_rev / total_spend * 100) if total_spend else 0

    st.caption(
        f"총 {len(df)}개 캠페인 · "
        f"광고비 {format_won_compact(total_spend)} → "
        f"매출 {format_won_compact(total_rev)} · "
        f"블렌디드 ROAS {total_roas:.0f}%"
    )

    st.dataframe(
        display,
        width="stretch",
        hide_index=True,
        column_config={
            "상태": st.column_config.TextColumn("상태", width="small"),
            "캠페인": st.column_config.TextColumn("캠페인", width="large"),
            "광고비": st.column_config.NumberColumn("광고비", format="%d원"),
            "매출": st.column_config.NumberColumn("매출", format="%d원"),
            "ROAS(%)": st.column_config.ProgressColumn(
                "ROAS",
                format="%d%%",
                min_value=0,
                max_value=max(int(target_roas_pct * 2), 400),
            ),
            "전환": st.column_config.NumberColumn("전환", format="%d"),
            "클릭": st.column_config.NumberColumn("클릭", format="%d"),
            "노출": st.column_config.NumberColumn("노출", format="%d"),
            "CTR(%)": st.column_config.NumberColumn("CTR", format="%.2f%%"),
            "CPC": st.column_config.NumberColumn("CPC", format="%d원"),
        },
        height=min(600, 50 + len(display) * 36),
        key=key,
    )

    # 📌 문제 캠페인 요약
    waste = df[(df["spend"] >= 500_000) & (df["roas_pct"] < target_roas_pct * 0.5)]
    if not waste.empty:
        waste_spend = int(waste["spend"].sum())
        waste_rev = int(waste["revenue"].sum())
        st.error(
            f"🚨 **문제 캠페인 {len(waste)}개 감지** — 광고비 "
            f"{format_won_compact(waste_spend)} 집행 → 매출 {format_won_compact(waste_rev)} "
            f"(ROAS 목표 {target_roas_pct:.0f}%의 절반 이하). "
            f"**즉시 일시정지 또는 소재/랜딩 재검토 권장.**"
        )
    winners = df[(df["roas_pct"] >= target_roas_pct * 1.3) & (df["spend"] >= 100_000)]
    if not winners.empty:
        best_name = winners.iloc[0]["campaign_name"][:40]
        st.success(
            f"✨ **우수 캠페인 {len(winners)}개** — 예산 증액 여지. "
            f"예: '{best_name}' ROAS {int(winners.iloc[0]['roas_pct'])}%"
        )


def _render_daily_spend_roas_chart(df: pd.DataFrame, brand_label: str):
    """일별 광고비(막대) + ROAS(선) 이중 축 차트. 채널별 stacked.

    Args:
        df: 기간 필터된 광고 DataFrame (date, channel, spend, revenue 포함)
        brand_label: 브랜드명 (차트 제목용)
    """
    # 일별 x 채널 집계
    daily_ch = (
        df.groupby([df["date"].dt.date, "channel"])
        .agg(spend=("spend", "sum"), revenue=("revenue", "sum"))
        .reset_index()
    )
    daily_ch["date"] = pd.to_datetime(daily_ch["date"])

    # 일별 총합 (ROAS 계산)
    daily_total = (
        df.groupby(df["date"].dt.date)
        .agg(spend=("spend", "sum"), revenue=("revenue", "sum"))
        .reset_index()
    )
    daily_total["date"] = pd.to_datetime(daily_total["date"])
    daily_total["roas"] = (
        daily_total["revenue"] / daily_total["spend"].replace(0, pd.NA) * 100
    ).round(0)

    # 이중 축 차트
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 광고비 — 채널별 stacked 막대
    channel_colors = {
        "네이버": "#22c55e",   # green
        "자사몰": "#2563eb",   # blue
        "쿠팡":   "#f97316",   # orange
    }
    for ch in sorted(daily_ch["channel"].unique()):
        ch_data = daily_ch[daily_ch["channel"] == ch]
        fig.add_trace(
            go.Bar(
                x=ch_data["date"],
                y=ch_data["spend"],
                name=f"{ch} 광고비",
                marker_color=channel_colors.get(ch, "#64748b"),
                opacity=0.85,
                hovertemplate="%{x|%m/%d}<br>%{y:,.0f}원<extra></extra>",
            ),
            secondary_y=False,
        )

    # ROAS — 선 그래프 (일별 총합 기준)
    fig.add_trace(
        go.Scatter(
            x=daily_total["date"],
            y=daily_total["roas"],
            name="블렌디드 ROAS",
            mode="lines+markers",
            line=dict(color="#dc2626", width=3),
            marker=dict(size=7, color="#dc2626"),
            hovertemplate="%{x|%m/%d}<br>ROAS %{y:.0f}%<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_layout(
        barmode="stack",
        height=340,
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(
            orientation="h",
            y=-0.18,
            x=0.5,
            xanchor="center",
            font=dict(size=11),
        ),
        hovermode="x unified",
        plot_bgcolor="white",
        xaxis=dict(
            showgrid=False,
            tickformat="%m/%d",
        ),
    )
    fig.update_yaxes(
        title_text="광고비 (원)",
        secondary_y=False,
        showgrid=True,
        gridcolor="#f1f5f9",
        tickformat=",",
    )
    fig.update_yaxes(
        title_text="ROAS (%)",
        secondary_y=True,
        showgrid=False,
        tickformat=",",
    )

    st.plotly_chart(
        fig, width="stretch",
        key=f"daily_spend_roas_{brand_label}",
    )


def render_ad_overview(
    ads_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    full_ads_df: pd.DataFrame,  # 트렌드 계산은 브랜드 전체 기간 필요
    brand: str | None = None,
):
    """브랜드별 광고 성과 렌더링."""
    brand_label = brand if brand else "전체"
    df = ads_df[(ads_df["date"] >= start) & (ads_df["date"] <= end)]

    # 이 브랜드에서 다룰 채널 — 전체면 3개 전부, 브랜드별이면 해당 store가 매핑되는 채널만
    if brand:
        brand_stores = set(BRAND_AD_STORES.get(brand, []))
        # store→channel 매핑 (네이버 검색광고가 브랜드별로 분리된 후 갱신)
        store_to_channel = {
            "네이버":              "네이버",   # 구버전 호환
            "네이버_똑똑연구소":   "네이버",
            "네이버_롤라루":       "네이버",
            "자사몰_똑똑연구소":   "자사몰",
            "자사몰_롤라루":       "자사몰",
        }
        available_channels = sorted({
            store_to_channel[s] for s in brand_stores if s in store_to_channel
        })
        # 쿠팡은 똑똑연구소에만 표시 (쿠팡 로켓그로스는 똑똑연구소 제품)
        if brand == "똑똑연구소":
            available_channels.append("쿠팡")
            available_channels = sorted(set(available_channels))
    else:
        available_channels = ["네이버", "쿠팡", "자사몰"]

    # ---------- 전체 KPI ----------
    total_spend = df["spend"].sum()
    total_rev = df["revenue"].sum()
    total_conv = df["conversions"].sum()
    blended_roas = total_rev / total_spend * 100 if total_spend else 0

    st.markdown(f"#### 📈 {brand_label} 광고 요약")
    k1, k2, k3, k4 = st.columns(4)

    # ROAS 상태 컬러
    roas_color, _, roas_label = status_color(blended_roas)

    k1.markdown(
        kpi_card(
            "총 광고비",
            format_won_compact(total_spend),
            sub=f"{int(total_spend):,}원",
        ),
        unsafe_allow_html=True,
    )
    k2.markdown(
        kpi_card(
            "총 전환매출",
            format_won_compact(total_rev),
            sub=f"{int(total_rev):,}원",
            value_color="#2563eb",
        ),
        unsafe_allow_html=True,
    )
    k3.markdown(
        kpi_card(
            "블렌디드 ROAS",
            f"{blended_roas:.0f}%",
            sub=roas_label,
            value_color=roas_color,
        ),
        unsafe_allow_html=True,
    )
    k4.markdown(
        kpi_card(
            "총 구매전환",
            f"{int(total_conv):,}건",
            sub=f"전환당 {(int(total_spend)/int(total_conv)):,.0f}원" if total_conv else "—",
        ),
        unsafe_allow_html=True,
    )

    # ---------- 브랜드 일별 광고비 + ROAS 추이 ----------
    if not df.empty:
        st.markdown(f"#### 📈 {brand_label} 일별 광고비 & ROAS 추이")
        _render_daily_spend_roas_chart(df, brand_label)

    # ---------- 채널별 카드 ----------
    st.divider()
    st.markdown(f"#### 📊 {brand_label} 채널별 성과 및 액션")

    for ch in available_channels:
        status, status_msg = CHANNEL_STATUS.get(ch, ("unknown", ""))
        ch_data = df[df["channel"] == ch]

        if status == "untracked":
            render_untracked_card(ch, status_msg)
            continue

        if ch_data.empty:
            render_untracked_card(ch, f"해당 기간 데이터 없음. ({status_msg})")
            continue

        spend = int(ch_data["spend"].sum())
        rev = int(ch_data["revenue"].sum())
        clicks = int(ch_data["clicks"].sum())
        imp = int(ch_data["impressions"].sum())
        conv = int(ch_data["conversions"].sum())

        roas = rev / spend * 100 if spend else 0
        ctr = clicks / imp * 100 if imp else 0
        cvr = conv / clicks * 100 if clicks else 0
        target = TARGET_ROAS.get(ch, 3.0) * 100

        # 트렌드는 full_ads_df (이 브랜드의 전체 기간)에서 계산
        daily = calc_daily_trend(full_ads_df, ch)
        daily_last3 = daily.tail(3)
        last3_roas = daily_last3["roas"].fillna(0).tolist() if len(daily_last3) == 3 else []

        ctr_recent_7d = 0.0
        ctr_prev_7d = 0.0
        if len(daily) >= 14:
            ctr_recent_7d = float(daily.tail(7)["ctr"].fillna(0).mean())
            ctr_prev_7d = float(daily.iloc[-14:-7]["ctr"].fillna(0).mean())

        metrics = {
            "channel": ch, "spend": spend, "revenue": rev,
            "conversions": conv, "clicks": clicks, "impressions": imp,
            "roas": roas,
            "roas_trend_3d": last3_roas,
            "ctr_recent_7d": ctr_recent_7d,
            "ctr_prev_7d": ctr_prev_7d,
        }
        actions = ad_channel_actions(metrics)

        with st.container(border=True):
            col_info, col_chart, col_action = st.columns([2, 2, 3])

            with col_info:
                st.markdown(f"### {CHANNEL_LABELS[ch]}")
                m1, m2 = st.columns(2)
                m1.metric("광고비", f"{spend:,}원")
                m2.metric("매출", f"{rev:,}원")
                m1.metric("ROAS", f"{roas:.0f}%",
                          delta=f"목표 {target:.0f}% 대비 {roas - target:+.0f}%p")
                m2.metric("구매전환", f"{conv:,}건")
                if ctr > 0 or cvr > 0:
                    st.caption(f"CTR **{ctr:.2f}%** · CVR **{cvr:.2f}%**")

            with col_chart:
                st.markdown("**일별 ROAS 추이 (최근 14일)**")
                chart_data = daily.tail(14)
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=chart_data["date"], y=chart_data["roas"],
                    mode="lines+markers",
                    line=dict(color="#2563eb", width=2),
                    marker=dict(size=6),
                ))
                fig.add_hline(
                    y=target, line_dash="dash", line_color="#dc2626",
                    annotation_text=f"목표 {target:.0f}%",
                    annotation_position="right",
                )
                fig.update_layout(
                    height=200, margin=dict(l=10, r=10, t=10, b=10),
                    yaxis_title="ROAS(%)", showlegend=False,
                )
                st.plotly_chart(fig, width="stretch",
                                key=f"chart_{brand_label}_{ch}")

            with col_action:
                st.markdown("**추천 액션**")
                for a in actions:
                    sev = a["severity"]
                    body = f"**{a['label']}**  \n{a['detail']}"
                    if sev == "critical":
                        st.error(body)
                    elif sev == "warning":
                        st.warning(body)
                    elif sev == "opportunity":
                        st.success(body)
                    elif sev == "info":
                        st.info(body)
                    else:
                        st.caption(body)

    # ---------- 캠페인 Drill-down ----------
    if brand in ("똑똑연구소", "롤라루"):
        st.divider()
        st.markdown(f"#### 🔍 {brand_label} 캠페인 상세")
        st.caption(
            "캠페인 단위 광고비·매출·ROAS · 문제 캠페인 자동 감지 "
            "(광고비 50만원+ & ROAS 목표의 50% 미만 → 🚨 낭비)"
        )

        start_iso = str(start.date())
        end_iso = str(end.date())

        # Meta 캠페인 (자사몰)
        with st.expander(f"📣 Meta 광고 ({brand}) 캠페인 목록", expanded=False):
            try:
                meta_camp = _cached_meta_campaigns(brand, start_iso, end_iso)
                if meta_camp is None:
                    st.warning(f"Meta {brand} API 클라이언트 로드 실패 (자격증명 확인)")
                elif meta_camp.empty:
                    st.info("선택 기간 Meta 캠페인 데이터 없음.")
                else:
                    target_meta = TARGET_ROAS.get("자사몰", 2.5) * 100
                    _render_campaign_table(
                        meta_camp, target_meta,
                        key=f"camp_meta_{brand}_{start_iso}",
                    )
            except Exception as e:
                st.error(f"Meta 캠페인 조회 실패: {type(e).__name__}: {e}")

        # 네이버 검색광고 (브랜드 필터 적용)
        with st.expander(f"📣 네이버 검색광고 ({brand}) 캠페인 목록", expanded=False):
            try:
                naver_camp = _cached_naver_campaigns(start_iso, end_iso)
                if naver_camp is None:
                    st.warning("네이버 검색광고 API 클라이언트 로드 실패")
                elif naver_camp.empty:
                    st.info("선택 기간 네이버 캠페인 데이터 없음.")
                else:
                    brand_camp = naver_camp[naver_camp["brand"] == brand]
                    if brand_camp.empty:
                        st.info(f"{brand} 소속 캠페인 없음.")
                    else:
                        target_naver = TARGET_ROAS.get("네이버", 4.0) * 100
                        _render_campaign_table(
                            brand_camp, target_naver,
                            key=f"camp_naver_{brand}_{start_iso}",
                        )
            except Exception as e:
                st.error(f"네이버 캠페인 조회 실패: {type(e).__name__}: {e}")


# ==========================================================
# 브랜드 탭
# ==========================================================
tab_all, tab_ddok, tab_rolla, tab_ruti = st.tabs([
    "📊 전체",
    "🍙 똑똑연구소",
    "🧳 롤라루",
    "👟 루티니스트",
])

with tab_all:
    st.caption("전체 채널 광고 합산 (네이버 · Meta). 브랜드별 캠페인 상세는 각 브랜드 탭에서.")
    render_ad_overview(ads, start_date, pd.Timestamp(end_date), ads, brand=None)

with tab_ddok:
    render_brand_banner(
        "똑똑연구소",
        "네이버 검색광고 (김똑똑/떡뻥) · Meta 광고 · 쿠팡 광고(미집계)",
    )
    ddok_ads = filter_ads_by_brand(ads, "똑똑연구소")
    render_ad_overview(
        ddok_ads, start_date, pd.Timestamp(end_date), ddok_ads,
        brand="똑똑연구소",
    )

with tab_rolla:
    render_brand_banner(
        "롤라루",
        "Meta 광고 · 네이버 검색광고 (롤라루 쇼핑검색)",
    )
    rolla_ads = filter_ads_by_brand(ads, "롤라루")
    render_ad_overview(
        rolla_ads, start_date, pd.Timestamp(end_date), rolla_ads,
        brand="롤라루",
    )

with tab_ruti:
    render_brand_banner(
        "루티니스트",
        "현재 광고 API 미연동 (Meta/네이버 검색광고 계정 없음)",
    )
    st.info(
        "👟 **루티니스트 광고 데이터 없음** — 대시보드에서 추적 중인 광고 계정이 없습니다. "
        "Meta/네이버 검색광고 계정 연결 시 자동으로 이 탭에 추가됩니다. "
        "매출 추적은 **💰 매출 분석 → 👟 루티니스트** 탭에서 구글 시트 기반으로 확인 가능."
    )


# ==========================================================
# 우리 기준 요약
# ==========================================================
st.divider()
with st.expander("우리 기준 (현재 값)"):
    st.markdown(f"""
- **목표 ROAS**: 네이버 {THRESHOLDS['target_roas']['네이버']}% / 쿠팡 {THRESHOLDS['target_roas']['쿠팡']}% / 자사몰 {THRESHOLDS['target_roas']['자사몰']}%
- **예산 증액 조건**: ROAS 3일 연속 목표 × {THRESHOLDS['roas_up_ratio']} 이상
- **소재 피로도 판정**: 최근 7일 CTR이 직전 7일 대비 {THRESHOLDS['ctr_fatigue'] * 100:.0f}% 이하 하락

기준 조정은 `utils/actions.py`의 `THRESHOLDS` 딕셔너리 수정.
""")
