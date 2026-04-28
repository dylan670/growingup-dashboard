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
    render_period_picker,
    METRIC_COLORS, CHANNEL_COLORS, TEXT_MAIN,
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

_pp = render_period_picker(
    max_date=ads_max, min_date=ads_min,
    key_prefix="ads", default_option="최근 7일",
)
period = _pp["period"]
start_date = _pp["start_date"]
end_date = _pp["end_date"].date()
days = _pp["days"]


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
    "쿠팡":   ("tracked", "CSV 업로드 기반 · 광고센터 리포트 주 1회 병합"),
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


def _aggregate_daily_campaigns(
    daily_df: pd.DataFrame, since_iso: str, until_iso: str,
) -> pd.DataFrame:
    """일자 단위 캠페인 parquet → 선택 기간 합산 + 파생 지표 계산."""
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    since = pd.Timestamp(since_iso)
    until = pd.Timestamp(until_iso)
    mask = (df["date"] >= since) & (df["date"] <= until)
    df = df[mask]
    if df.empty:
        return pd.DataFrame()

    group_cols = ["campaign_id", "campaign_name"]
    if "brand" in df.columns:
        group_cols.append("brand")

    agg = (
        df.groupby(group_cols)
        .agg(
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            conversions=("conversions", "sum"),
            revenue=("revenue", "sum"),
        )
        .reset_index()
    )

    agg["ctr_pct"] = (
        agg["clicks"] / agg["impressions"].replace(0, pd.NA) * 100
    ).round(2).fillna(0)
    agg["cpc"] = (
        agg["spend"] / agg["clicks"].replace(0, pd.NA)
    ).round(0).fillna(0).astype(int)
    agg["roas_pct"] = (
        agg["revenue"] / agg["spend"].replace(0, pd.NA) * 100
    ).round(0).fillna(0).astype(int)

    agg = agg.sort_values("spend", ascending=False).reset_index(drop=True)
    return agg


# 캐시 버전 (loader 로직 바뀌면 증가시켜 기존 Streamlit 캐시 무효화)
_CAMP_LOADER_VERSION = "v2-daily"


@st.cache_data(ttl=600, show_spinner="🔍 Meta 캠페인 로드 중...")
def _cached_meta_campaigns(
    brand: str, since_iso: str, until_iso: str,
    _cache_ver: str = _CAMP_LOADER_VERSION,
):
    """Meta 캠페인 — 일자 단위 프리컴퓨트 읽어 기간 슬라이스 + 집계."""
    from utils.precomputed import load_precomputed_parquet

    # 1) 우선: 일자 단위 프리컴퓨트 (매일 10시 precompute.py 저장)
    try:
        daily = load_precomputed_parquet(f"meta_campaigns_{brand}_daily.parquet")
        if not daily.empty:
            return _aggregate_daily_campaigns(daily, since_iso, until_iso)
    except Exception:
        pass

    # 2) Fallback: live API (Korean IP 필요 — Streamlit Cloud 에선 실패 가능)
    from datetime import date as _date
    client = load_meta_client(brand)
    if client is None:
        return None
    return client.fetch_campaigns_df(
        _date.fromisoformat(since_iso),
        _date.fromisoformat(until_iso),
    )


@st.cache_data(ttl=600, show_spinner="🔍 네이버 캠페인 로드 중...")
def _cached_naver_campaigns(
    since_iso: str, until_iso: str,
    _cache_ver: str = _CAMP_LOADER_VERSION,
):
    """네이버 검색광고 캠페인 — 일자 단위 프리컴퓨트 기반."""
    from utils.precomputed import load_precomputed_parquet

    # 1) 일자 단위 프리컴퓨트
    try:
        daily = load_precomputed_parquet("naver_campaigns_daily.parquet")
        if not daily.empty:
            return _aggregate_daily_campaigns(daily, since_iso, until_iso)
    except Exception:
        pass

    # 2) Fallback: live API
    from datetime import date as _date
    client = load_naver_client()
    if client is None:
        return None
    return client.fetch_campaigns_df(
        _date.fromisoformat(since_iso),
        _date.fromisoformat(until_iso),
    )


@st.cache_data(ttl=600, show_spinner="🔍 쿠팡 캠페인 로드 중...")
def _cached_coupang_campaigns(
    since_iso: str | None = None, until_iso: str | None = None,
    _cache_ver: str = _CAMP_LOADER_VERSION,
):
    """쿠팡 광고 캠페인 — CSV 업로드 기반 (일자 단위 parquet 슬라이스)."""
    from utils.precomputed import load_precomputed_parquet

    if since_iso and until_iso:
        try:
            daily = load_precomputed_parquet("coupang_campaigns_daily.parquet")
            if not daily.empty:
                agg = _aggregate_daily_campaigns(daily, since_iso, until_iso)
                if not agg.empty:
                    return agg
        except Exception:
            pass

    # Fallback: legacy 전체 합계 (아직 일자 parquet 없을 때)
    try:
        df = load_precomputed_parquet("coupang_campaigns.parquet")
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


# ==========================================================
# 캠페인 개별 진단 — ROAS/CTR/CPC/전환률 기반 규칙
# ==========================================================
def _diagnose_campaign(row: pd.Series, target_roas_pct: float, channel: str) -> dict:
    """캠페인 1개 진단. 반환: {severity, title, detail}.

    severity:
        critical   — 즉시 조치 (🚨 낭비)
        warning    — 주의 (⚠️ 부진/피로도)
        opportunity — 기회 (✨ 우수/증액)
        neutral    — 정상/정지
    """
    spend = int(row.get("spend", 0) or 0)
    revenue = int(row.get("revenue", 0) or 0)
    roas = float(row.get("roas_pct", 0) or 0)
    ctr = float(row.get("ctr_pct", 0) or 0)
    cpc = int(row.get("cpc", 0) or 0)
    clicks = int(row.get("clicks", 0) or 0)
    impressions = int(row.get("impressions", 0) or 0)
    conversions = int(row.get("conversions", 0) or 0)
    name = str(row.get("campaign_name", "") or "")

    # 0. 정지
    if spend == 0:
        return {
            "severity": "neutral",
            "title": "⏸ 집행 중지",
            "detail": "해당 기간 광고비 0원. 재시작 여부 전략 검토 필요.",
        }

    # 1. 즉시 조치 (대규모 낭비)
    if spend >= 500_000 and roas < target_roas_pct * 0.5:
        return {
            "severity": "critical",
            "title": "🚨 즉시 정지 검토",
            "detail": (
                f"광고비 {spend:,}원 집행 → 매출 {revenue:,}원 "
                f"(ROAS {roas:.0f}% · 목표 {target_roas_pct:.0f}%의 절반 미만). "
                "소재/랜딩/타겟 전면 재검토 또는 일시정지 권장."
            ),
        }

    # 2. 클릭 대비 전환 0
    if clicks >= 50 and conversions == 0:
        return {
            "severity": "critical",
            "title": "⚠️ 클릭 대비 전환 0",
            "detail": (
                f"클릭 {clicks}회 · 광고비 {spend:,}원 투입했지만 전환 0건. "
                "랜딩페이지 가격/후기/배송 정보 점검 및 A/B 테스트 우선."
            ),
        }

    # 3. 우수 캠페인 (증액 기회)
    if roas >= target_roas_pct * 1.3 and spend >= 100_000:
        new_budget = int(spend * 1.5)
        return {
            "severity": "opportunity",
            "title": "✨ 예산 증액 기회",
            "detail": (
                f"ROAS {roas:.0f}% (목표 {target_roas_pct:.0f}%의 1.3배 이상) "
                f"· 광고비 {spend:,}원. 기간 예산 +50%(≈ {new_budget:,}원) "
                "단계적 증액 검토."
            ),
        }

    # 4. 성과 부진
    if spend >= 100_000 and roas < target_roas_pct * 0.8:
        return {
            "severity": "warning",
            "title": "⚠️ 성과 부진",
            "detail": (
                f"ROAS {roas:.0f}% (목표 {target_roas_pct:.0f}% 미달) · "
                f"광고비 {spend:,}원. 소재 A/B 또는 키워드·타겟 재조정 필요."
            ),
        }

    # 5. 소재 피로도 (노출 많은데 CTR 낮음) — 채널별 임계
    ctr_threshold = {
        "네이버": 1.0,    # 검색광고 일반 평균 1.5%+
        "자사몰": 0.7,    # Meta 평균 0.8~1.2%
        "쿠팡":   0.5,    # 쿠팡 광고 통상 0.6~1.0%
    }.get(channel, 0.8)
    if impressions >= 10_000 and ctr > 0 and ctr < ctr_threshold:
        return {
            "severity": "warning",
            "title": "🖼 썸네일/카피 피로",
            "detail": (
                f"노출 {impressions:,}회 · CTR {ctr:.2f}% "
                f"(평균 {ctr_threshold:.1f}% 이하). "
                "썸네일/헤드라인/오퍼 재검토 — 신규 소재 2안 준비."
            ),
        }

    # 6. CPC 과다 (검색광고 전용)
    if channel == "네이버" and cpc >= 2000 and roas < target_roas_pct:
        return {
            "severity": "warning",
            "title": "💰 CPC 과다",
            "detail": (
                f"CPC {cpc:,}원 · ROAS {roas:.0f}% (목표 {target_roas_pct:.0f}%). "
                "입찰가 하향 또는 롱테일 키워드 중심 재편 검토."
            ),
        }

    # 7. 저지출 (테스트 단계)
    if spend < 50_000:
        return {
            "severity": "info",
            "title": "🧪 테스트 집행",
            "detail": (
                f"광고비 {spend:,}원 · ROAS {roas:.0f}%. "
                "판단 지표 부족 — 광고비 증액 후 재평가 또는 유지."
            ),
        }

    # 8. 정상 범위
    return {
        "severity": "neutral",
        "title": "✓ 정상 범위",
        "detail": (
            f"ROAS {roas:.0f}% · CTR {ctr:.2f}% · 광고비 {spend:,}원. "
            "현 운영 유지."
        ),
    }


def _render_campaign_diagnosis(
    df: pd.DataFrame,
    target_roas_pct: float,
    channel: str,
    max_items: int = 10,
):
    """캠페인별 진단 카드 렌더링 — 심각도 순 정렬 후 상위 N개."""
    if df.empty:
        return

    # 진단 생성
    severity_order = {"critical": 0, "warning": 1, "opportunity": 2, "info": 3, "neutral": 4}
    diagnosed = []
    for _, row in df.iterrows():
        d = _diagnose_campaign(row, target_roas_pct, channel)
        d["row"] = row
        d["order"] = severity_order.get(d["severity"], 99)
        diagnosed.append(d)

    # 심각도 우선 → 심각도 같으면 광고비 큰 순
    diagnosed.sort(
        key=lambda x: (x["order"], -int(x["row"].get("spend", 0) or 0))
    )

    shown = diagnosed[:max_items]
    hidden = len(diagnosed) - len(shown)

    st.markdown(f"**🩺 캠페인별 진단 ({len(shown)}/{len(diagnosed)}개 표시)**")

    for d in shown:
        row = d["row"]
        name = str(row.get("campaign_name", "") or "(이름 없음)")
        spend = int(row.get("spend", 0) or 0)
        roas = float(row.get("roas_pct", 0) or 0)
        body = (
            f"**{d['title']} · {name[:60]}**  \n"
            f"{d['detail']}  \n"
            f":grey[광고비 {spend:,}원 · ROAS {roas:.0f}%]"
        )
        sev = d["severity"]
        if sev == "critical":
            st.error(body)
        elif sev == "warning":
            st.warning(body)
        elif sev == "opportunity":
            st.success(body)
        elif sev == "info":
            st.info(body)
        else:
            st.markdown(
                f"<div style='padding:8px 12px; background:#f8fafc; "
                f"border-left:3px solid #94a3b8; border-radius:4px; "
                f"margin-bottom:8px; font-size:0.9em;'>{body}</div>",
                unsafe_allow_html=True,
            )

    if hidden > 0:
        st.caption(f":grey[… 정상 범위 {hidden}개 캠페인 생략]")


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

    # 광고비 — 채널별 stacked 막대 (통일 팔레트)
    for ch in sorted(daily_ch["channel"].unique()):
        ch_data = daily_ch[daily_ch["channel"] == ch]
        fig.add_trace(
            go.Bar(
                x=ch_data["date"],
                y=ch_data["spend"],
                name=f"{ch} 광고비",
                marker_color=CHANNEL_COLORS.get(ch, "#64748b"),
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
            line=dict(color=METRIC_COLORS["roas"], width=3),
            marker=dict(size=7, color=METRIC_COLORS["roas"]),
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
        # store→channel 매핑 (네이버/자사몰/쿠팡 브랜드별로 분리된 후 갱신)
        store_to_channel = {
            "네이버":              "네이버",   # 구버전 호환
            "네이버_똑똑연구소":   "네이버",
            "네이버_롤라루":       "네이버",
            "자사몰_똑똑연구소":   "자사몰",
            "자사몰_롤라루":       "자사몰",
            "쿠팡":                "쿠팡",     # 구버전 호환
            "쿠팡_똑똑연구소":     "쿠팡",
            "쿠팡_롤라루":         "쿠팡",
        }
        available_channels = sorted({
            store_to_channel[s] for s in brand_stores if s in store_to_channel
        })
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
                # 광고비·매출은 자릿수가 길어 col_info 전폭 사용 (세로 스택)
                st.metric("광고비", f"{spend:,}원")
                st.metric("매출", f"{rev:,}원")
                # ROAS/전환 은 짧아서 가로 분할 OK
                mc1, mc2 = st.columns(2)
                mc1.metric(
                    "ROAS", f"{roas:.0f}%",
                    delta=f"목표 {target:.0f}% 대비 {roas - target:+.0f}%p",
                )
                mc2.metric("구매전환", f"{conv:,}건")
                if ctr > 0 or cvr > 0:
                    st.caption(f"CTR **{ctr:.2f}%** · CVR **{cvr:.2f}%**")

            with col_chart:
                st.markdown(f"**일별 ROAS 추이 (최근 {days}일)**")
                chart_data = daily.tail(days)
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

    # ---------- 캠페인별 상세 및 진단 ----------
    if brand in ("똑똑연구소", "롤라루", "루티니스트") or brand is None:
        st.divider()
        st.markdown(f"#### 🔍 {brand_label} 캠페인별 상세 및 진단")
        st.caption(
            "캠페인 단위 광고비·매출·ROAS · 자동 진단 "
            "(🚨 즉시 조치 · ⚠️ 주의 · ✨ 증액 기회 · 🧪 테스트 · ✓ 정상)"
        )

        start_iso = str(start.date())
        end_iso = str(end.date())
        target_naver = TARGET_ROAS.get("네이버", 4.0) * 100
        target_meta = TARGET_ROAS.get("자사몰", 3.0) * 100
        target_coupang = TARGET_ROAS.get("쿠팡", 5.0) * 100

        # ----- 네이버 검색광고 -----
        naver_header = (
            f"📣 네이버 검색광고 ({brand}) 캠페인"
            if brand else "📣 네이버 검색광고 (전체) 캠페인"
        )
        with st.expander(naver_header, expanded=False):
            try:
                naver_camp = _cached_naver_campaigns(start_iso, end_iso)
                if naver_camp is None:
                    st.warning("네이버 검색광고 API 클라이언트 로드 실패")
                elif naver_camp.empty:
                    st.info("선택 기간 네이버 캠페인 데이터 없음.")
                else:
                    if brand:
                        filt_camp = naver_camp[naver_camp["brand"] == brand]
                    else:
                        filt_camp = naver_camp
                    if filt_camp.empty:
                        st.info(f"{brand_label} 소속 네이버 캠페인 없음.")
                    else:
                        _render_campaign_table(
                            filt_camp, target_naver,
                            key=f"camp_naver_{brand_label}_{start_iso}",
                        )
                        st.markdown("")
                        _render_campaign_diagnosis(
                            filt_camp, target_naver, "네이버",
                        )
            except Exception as e:
                st.error(f"네이버 캠페인 조회 실패: {type(e).__name__}: {e}")

        # ----- Meta 광고 -----
        # 전체 탭은 브랜드별 Meta 를 합쳐서 보여주고, 브랜드 탭은 해당 브랜드만.
        meta_header = (
            f"📣 Meta 광고 ({brand}) 캠페인"
            if brand else "📣 Meta 광고 (전체) 캠페인"
        )
        with st.expander(meta_header, expanded=False):
            try:
                if brand:
                    meta_camp = _cached_meta_campaigns(brand, start_iso, end_iso)
                    camps = [(brand, meta_camp)]
                else:
                    # 전체 — 똑똑연구소 + 롤라루 합산
                    ddok_c = _cached_meta_campaigns("똑똑연구소", start_iso, end_iso)
                    rolla_c = _cached_meta_campaigns("롤라루", start_iso, end_iso)
                    parts = []
                    if ddok_c is not None and not ddok_c.empty:
                        ddok_c = ddok_c.copy()
                        if "brand" not in ddok_c.columns:
                            ddok_c["brand"] = "똑똑연구소"
                        parts.append(ddok_c)
                    if rolla_c is not None and not rolla_c.empty:
                        rolla_c = rolla_c.copy()
                        if "brand" not in rolla_c.columns:
                            rolla_c["brand"] = "롤라루"
                        parts.append(rolla_c)
                    combined = pd.concat(parts, ignore_index=True) if parts else None
                    camps = [("전체", combined)]

                for _tag, meta_camp in camps:
                    if meta_camp is None:
                        st.warning(f"Meta {_tag} API 클라이언트 로드 실패 (자격증명 확인)")
                        continue
                    if meta_camp.empty:
                        st.info(f"Meta {_tag} 캠페인 데이터 없음.")
                        continue
                    _render_campaign_table(
                        meta_camp, target_meta,
                        key=f"camp_meta_{_tag}_{start_iso}",
                    )
                    st.markdown("")
                    _render_campaign_diagnosis(
                        meta_camp, target_meta, "자사몰",
                    )
            except Exception as e:
                st.error(f"Meta 캠페인 조회 실패: {type(e).__name__}: {e}")

        # ----- 쿠팡 광고 (CSV 업로드 기반) -----
        coupang_visible = brand in ("똑똑연구소", "롤라루") or brand is None
        if coupang_visible:
            coupang_header = (
                f"📣 쿠팡 광고 ({brand}) 캠페인 — CSV 업로드"
                if brand else "📣 쿠팡 광고 (전체) 캠페인 — CSV 업로드"
            )
            with st.expander(coupang_header, expanded=False):
                try:
                    coupang_camp = _cached_coupang_campaigns(start_iso, end_iso)
                    if coupang_camp is None:
                        st.info(
                            "📥 **쿠팡 광고 데이터 없음**  \n"
                            "쿠팡은 공식 광고 Open API 가 없어 수동 CSV 업로드로 집계합니다.  \n"
                            "`data/coupang_ads_upload/` 폴더에 광고센터 CSV 를 드롭 후 "
                            "`sync_coupang_ads_csv.py` 실행 (sync_all.bat 에 자동 포함)."
                        )
                    else:
                        if brand:
                            filt_camp = coupang_camp[coupang_camp["brand"] == brand]
                        else:
                            # 전체 — 분류된 브랜드만 포함 (공통 제외해도 되고 포함해도 됨)
                            filt_camp = coupang_camp
                        if filt_camp.empty:
                            st.info(f"{brand_label} 소속 쿠팡 캠페인 없음.")
                        else:
                            _render_campaign_table(
                                filt_camp, target_coupang,
                                key=f"camp_coupang_{brand_label}_{start_iso}",
                            )
                            st.markdown("")
                            _render_campaign_diagnosis(
                                filt_camp, target_coupang, "쿠팡",
                            )
                except Exception as e:
                    st.error(f"쿠팡 캠페인 조회 실패: {type(e).__name__}: {e}")

    # ---------- 🔗 캠페인 → 제품 연결 분석 (가중 상관) ----------
    if brand in ("똑똑연구소", "롤라루", "루티니스트"):
        _render_campaign_product_correlation(brand, start, end)


def _render_campaign_product_correlation(
    brand: str, start: pd.Timestamp, end: pd.Timestamp,
) -> None:
    """광고 캠페인 × 제품 일별 가중 상관계수 — 숨은 attribution 힌트."""
    from utils.forecasting import campaign_product_correlation
    from utils.data import load_orders, load_coupang_inbound
    from utils.products import classify_orders, BRAND_AD_STORES

    st.divider()
    st.markdown(
        f"#### 🔗 {brand} 캠페인 ↔ 제품 연결 분석 "
        f"<span style='font-size:0.72rem; color:{TEXT_MUTED}; font-weight:400;'>"
        f"(가중 Pearson 상관 · 최근 관측 가중)</span>",
        unsafe_allow_html=True,
    )
    st.caption(
        "일별 광고비와 제품 매출의 상관관계를 가중 통계로 측정. "
        "양의 상관 = 해당 캠페인 지출이 그 제품 매출 증가와 동조 "
        "(인과 증명 아님, attribution 힌트)."
    )

    # 광고 데이터 — 쿠팡/네이버/Meta 캠페인별 일별 spend
    # ads.csv 는 store × channel 단위이므로 캠페인 상세는 precompute parquet 에서
    try:
        from utils.precomputed import load_precomputed_parquet
        naver_daily = load_precomputed_parquet("naver_campaigns_daily.parquet")
        coupang_daily = load_precomputed_parquet("coupang_campaigns_daily.parquet")

        # Meta 는 brand 별 파일
        meta_daily = load_precomputed_parquet(
            f"meta_campaigns_{brand}_daily.parquet"
        )

        frames = []
        if not naver_daily.empty:
            n = naver_daily[naver_daily["brand"] == brand][
                ["date", "campaign_name", "spend"]
            ].copy()
            n["campaign_name"] = n["campaign_name"].astype(str) + " (네이버)"
            frames.append(n)
        if not coupang_daily.empty:
            c = coupang_daily[coupang_daily["brand"] == brand][
                ["date", "campaign_name", "spend"]
            ].copy()
            c["campaign_name"] = c["campaign_name"].astype(str) + " (쿠팡)"
            frames.append(c)
        if not meta_daily.empty:
            m = meta_daily[["date", "campaign_name", "spend"]].copy()
            m["campaign_name"] = m["campaign_name"].astype(str) + " (Meta)"
            frames.append(m)

        if not frames:
            st.info(
                "📥 캠페인 일자 단위 프리컴퓨트 parquet 없음. "
                "`sync_all.bat` 실행 후 precompute 가 생성하면 반영됩니다."
            )
            return
        ads_daily = pd.concat(frames, ignore_index=True)
        ads_daily["date"] = pd.to_datetime(ads_daily["date"])
        # 기간 필터 — 최근 90일 (히스토리 풍부할수록 좋음)
        cutoff = pd.Timestamp(end) - pd.Timedelta(days=90)
        ads_daily = ads_daily[ads_daily["date"] >= cutoff]

        # 주문 데이터 (brand 의 umbrella)
        orders = classify_orders(load_orders())
        inbound = load_coupang_inbound()
        if not inbound.empty:
            inbound_cls = classify_orders(inbound)
            orders = pd.concat([orders, inbound_cls], ignore_index=True)
        brand_orders = orders[orders["umbrella"] == brand].copy()
        brand_orders["date"] = pd.to_datetime(brand_orders["date"])
        brand_orders = brand_orders[brand_orders["date"] >= cutoff]

        if brand_orders.empty or ads_daily.empty:
            st.info("선택 브랜드의 최근 90일 광고/주문 데이터 부족.")
            return

        corr_df = campaign_product_correlation(
            ads_daily, brand_orders[["date", "product", "revenue"]],
            today=pd.Timestamp(end), half_life_days=14.0, min_days=10,
        )

        if corr_df.empty:
            st.info("상관 분석 결과 없음 (공통 관측일 부족).")
            return

        # Top correlations + worst (negative)
        pos = corr_df[corr_df["corr"] >= 0.3].head(8)
        neg = corr_df[corr_df["corr"] <= -0.3].head(5)

        cc1, cc2 = st.columns(2)
        with cc1:
            st.markdown("**✨ 양의 상관 TOP (광고 지출 ↑ → 제품 매출 ↑)**")
            if pos.empty:
                st.caption(":grey[상관 ≥ 0.3 인 쌍 없음. 광고-매출 연동 약함.]")
            else:
                pos_display = pos.rename(columns={
                    "campaign_name": "캠페인",
                    "product": "제품",
                    "corr": "상관",
                    "joint_days": "공통일수",
                    "total_spend": "광고비 합계",
                    "total_rev": "매출 합계",
                })
                st.dataframe(
                    pos_display[["캠페인", "제품", "상관", "공통일수", "광고비 합계", "매출 합계"]],
                    width="stretch", hide_index=True,
                    column_config={
                        "캠페인": st.column_config.TextColumn("캠페인", width="medium"),
                        "제품": st.column_config.TextColumn("제품", width="medium"),
                        "상관": st.column_config.ProgressColumn(
                            "상관", format="%.2f", min_value=0, max_value=1,
                        ),
                        "광고비 합계": st.column_config.NumberColumn("광고비", format="%d원"),
                        "매출 합계": st.column_config.NumberColumn("매출", format="%d원"),
                    },
                    height=min(350, 50 + len(pos_display) * 35),
                )
        with cc2:
            st.markdown("**⚠️ 음의 상관 (광고 지출 ↑ → 제품 매출 ↓)**")
            if neg.empty:
                st.caption(":grey[음의 상관 ≤ -0.3 인 쌍 없음 (정상).]")
            else:
                neg_display = neg.rename(columns={
                    "campaign_name": "캠페인",
                    "product": "제품",
                    "corr": "상관",
                    "joint_days": "공통일수",
                })
                st.dataframe(
                    neg_display[["캠페인", "제품", "상관", "공통일수"]],
                    width="stretch", hide_index=True,
                    column_config={
                        "상관": st.column_config.NumberColumn("상관", format="%.2f"),
                    },
                )
                st.caption(
                    ":grey[음의 상관은 광고와 무관하거나 카니발라이제이션 "
                    "(같은 카테고리 내 대체) 의심 — 검증 필요]"
                )

        st.caption(
            f"📊 총 {len(corr_df)}쌍 분석 · 가중 Pearson (EWMA half-life 14일) · "
            f"최소 공통 관측 10일 · 광고비 1만원 미만 캠페인 제외"
        )
    except Exception as e:
        st.warning(f"캠페인-제품 상관 분석 실패: {type(e).__name__}: {e}")


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
    st.caption("전체 채널 광고 합산 (네이버 · Meta · 쿠팡). 캠페인별 상세는 아래 expander 에서.")
    render_ad_overview(ads, start_date, pd.Timestamp(end_date), ads, brand=None)

with tab_ddok:
    render_brand_banner(
        "똑똑연구소",
        "네이버 검색광고 (김똑똑/떡뻥) · Meta 광고 · 쿠팡 광고 (CSV 업로드)",
    )
    ddok_ads = filter_ads_by_brand(ads, "똑똑연구소")
    render_ad_overview(
        ddok_ads, start_date, pd.Timestamp(end_date), ddok_ads,
        brand="똑똑연구소",
    )

with tab_rolla:
    render_brand_banner(
        "롤라루",
        "Meta 광고 · 네이버 검색광고 (롤라루) · 쿠팡 AI 광고 (CSV 업로드)",
    )
    rolla_ads = filter_ads_by_brand(ads, "롤라루")
    render_ad_overview(
        rolla_ads, start_date, pd.Timestamp(end_date), rolla_ads,
        brand="롤라루",
    )

with tab_ruti:
    render_brand_banner(
        "루티니스트",
        "Meta 광고 (Routinist 자사몰) — API 연동 · 매일 10시 sync",
    )
    ruti_ads = filter_ads_by_brand(ads, "루티니스트")
    render_ad_overview(
        ruti_ads, start_date, pd.Timestamp(end_date), ruti_ads,
        brand="루티니스트",
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
