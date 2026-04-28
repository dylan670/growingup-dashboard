"""제품 × 브랜드 통합 분석 — 브랜드 탭(전체/똑똑연구소/롤라루) × 제품별 상세.

옵션별 매출 상세 분석 추가 (2026-04-28).
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from api.naver_searchad import load_client_from_env
from utils.data import load_orders, load_coupang_inbound
from utils.products import (
    classify_orders,
    aggregate_by_umbrella,
    attribute_naver_ad_spend,
    BRAND_RULES,
    UMBRELLA_BRANDS,
    store_display_name,
)
from utils.product_images import (
    load_image_cache,
    build_store_scoped_lookup,
    refresh_naver_image_cache,
)
from utils.ui import (
    setup_page, render_brand_banner,
    format_won_compact, kpi_card,
    render_period_picker, render_empty_state, render_status_pill,
    METRIC_COLORS, CHANNEL_COLORS, channel_color,
    TEXT_MAIN, TEXT_MUTED, TEXT_FAINT,
    BORDER_SUBTLE, BG_SUBTLE,
)


setup_page(
    page_title="제품 분석",
    page_icon="📦",
    header_title="📦 제품 분석",
    header_subtitle="제품 브랜드 단위 분석",
)

# 캐시 버전 — 제품명 정규화 규칙 바뀌면 bump 해서 기존 캐시 강제 무효화
_ORDERS_CACHE_VER = "v10-force-refresh"


@st.cache_data(ttl=300, show_spinner="주문 + 쿠팡 벤더 발주 데이터 로드 중...")
def _cached_orders_classified(
    _cache_ver: str = _ORDERS_CACHE_VER,
) -> pd.DataFrame:
    """주문 데이터 + 쿠팡 벤더 발주 병합 + 브랜드 분류 (5분 캐시).

    - orders.csv: 실 소비자 판매 (매출 분석/CRM 공유)
    - coupang_inbound.csv: 쿠팡 벤더 발주 (제품 분석 전용)
    - 제품명 정규화 자동 적용 (utils/products.py PRODUCT_NAME_RULES)
    """
    orders_df = load_orders()
    inbound_df = load_coupang_inbound()
    if inbound_df.empty:
        combined = orders_df
    else:
        combined = pd.concat([orders_df, inbound_df], ignore_index=True)
    return classify_orders(combined)


orders = load_orders()  # 날짜 range 계산용 (빠름)
_inbound_raw = load_coupang_inbound()
from datetime import date as _today_func
today_real = _today_func.today()
# 날짜 range 는 orders + inbound 합쳐서 산정
if not _inbound_raw.empty:
    _all_dates = pd.concat([orders["date"], _inbound_raw["date"]]) if not orders.empty else _inbound_raw["date"]
else:
    _all_dates = orders["date"] if not orders.empty else pd.Series([], dtype="datetime64[ns]")
orders_max = _all_dates.max().date() if not _all_dates.empty else today_real
orders_min = _all_dates.min().date() if not _all_dates.empty else _today_func(today_real.year - 1, 1, 1)

# 입고 예정일(미래)이 포함되면 date_input max_value(today_real)와 충돌 →
# orders_max 를 오늘 이하로 제한
if orders_max > today_real:
    orders_max = today_real
# orders_min 은 오늘 이후일 수 없음 (방어)
if orders_min > today_real:
    orders_min = _today_func(today_real.year - 1, 1, 1)

# ==========================================================
# 기간 선택 (통합 picker — 전 페이지 동일 UI)
# ==========================================================
_pp = render_period_picker(
    max_date=orders_max, min_date=orders_min,
    key_prefix="products", default_option="최근 30일",
)
period = _pp["period"]
start_date = _pp["start_date"]
end_date = _pp["end_date"].date()
days = _pp["days"]


# ==========================================================
# 기간 필터 + 브랜드 분류 (분류는 전체 데이터에 한 번만 — 캐시됨)
# ==========================================================
_orders_classified = _cached_orders_classified()
o_filt_all = _orders_classified[
    (_orders_classified["date"] >= start_date)
    & (_orders_classified["date"] <= pd.Timestamp(end_date))
].copy()


# ==========================================================
# 네이버 광고비 조회 (캐시) — 전체 기간 1회만
# ==========================================================
# 네이버 API 클라이언트는 프리컴퓨트 미존재 시에만 fallback 용으로 로드
# (로드 자체는 빠르지만 API 호출이 느림 → 프리컴퓨트 있으면 호출 안 함)
ad_spend_by_umbrella: dict[str, int] = {}
ad_spend_debug: pd.DataFrame | None = None


@st.cache_resource
def _cached_naver_client():
    """네이버 API 클라이언트 — 세션 1회만 초기화."""
    try:
        return load_client_from_env()
    except Exception:
        return None


naver_client = _cached_naver_client()


@st.cache_data(ttl=600, show_spinner="데이터 불러오는 중…")
def _get_brand_ad_spend(_client, since_iso: str, until_iso: str):
    """네이버 광고비 브랜드별 — 프리컴퓨트 parquet 우선, 없으면 API."""
    from datetime import date as _date

    # 1) 프리컴퓨트 parquet 우선 (naver_campaigns_daily.parquet 활용)
    try:
        from utils.precomputed import load_precomputed_parquet
        daily = load_precomputed_parquet("naver_campaigns_daily.parquet")
        if not daily.empty:
            daily = daily.copy()
            daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
            daily = daily.dropna(subset=["date"])
            since_ts = pd.Timestamp(since_iso)
            until_ts = pd.Timestamp(until_iso)
            sliced = daily[
                (daily["date"] >= since_ts) & (daily["date"] <= until_ts)
            ]
            if not sliced.empty and "brand" in sliced.columns:
                # brand 는 똑똑연구소 / 롤라루 / 공통
                by_umb = (
                    sliced.groupby("brand")["spend"].sum()
                    .astype(int).to_dict()
                )
                debug = (
                    sliced.groupby(["campaign_name", "brand"])
                    .agg(비용=("spend", "sum"), 매출=("revenue", "sum"))
                    .reset_index()
                    .rename(columns={"campaign_name": "이름", "brand": "umbrella"})
                    .sort_values("비용", ascending=False)
                )
                debug["brand"] = debug["umbrella"]
                debug["ROAS(%)"] = (
                    debug["매출"] / debug["비용"].replace(0, pd.NA) * 100
                ).round(0).fillna(0).astype(int)
                return by_umb, debug
    except Exception:
        pass

    # 2) Fallback — API (느림: 수 분)
    from utils.naver_insights import fetch_breakdown
    df = fetch_breakdown(
        _client, "adgroup",
        _date.fromisoformat(since_iso),
        _date.fromisoformat(until_iso),
    )
    if df.empty:
        return {}, pd.DataFrame()
    df = attribute_naver_ad_spend(df)
    by_umb = df.groupby("umbrella")["비용"].sum().astype(int).to_dict()
    debug = df[["이름", "brand", "umbrella", "비용", "매출", "ROAS(%)"]] \
        .sort_values("비용", ascending=False)
    return by_umb, debug


if naver_client:
    try:
        ad_spend_by_umbrella, ad_spend_debug = _get_brand_ad_spend(
            naver_client, str(start_date.date()), str(end_date),
        )
    except Exception as e:
        st.warning(f"네이버 광고비 조회 실패 (브랜드별 할당 스킵): {e}")
else:
    # 프리컴퓨트만으로도 시도 — API 클라이언트 불필요
    try:
        ad_spend_by_umbrella, ad_spend_debug = _get_brand_ad_spend(
            None, str(start_date.date()), str(end_date),
        )
    except Exception:
        st.info("네이버 광고비 데이터 없음 → 광고비 할당 없이 매출만 표시.")


# ==========================================================
# 이미지 캐시 + fuzzy 매칭 결과 (세션 단위 캐시, 10분 TTL)
# ==========================================================
image_cache = load_image_cache()


@st.cache_data(ttl=600, show_spinner="상품 이미지 매칭 중...")
def _cached_full_image_lookup(
    orders_fingerprint: tuple,
    cache_fingerprint: tuple,
) -> dict:
    """전체 주문 × 이미지 캐시 fuzzy 매칭 결과를 한 번만 계산.

    Args:
        orders_fingerprint: (행수, store 리스트, product 리스트 hash)
        cache_fingerprint: (행수, name 리스트 hash)
        → Streamlit 캐시 키 안정화용 (DataFrame 직접 해싱 느림)
    """
    # 실제 계산용 orders/cache 는 전역에서 로드
    all_orders_inner = classify_orders(load_orders())
    return build_store_scoped_lookup(
        all_orders_inner, image_cache, min_ratio=0.5,
    )


# fingerprint 계산 (DataFrame 해싱 없이 빠르게 캐시 키 생성)
_all_orders_for_fp = load_orders()
_orders_fp = (
    len(_all_orders_for_fp),
    tuple(sorted(_all_orders_for_fp["store"].dropna().unique().tolist()))
    if "store" in _all_orders_for_fp.columns else (),
)
_cache_fp = (
    len(image_cache),
    hash(tuple(image_cache["name"].dropna().astype(str).tolist()[:100]))
    if "name" in image_cache.columns else 0,
)

# 한 번 호출 → 이후 브랜드 탭 4개 모두 이 lookup 재사용 (퍼지 매칭 재계산 X)
full_image_lookup = _cached_full_image_lookup(_orders_fp, _cache_fp)


# ==========================================================
# 제품 상세 모달 (딥 다이브) — st.dialog 기반
# ==========================================================
@st.dialog("📦 제품 상세 딥 다이브", width="large")
def show_product_detail(
    product_name: str,
    orders_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    image_url: str | None = None,
):
    """제품 하나의 모든 정보 — 이미지, 판매 추이, 채널 분포, 요약 지표."""
    prod_df = orders_df[orders_df["product"] == product_name].copy()
    if prod_df.empty:
        st.info("선택 기간 내 해당 제품의 판매 기록 없음.")
        return

    prod_df["date"] = pd.to_datetime(prod_df["date"])
    period_df = prod_df[
        (prod_df["date"] >= start) & (prod_df["date"] <= end)
    ]

    # 헤더 (이미지 + 이름)
    hc1, hc2 = st.columns([1, 3])
    with hc1:
        if image_url and isinstance(image_url, str) and image_url.startswith("http"):
            st.markdown(
                f"<div style='width:100%; aspect-ratio:1/1; background:#f8fafc; "
                f"border-radius:14px; overflow:hidden;'>"
                f"<img src='{image_url}' style='width:100%; height:100%; object-fit:cover;' />"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='width:100%; aspect-ratio:1/1; "
                f"background:linear-gradient(135deg, #e2e8f0, #f1f5f9); "
                f"border-radius:14px; display:flex; align-items:center; "
                f"justify-content:center; color:#94a3b8; font-size:2rem;'>📦</div>",
                unsafe_allow_html=True,
            )
    with hc2:
        st.markdown(
            f"<div style='font-size:1.25rem; font-weight:700; color:{TEXT_MAIN}; "
            f"line-height:1.3; margin-bottom:6px;'>{product_name}</div>",
            unsafe_allow_html=True,
        )
        umbrella = period_df["umbrella"].iloc[0] if "umbrella" in period_df and not period_df.empty else "-"
        brand = period_df["brand"].iloc[0] if "brand" in period_df and not period_df.empty else "-"
        st.caption(f"{umbrella} · {brand}")
        # 기간 요약 KPI
        total_rev = int(period_df["revenue"].sum())
        total_qty = int(period_df["quantity"].sum()) if "quantity" in period_df else 0
        total_ord = len(period_df)
        uniq_cust = period_df["customer_id"].nunique() if "customer_id" in period_df else 0
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("매출", f"{total_rev:,}원")
        k2.metric("수량", f"{total_qty:,}개")
        k3.metric("주문", f"{total_ord:,}건")
        k4.metric("고객", f"{uniq_cust:,}명")

    st.divider()

    # 채널별 분포
    if "판매 채널" in period_df.columns:
        ch_col = "판매 채널"
    else:
        ch_col = "store"
    ch_dist = (
        period_df.groupby(ch_col).agg(
            rev=("revenue", "sum"),
            ord=("order_id", "count"),
        ).reset_index().sort_values("rev", ascending=False)
    )

    dc1, dc2 = st.columns([1, 1])
    with dc1:
        st.markdown(
            f"<div style='font-weight:700; color:{TEXT_MAIN}; "
            f"font-size:0.95rem; margin-bottom:8px;'>🛒 채널별 매출 비중</div>",
            unsafe_allow_html=True,
        )
        if not ch_dist.empty:
            total_r = ch_dist["rev"].sum()
            fig_pie = go.Figure(go.Pie(
                labels=ch_dist[ch_col].tolist(),
                values=ch_dist["rev"].tolist(),
                hole=0.55,
                marker=dict(colors=[
                    channel_color(c) for c in ch_dist[ch_col]
                ]),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>매출 %{value:,}원<br>%{percent}<extra></extra>",
            ))
            fig_pie.update_layout(
                height=260, margin=dict(l=10, r=10, t=10, b=10),
                showlegend=False,
                annotations=[dict(
                    text=f"<b>총매출</b><br>{total_r:,}",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=12, color=TEXT_MAIN),
                )],
            )
            st.plotly_chart(fig_pie, width="stretch",
                            key=f"dlg_pie_{product_name}")

    with dc2:
        st.markdown(
            f"<div style='font-weight:700; color:{TEXT_MAIN}; "
            f"font-size:0.95rem; margin-bottom:8px;'>📈 일별 판매 추이</div>",
            unsafe_allow_html=True,
        )
        daily = (
            period_df.groupby(period_df["date"].dt.date)
            .agg(qty=("quantity", "sum"), rev=("revenue", "sum"))
            .reset_index()
            .rename(columns={"date": "date"})
        )
        daily["date"] = pd.to_datetime(daily["date"])
        full_range = pd.date_range(start=start, end=end, freq="D")
        daily = daily.set_index("date").reindex(full_range, fill_value=0).reset_index()
        daily = daily.rename(columns={"index": "date"})
        fig_line = make_subplots(specs=[[{"secondary_y": True}]])
        fig_line.add_trace(go.Bar(
            x=daily["date"], y=daily["qty"],
            name="수량",
            marker_color=METRIC_COLORS["clicks"], opacity=0.7,
        ), secondary_y=False)
        fig_line.add_trace(go.Scatter(
            x=daily["date"], y=daily["rev"],
            name="매출", mode="lines+markers",
            line=dict(color=METRIC_COLORS["revenue"], width=2.5),
        ), secondary_y=True)
        fig_line.update_layout(
            height=260, margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False, plot_bgcolor="white",
            xaxis=dict(tickformat="%m/%d"),
        )
        st.plotly_chart(fig_line, width="stretch",
                        key=f"dlg_line_{product_name}")

    st.divider()
    # 채널별 상세 표
    st.markdown(
        f"<div style='font-weight:700; color:{TEXT_MAIN}; "
        f"font-size:0.95rem; margin-bottom:6px;'>📋 채널별 상세</div>",
        unsafe_allow_html=True,
    )
    if not ch_dist.empty:
        ch_dist["비중(%)"] = (ch_dist["rev"] / ch_dist["rev"].sum() * 100).round(1)
        ch_dist_display = ch_dist.rename(columns={
            ch_col: "채널", "rev": "매출", "ord": "주문",
        })
        st.dataframe(
            ch_dist_display,
            width="stretch", hide_index=True,
            column_config={
                "매출": st.column_config.NumberColumn("매출", format="%d원"),
                "비중(%)": st.column_config.ProgressColumn(
                    "비중(%)", format="%.1f%%", min_value=0, max_value=100,
                ),
            },
        )

    # ---------- 옵션별 상세 (색상/사이즈 등 SKU 단위) ----------
    st.divider()
    st.markdown(
        f"<div style='font-weight:700; color:{TEXT_MAIN}; "
        f"font-size:0.95rem; margin-bottom:6px;'>🎨 옵션별 판매 상세</div>",
        unsafe_allow_html=True,
    )

    if "option" not in period_df.columns:
        st.caption(
            ":grey[옵션 데이터 없음 — 다음 sync 부터 자동 수집됩니다.]"
        )
    else:
        opt_df = period_df.copy()
        opt_df["option"] = opt_df["option"].fillna("").astype(str).str.strip()
        with_opt = opt_df[opt_df["option"] != ""]

        if with_opt.empty:
            st.caption(
                ":grey[이 제품의 옵션 데이터가 아직 수집되지 않았습니다. "
                "Cafe24 / 네이버 / 쿠팡 API 가 다음 sync(매일 10시 또는 사이드바 "
                "🔄 버튼) 부터 옵션 정보를 자동 추출합니다.]"
            )
        else:
            opt_agg = (
                with_opt.groupby("option")
                .agg(
                    매출=("revenue", "sum"),
                    수량=("quantity", "sum"),
                    주문=("order_id", "count"),
                )
                .reset_index()
                .sort_values("매출", ascending=False)
                .rename(columns={"option": "옵션"})
            )
            opt_agg["비중(%)"] = (
                opt_agg["매출"] / opt_agg["매출"].sum() * 100
            ).round(1)

            # 채널별로도 옵션이 다르게 잡힐 수 있으므로, 채널 정보 같이 표시
            opt_by_ch = (
                with_opt.groupby(["option", ch_col])
                .agg(매출=("revenue", "sum"), 수량=("quantity", "sum"))
                .reset_index()
            )
            # 옵션마다 가장 큰 채널 표시
            top_ch_per_opt = (
                opt_by_ch.sort_values("매출", ascending=False)
                .drop_duplicates(subset=["option"], keep="first")
                [["option", ch_col]]
                .rename(columns={"option": "옵션", ch_col: "주력 채널"})
            )
            opt_agg = opt_agg.merge(top_ch_per_opt, on="옵션", how="left")

            # 요약 — 총 옵션 개수 + 상위 3개 비중
            total_opts = len(opt_agg)
            top3_pct = opt_agg.head(3)["비중(%)"].sum()
            st.caption(
                f"📊 총 **{total_opts}개 옵션** · "
                f"상위 3개 옵션이 매출의 **{top3_pct:.1f}%** 차지"
            )

            st.dataframe(
                opt_agg[[
                    "옵션", "주력 채널", "매출", "수량", "주문", "비중(%)",
                ]],
                width="stretch", hide_index=True,
                column_config={
                    "옵션": st.column_config.TextColumn("옵션", width="large"),
                    "주력 채널": st.column_config.TextColumn("주력 채널", width="medium"),
                    "매출": st.column_config.NumberColumn("매출", format="%d원"),
                    "수량": st.column_config.NumberColumn("수량", format="%d개"),
                    "주문": st.column_config.NumberColumn("주문", format="%d건"),
                    "비중(%)": st.column_config.ProgressColumn(
                        "비중", format="%.1f%%", min_value=0, max_value=100,
                    ),
                },
                height=min(360, 50 + len(opt_agg) * 36),
            )

            # 옵션 매출 막대 차트 (상위 10개)
            if len(opt_agg) > 1:
                top_opts = opt_agg.head(10)
                fig_opt = go.Figure(go.Bar(
                    x=top_opts["매출"],
                    y=top_opts["옵션"],
                    orientation="h",
                    marker=dict(color=METRIC_COLORS["revenue"], opacity=0.85),
                    text=[f"{v:,}원" for v in top_opts["매출"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>매출 %{x:,}원<extra></extra>",
                ))
                fig_opt.update_layout(
                    height=max(220, 35 * len(top_opts) + 60),
                    margin=dict(l=10, r=80, t=10, b=10),
                    showlegend=False,
                    plot_bgcolor="white",
                    xaxis=dict(tickformat=",", showgrid=True, gridcolor="#f1f5f9"),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(
                    fig_opt, width="stretch",
                    key=f"dlg_opt_{product_name}",
                )


# ==========================================================
# 제품별 일별 판매 추이 차트 (수량 막대 + 매출 선)
# ==========================================================
def _render_daily_product_chart(
    product_name: str,
    o_filt: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    key: str,
) -> None:
    """선택 제품의 일별 판매량·매출 추이.

    Args:
        product_name: 제품명 (정확 일치)
        o_filt: 기간 필터된 주문 DF (date, product, quantity, revenue, channel)
        start, end: 기간 (x축 범위 고정)
        key: plotly_chart key
    """
    prod_df = o_filt[o_filt["product"] == product_name].copy()
    if prod_df.empty:
        st.caption(":grey[선택 기간 내 판매 없음]")
        return

    prod_df["date"] = pd.to_datetime(prod_df["date"])
    daily = (
        prod_df.groupby(prod_df["date"].dt.date)
        .agg(quantity=("quantity", "sum"),
             revenue=("revenue", "sum"),
             orders=("order_id", "count"))
        .reset_index()
        .rename(columns={"date": "date"})
    )
    daily["date"] = pd.to_datetime(daily["date"])

    # 기간 전체 범위로 reindex (판매 없는 날은 0)
    full_range = pd.date_range(start=start, end=end, freq="D")
    daily = daily.set_index("date").reindex(full_range, fill_value=0).reset_index()
    daily = daily.rename(columns={"index": "date"})

    # 요약 지표
    total_qty = int(daily["quantity"].sum())
    total_rev = int(daily["revenue"].sum())
    total_orders = int(daily["orders"].sum())
    sold_days = int((daily["quantity"] > 0).sum())
    avg_daily_qty = total_qty / len(daily) if len(daily) else 0

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("총 판매량", f"{total_qty:,}개")
    sc2.metric("총 매출", f"{total_rev:,}원")
    sc3.metric("총 주문", f"{total_orders:,}건")
    sc4.metric(
        "판매 발생일",
        f"{sold_days}/{len(daily)}일",
        delta=f"일평균 {avg_daily_qty:.1f}개",
    )

    # 이중축 차트 — 통일 METRIC_COLORS 팔레트
    #   수량(막대) = clicks 색 (하늘) · 매출(선) = revenue 색 (파랑)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=daily["date"], y=daily["quantity"],
            name="판매 수량",
            marker_color=METRIC_COLORS["clicks"],
            opacity=0.75,
            hovertemplate="%{x|%m/%d}<br>%{y:,}개<extra></extra>",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=daily["date"], y=daily["revenue"],
            name="매출",
            mode="lines+markers",
            line=dict(color=METRIC_COLORS["revenue"], width=2.5),
            marker=dict(size=6, color=METRIC_COLORS["revenue"]),
            hovertemplate="%{x|%m/%d}<br>%{y:,.0f}원<extra></extra>",
        ),
        secondary_y=True,
    )
    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
        hovermode="x unified",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, tickformat="%m/%d",
                   range=[start, end]),
    )
    fig.update_yaxes(
        title_text="판매 수량 (개)", secondary_y=False,
        showgrid=True, gridcolor="#f1f5f9",
        rangemode="tozero",
    )
    fig.update_yaxes(
        title_text="매출 (원)", secondary_y=True,
        showgrid=False, tickformat=",",
        rangemode="tozero",
    )
    st.plotly_chart(fig, width="stretch", key=key)


# ==========================================================
# 렌더링 함수
# ==========================================================
def render_product_view(
    o_filt: pd.DataFrame,
    brand: str | None = None,
):
    """브랜드별 제품 분석 렌더링.

    brand=None → 전체 / brand='똑똑연구소' or '롤라루' → umbrella 필터.
    """
    brand_label = brand if brand else "전체"

    if brand:
        o_filt = o_filt[o_filt["umbrella"] == brand].copy()

    # ---------- 상단 KPI ----------
    total_rev = int(o_filt["revenue"].sum())
    n_products = o_filt["product"].nunique()
    n_orders = len(o_filt)
    avg_rev_per_order = int(total_rev / n_orders) if n_orders else 0

    st.markdown(f"#### 📈 {brand_label} 제품 요약")
    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(
        kpi_card(
            "총 매출",
            format_won_compact(total_rev),
            sub=f"{total_rev:,}원",
            value_color="#2563eb",
        ),
        unsafe_allow_html=True,
    )
    k2.markdown(
        kpi_card(
            "상품 종류",
            f"{n_products}종",
            sub="판매 중인 SKU",
        ),
        unsafe_allow_html=True,
    )
    k3.markdown(
        kpi_card(
            "총 주문",
            f"{n_orders:,}건",
            sub=f"상품 1종당 {(n_orders/n_products):.0f}건" if n_products else "—",
        ),
        unsafe_allow_html=True,
    )
    k4.markdown(
        kpi_card(
            "주문당 매출",
            format_won_compact(avg_rev_per_order),
            sub=f"{avg_rev_per_order:,}원",
        ),
        unsafe_allow_html=True,
    )

    if o_filt.empty:
        render_empty_state(
            title=f"{brand_label}: 해당 기간 주문 데이터 없음",
            description=(
                f"선택된 기간 내에 {brand_label} 주문이 없습니다. "
                f"상단의 기간을 넓혀보거나 쿠팡 판매/벤더 발주 CSV 가 "
                f"업로드되어 있는지 확인해보세요."
            ),
            icon="📭",
            action_label="기간 필터 조정 · CSV 업로드 확인",
        )
        return

    # ---------- 운영 브랜드 카드 (전체 탭에서만, 또는 단일 브랜드 요약) ----------
    st.divider()
    umbrella_agg = aggregate_by_umbrella(o_filt)
    if brand is None:
        st.markdown("#### 🏷️ 운영 브랜드별 성과")
    else:
        st.markdown(f"#### 🏷️ {brand_label} 브랜드 요약")

    for _, row in umbrella_agg.iterrows():
        umbrella = row["umbrella"]
        revenue = int(row["revenue"])
        orders_n = int(row["orders"])
        customers = int(row["customers"])
        products_n = int(row["products"])

        with st.container(border=True):
            # 상단: 브랜드 정보 + 매출/주문 (네이버 광고비/ROAS 제거)
            cc = st.columns([2, 3, 2])
            cc[0].markdown(f"### {umbrella}")
            cc[0].caption(f"{products_n}종 상품 · {customers}명 고객")
            cc[1].metric("매출", f"{revenue:,}원")
            cc[2].metric("주문", f"{orders_n:,}건")

            # 하단: 채널별 매출 비중 (전폭 사용, 가로 나열)
            ch_rev = o_filt[o_filt["umbrella"] == umbrella].groupby("channel")["revenue"].sum()
            if len(ch_rev) > 0:
                total = ch_rev.sum()
                parts = []
                for ch, r in ch_rev.sort_values(ascending=False).items():
                    pct = r / total * 100 if total else 0
                    parts.append(f"**{ch}** {int(r):,}원 ({pct:.0f}%)")
                st.caption("📊 채널별 매출 비중 · " + "   |   ".join(parts))

    # ---------- 제품 × 판매채널 상세 ----------
    st.divider()
    st.markdown(f"#### 📦 {brand_label} 제품별 상세")

    o_filt = o_filt.copy()
    # "판매 채널" = 사용자 용어의 간결한 스토어명 (자사몰 / 네이버 스마트스토어 / 쿠팡)
    o_filt["판매 채널"] = o_filt["store"].apply(
        lambda s: store_display_name(s, brand_context=brand)
    )

    prod_ch_agg_full = o_filt.groupby(
        ["umbrella", "brand", "판매 채널", "store", "channel", "product"],
        dropna=False,
    ).agg(
        orders=("order_id", "count"),
        quantity=("quantity", "sum"),
        revenue=("revenue", "sum"),
        customers=("customer_id", "nunique"),
    ).reset_index().sort_values("revenue", ascending=False)

    if prod_ch_agg_full.empty:
        render_empty_state(
            title="제품별 상세 데이터 없음",
            description="해당 기간의 제품 단위 집계 결과가 비어있습니다.",
            icon="📦",
        )
        return

    # ---------- 🔍 검색 + 필터 ----------
    with st.container(border=False):
        fc1, fc2, fc3 = st.columns([2, 1.3, 1.3])
        with fc1:
            search_q = st.text_input(
                "🔍 제품명 검색",
                value="",
                placeholder="예: 오프너, 김똑똑, 떡뻥 — 한 단어 또는 공백 구분 여러 단어",
                key=f"prod_search_{brand_label}",
            )
        with fc2:
            channel_options = sorted(prod_ch_agg_full["판매 채널"].dropna().unique().tolist())
            selected_channels = st.multiselect(
                "🏪 판매 채널",
                options=channel_options,
                default=channel_options,
                key=f"prod_ch_filter_{brand_label}",
            )
        with fc3:
            rev_max = int(prod_ch_agg_full["revenue"].max())
            min_rev = st.number_input(
                "💰 최소 매출 (원)",
                min_value=0, max_value=rev_max,
                value=0, step=10000,
                key=f"prod_min_rev_{brand_label}",
                help="이 값 이상의 제품만 표시",
            )

    # 필터 적용
    prod_ch_agg = prod_ch_agg_full.copy()
    if search_q.strip():
        terms = [t.lower() for t in search_q.strip().split() if t]
        mask = prod_ch_agg["product"].astype(str).str.lower().apply(
            lambda s: all(t in s for t in terms)
        )
        prod_ch_agg = prod_ch_agg[mask]
    if selected_channels:
        prod_ch_agg = prod_ch_agg[prod_ch_agg["판매 채널"].isin(selected_channels)]
    if min_rev > 0:
        prod_ch_agg = prod_ch_agg[prod_ch_agg["revenue"] >= min_rev]

    if prod_ch_agg.empty:
        render_empty_state(
            title="필터 조건에 맞는 제품 없음",
            description=(
                f"검색어 '{search_q}' 및 설정된 필터에 매칭되는 제품이 없습니다. "
                f"검색어를 줄이거나 채널/최소 매출 필터를 완화해보세요."
            ),
            icon="🔍",
            action_label="검색어 지우기 · 모든 채널 선택",
        )
        return

    # 이미지 매칭 — 전역 lookup 재사용 (퍼지 매칭 재계산 X)
    if full_image_lookup:
        prod_ch_agg["image"] = prod_ch_agg.apply(
            lambda r: full_image_lookup.get((r["store"], r["product"])), axis=1,
        )
    else:
        prod_ch_agg["image"] = None
    prod_ch_agg["image_display"] = prod_ch_agg["image"].fillna("").astype(str)

    display = prod_ch_agg[[
        "image_display", "umbrella", "brand", "판매 채널", "product",
        "orders", "quantity", "revenue", "customers",
    ]].rename(columns={
        "image_display": "이미지",
        "umbrella": "운영 브랜드",
        "brand": "제품 라인",
        "product": "제품명",
        "orders": "주문",
        "quantity": "수량",
        "revenue": "매출",
        "customers": "고객수",
    })

    matched_n = int(prod_ch_agg["image"].notna().sum())
    total_rows = len(prod_ch_agg)
    coupang_rows = int((prod_ch_agg["channel"] == "쿠팡").sum())
    st.caption(
        f"전체 {total_rows}행 · 이미지 매칭 {matched_n}행 · "
        f"쿠팡 {coupang_rows}행은 Coupang 상품 이미지 캐시가 있을 때 자동 포함"
    )

    st.dataframe(
        display,
        width="stretch", hide_index=True,
        column_config={
            "이미지": st.column_config.ImageColumn("이미지", width="small"),
            "매출": st.column_config.NumberColumn("매출", format="%d원"),
            "제품명": st.column_config.TextColumn("제품명", width="large"),
            "판매 채널": st.column_config.TextColumn("판매 채널", width="small"),
        },
        height=min(700, 50 + len(display) * 60),
    )

    csv = display.drop(columns=["이미지"]).to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        f"{brand_label} 제품 × 채널 집계 CSV 다운로드", csv,
        f"products_{brand_label}_{start_date.date()}_{end_date}.csv",
        "text/csv", key=f"prod_dl_{brand_label}",
    )

    # ---------- 제품별 일별 판매 추이 (선택형) ----------
    st.markdown("##### 📈 제품별 일별 판매 추이")
    st.caption(
        f"제품을 선택하면 선택 기간({days}일) 내 일별 판매량·매출 추이를 "
        "확인할 수 있습니다. (TOP 6 아래 카드에도 expander 로 바로 확인 가능)"
    )
    # 매출순 정렬된 제품 목록 — 전체 상세 테이블 기준
    prod_list_for_select = (
        prod_ch_agg.groupby("product")["revenue"].sum()
        .sort_values(ascending=False).index.tolist()
    )
    if prod_list_for_select:
        # 기본 선택 = TOP 1
        selected_prod = st.selectbox(
            "제품 선택",
            options=prod_list_for_select,
            index=0,
            format_func=lambda p: (
                p[:60] + ("…" if len(p) > 60 else "")
                + f"  (매출 {int(prod_ch_agg[prod_ch_agg['product']==p]['revenue'].sum()):,}원)"
            ),
            key=f"prod_select_{brand_label}",
        )
        if selected_prod:
            _render_daily_product_chart(
                selected_prod, o_filt,
                start_date, pd.Timestamp(end_date),
                key=f"daily_chart_select_{brand_label}",
            )

    # ---------- 상위 제품 TOP 6 카드 ----------
    prod_agg = prod_ch_agg.groupby(
        ["umbrella", "brand", "product"], dropna=False,
    ).agg(
        orders=("orders", "sum"),
        quantity=("quantity", "sum"),
        revenue=("revenue", "sum"),
        customers=("customers", "sum"),
    ).reset_index().sort_values("revenue", ascending=False)

    prod_images: dict[str, str | None] = {}
    for _, r in prod_ch_agg.iterrows():
        prod_name = r["product"]
        if prod_name not in prod_images and pd.notna(r["image"]):
            prod_images[prod_name] = r["image"]
    prod_agg["image"] = prod_agg["product"].map(prod_images)

    prod_channel_dist = prod_ch_agg.groupby(
        ["product", "판매 채널"]
    )["revenue"].sum().reset_index()

    st.divider()
    st.markdown(f"#### 🏆 {brand_label} 매출 상위 제품 TOP 6")
    top6 = prod_agg.head(6)
    # 2열 배치로 이미지 더 크게
    cols = st.columns(2)
    for idx, (_, row) in enumerate(top6.iterrows()):
        col = cols[idx % 2]
        with col:
            with st.container(border=True):
                img = row.get("image")
                # 이미지 영역 (카드 상단 크게)
                if img and pd.notna(img) and isinstance(img, str) and img.startswith("http"):
                    # HTML 로 aspect ratio 1:1 강제 + 중앙 정렬 + 부드러운 모서리
                    st.markdown(
                        f"""
                        <div style="width:100%; aspect-ratio:1/1;
                                    background:#f8fafc; border-radius:12px;
                                    overflow:hidden; margin-bottom:12px;
                                    display:flex; align-items:center; justify-content:center;">
                            <img src="{img}"
                                 style="width:100%; height:100%; object-fit:cover;" />
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                else:
                    # 이미지 없을 때 플레이스홀더
                    st.markdown(
                        f"""
                        <div style="width:100%; aspect-ratio:1/1;
                                    background: linear-gradient(135deg, #e2e8f0, #f1f5f9);
                                    border-radius:12px; margin-bottom:12px;
                                    display:flex; align-items:center; justify-content:center;
                                    color:#94a3b8; font-size:0.85rem;">
                            📦 이미지 없음
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                # 제품명 + 브랜드 + 매출
                prod_name = str(row["product"])
                short = prod_name[:50] + ("…" if len(prod_name) > 50 else "")
                st.markdown(
                    f"<div style='font-weight:700; font-size:1rem; color:#0f172a; "
                    f"line-height:1.35; margin-bottom:6px;'>{short}</div>",
                    unsafe_allow_html=True,
                )
                st.caption(f"{row['umbrella']} · {row['brand']}")

                total_rev_p = int(row["revenue"])
                st.markdown(
                    f"<div style='font-size:1.25rem; font-weight:700; color:#2563eb; "
                    f"margin-top:8px;'>{total_rev_p:,}원</div>"
                    f"<div style='color:#64748b; font-size:0.82rem;'>"
                    f"주문 {int(row['orders'])}건 · 수량 {int(row['quantity'])}</div>",
                    unsafe_allow_html=True,
                )

                # 채널별 분포
                dist = prod_channel_dist[prod_channel_dist["product"] == prod_name]
                lines = []
                for _, dr in dist.sort_values("revenue", ascending=False).iterrows():
                    ch_rev = int(dr["revenue"])
                    if ch_rev > 0 and total_rev_p > 0:
                        pct = ch_rev / total_rev_p * 100
                        lines.append(f"· {dr['판매 채널']} {ch_rev:,}원 ({pct:.0f}%)")
                if lines:
                    st.caption("\n".join(lines))

                # 🔍 제품 상세 딥 다이브 모달 트리거
                btn_c1, btn_c2 = st.columns(2)
                if btn_c1.button(
                    "🔍 상세 보기",
                    key=f"btn_detail_{brand_label}_{idx}",
                    width="stretch",
                    help="제품 딥 다이브 모달 — 채널 분포, 일별 추이 통합 확인",
                ):
                    show_product_detail(
                        prod_name, o_filt,
                        start_date, pd.Timestamp(end_date),
                        image_url=img if (img and pd.notna(img)) else None,
                    )

                # 📈 일별 판매 추이 — lazy render (버튼 클릭 시에만 차트 생성)
                chart_toggle_key = f"show_chart_top6_{brand_label}_{idx}"
                if chart_toggle_key not in st.session_state:
                    st.session_state[chart_toggle_key] = False
                btn_label = "📈 차트 닫기" \
                    if st.session_state[chart_toggle_key] else "📈 일별 차트"
                if btn_c2.button(
                    btn_label, width="stretch",
                    key=f"btn_{chart_toggle_key}",
                ):
                    st.session_state[chart_toggle_key] = \
                        not st.session_state[chart_toggle_key]
                    st.rerun()
                if st.session_state[chart_toggle_key]:
                    _render_daily_product_chart(
                        prod_name, o_filt,
                        start_date, pd.Timestamp(end_date),
                        key=f"daily_chart_top6_{brand_label}_{idx}",
                    )

    # ---------- 제품 × 채널 히트맵 ----------
    if brand is None and len(o_filt) > 0:
        st.divider()
        st.markdown("#### 🔥 제품 × 채널 매출 히트맵 (상위 20개)")
        top20 = (
            o_filt.groupby("product")["revenue"].sum()
            .sort_values(ascending=False).head(20).index.tolist()
        )
        subset = o_filt[o_filt["product"].isin(top20)]
        pivot = subset.pivot_table(
            index="product", columns="channel",
            values="revenue", aggfunc="sum", fill_value=0,
        )
        pivot = pivot.reindex(top20)

        if not pivot.empty:
            short_y = [p[:40] + ("…" if len(p) > 40 else "") for p in pivot.index]
            fig = px.imshow(
                pivot.values,
                x=pivot.columns.tolist(),
                y=short_y,
                aspect="auto",
                color_continuous_scale="Blues",
                labels=dict(x="채널", y="제품", color="매출(원)"),
                text_auto=True,
            )
            fig.update_layout(
                height=max(400, 40 + len(pivot) * 25),
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig, width="stretch",
                            key=f"heatmap_{brand_label}")


# ==========================================================
# 브랜드별 스택 뷰
# ==========================================================
# 갱신 버튼 2개 (이미지 / 전체 캐시)
hc1, hc2, hc3 = st.columns([3, 1, 1])
with hc2:
    if st.button("🔄 이미지 갱신", width="stretch",
                 help="네이버 커머스 API 로 상품 이미지 재조회"):
        with st.spinner("이미지 갱신 중..."):
            n = refresh_naver_image_cache()
            st.success(f"{n}개 상품 이미지 저장 완료")
            st.cache_data.clear()
            st.rerun()
with hc3:
    if st.button("♻️ 캐시 초기화", width="stretch",
                 help="대시보드 모든 캐시 강제 무효화 — 정규화 규칙 변경 즉시 반영"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("✓ 전체 캐시 초기화 완료. 페이지 새로고침 중...")
        st.rerun()


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
    st.caption("전체 브랜드 제품 통합 · 제품 × 채널 히트맵 포함")
    render_product_view(o_filt_all, brand=None)

with tab_ddok:
    render_brand_banner(
        "똑똑연구소",
        "김똑똑 어린이김 · 똑똑떡뻥 · 번들 (유아식 제품군)",
    )
    render_product_view(o_filt_all, brand="똑똑연구소")

with tab_rolla:
    render_brand_banner(
        "롤라루",
        "여행용 캐리어 · 백팩 (여행용품 제품군)",
    )
    render_product_view(o_filt_all, brand="롤라루")

with tab_ruti:
    render_brand_banner(
        "루티니스트",
        "네이버 스마트스토어 · Cafe24 자사몰 (API 연동 · 매일 10시 동기화)",
    )
    ruti_ofilt = o_filt_all[o_filt_all["umbrella"] == "루티니스트"]
    if ruti_ofilt.empty:
        render_empty_state(
            title="루티니스트 주문 데이터 없음",
            description=(
                "선택 기간 내 루티니스트 주문이 아직 수집되지 않았습니다.\n\n"
                "• **최초 연동 직후** 라면 내일 오전 10시 `sync_all.bat` 이 최대 90일 데이터 수집\n"
                "• **이미 연동 완료** 라면 `.env` 의 `NAVER_COMMERCE_CLIENT_ID_RUTI` / "
                "`CAFE24_MALL_ID_RUTI` 자격증명 유효성 확인 필요\n"
                "• **매출 분석** 탭은 구글 시트 기준으로 집계되므로 API 없이도 표시됨"
            ),
            icon="👟",
            action_label="🔌 API 연결 페이지에서 루티니스트 탭 확인",
        )
    else:
        render_product_view(o_filt_all, brand="루티니스트")


# ==========================================================
# 브랜드 매칭 규칙 참조 (debug 섹션은 제거 — 일반 사용자에게 불필요)
# ==========================================================
with st.expander("현재 브랜드 매칭 규칙"):
    rules_table = pd.DataFrame([
        {"키워드": ", ".join(kws), "제품 라인": b,
         "운영 브랜드": UMBRELLA_BRANDS.get(b, "-")}
        for kws, b in BRAND_RULES
    ])
    st.dataframe(rules_table, width="stretch", hide_index=True)
