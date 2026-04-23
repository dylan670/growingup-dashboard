"""제품 × 브랜드 통합 분석 — 브랜드 탭(전체/똑똑연구소/롤라루) × 제품별 상세."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from api.naver_searchad import load_client_from_env
from utils.data import load_orders
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
)


setup_page(
    page_title="제품 분석",
    page_icon="📦",
    header_title="📦 제품 분석",
    header_subtitle="제품·브랜드 단위 매출·광고 통합 (네이버 광고비 자동 브랜드 할당)",
)

@st.cache_data(ttl=300, show_spinner="주문 데이터 로드 중...")
def _cached_orders_classified() -> pd.DataFrame:
    """주문 데이터 로드 + 브랜드 분류 (5분 캐시)."""
    df = load_orders()
    return classify_orders(df)


orders = load_orders()  # 날짜 range 계산용 (빠름)
from datetime import date as _today_func
today_real = _today_func.today()
orders_max = orders["date"].max().date() if not orders.empty else today_real
orders_min = orders["date"].min().date() if not orders.empty else _today_func(today_real.year - 1, 1, 1)

# ==========================================================
# 기간 선택
# ==========================================================
c1, c2, _ = st.columns([1, 1, 2])
with c1:
    period = st.selectbox(
        "기간", ["최근 7일", "최근 14일", "최근 30일", "최근 90일"], index=2,
    )
with c2:
    end_date = st.date_input(
        "종료일", value=orders_max,
        min_value=orders_min, max_value=today_real,
        help="실제 오늘까지 선택 가능.",
    )

days = {"최근 7일": 7, "최근 14일": 14,
        "최근 30일": 30, "최근 90일": 90}[period]
start_date = pd.Timestamp(end_date) - pd.Timedelta(days=days - 1)
st.caption(f"분석 기간: **{start_date.date()} ~ {end_date}** ({days}일)")


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

    # 이중축 차트: 수량(막대, 파랑) + 매출(선, 빨강)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=daily["date"], y=daily["quantity"],
            name="판매 수량",
            marker_color="#3b82f6",
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
            line=dict(color="#dc2626", width=2.5),
            marker=dict(size=6, color="#dc2626"),
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
        st.info(f"{brand_label}: 해당 기간 주문 데이터 없음.")
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

        ad_spend = ad_spend_by_umbrella.get(umbrella, 0)
        roas = revenue / ad_spend * 100 if ad_spend else 0

        with st.container(border=True):
            # 상단: 브랜드 정보 + 지표 4개 (숫자 컬럼 너비 확대해 짤림 방지)
            cc = st.columns([2, 2, 1, 2, 1])
            cc[0].markdown(f"### {umbrella}")
            cc[0].caption(f"{products_n}종 상품 · {customers}명 고객")
            cc[1].metric("매출", f"{revenue:,}원")
            cc[2].metric("주문", f"{orders_n:,}건")
            cc[3].metric("네이버 광고비", f"{int(ad_spend):,}원")
            cc[4].metric(
                "추정 ROAS",
                f"{roas:.0f}%" if ad_spend else "—",
                help="네이버 광고비 대비 매출. 쿠팡/메타 광고비 별도.",
            )

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

    prod_ch_agg = o_filt.groupby(
        ["umbrella", "brand", "판매 채널", "store", "channel", "product"],
        dropna=False,
    ).agg(
        orders=("order_id", "count"),
        quantity=("quantity", "sum"),
        revenue=("revenue", "sum"),
        customers=("customer_id", "nunique"),
    ).reset_index().sort_values("revenue", ascending=False)

    if prod_ch_agg.empty:
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

                # 📈 일별 판매 추이 — lazy render (버튼 클릭 시에만 차트 생성)
                chart_toggle_key = f"show_chart_top6_{brand_label}_{idx}"
                if chart_toggle_key not in st.session_state:
                    st.session_state[chart_toggle_key] = False
                btn_label = (
                    f"📈 일별 판매 추이 ({days}일) 닫기"
                    if st.session_state[chart_toggle_key]
                    else f"📈 일별 판매 추이 ({days}일) 보기"
                )
                if st.button(
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
# 상품 이미지 갱신 버튼 (상단 우측)
hc1, hc2 = st.columns([4, 1])
with hc2:
    if st.button("🔄 상품 이미지 갱신", width="stretch",
                 help="네이버 커머스 API로 상품 이미지 캐시 재조회"):
        with st.spinner("이미지 갱신 중..."):
            n = refresh_naver_image_cache()
            st.success(f"{n}개 상품 이미지 저장 완료")
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
        "현재 제품 API 미연동 (자사몰/네이버 제품 계정 없음)",
    )
    st.info(
        "👟 **루티니스트 제품 데이터 없음** — 대시보드에서 수집 중인 주문 API가 없습니다. "
        "자사몰/네이버 API 연결 시 자동으로 이 탭에 제품별 상세가 추가됩니다. "
        "매출 현황은 **💰 매출 분석 → 👟 루티니스트** 탭에서 구글 시트 기반으로 확인 가능."
    )


# ==========================================================
# 네이버 광고그룹 매칭 디버그 + 매칭 규칙
# ==========================================================
if ad_spend_debug is not None and not ad_spend_debug.empty:
    with st.expander("네이버 광고그룹 → 브랜드 매칭 결과"):
        st.dataframe(
            ad_spend_debug.rename(columns={
                "이름": "광고그룹",
                "brand": "매칭된 제품 라인",
                "umbrella": "매칭된 운영 브랜드",
                "비용": "광고비",
                "매출": "전환매출",
                "ROAS(%)": "ROAS(%)",
            }),
            width="stretch", hide_index=True,
        )
        st.caption(
            "'공통' 매칭 그룹은 키워드 미인식 케이스. "
            "`utils/products.py` 의 `BRAND_RULES` 에 키워드 추가하면 자동 재분류."
        )

with st.expander("현재 브랜드 매칭 규칙"):
    rules_table = pd.DataFrame([
        {"키워드": ", ".join(kws), "제품 라인": b,
         "운영 브랜드": UMBRELLA_BRANDS.get(b, "-")}
        for kws, b in BRAND_RULES
    ])
    st.dataframe(rules_table, width="stretch", hide_index=True)
