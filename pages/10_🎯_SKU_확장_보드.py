"""SKU 확장 의사결정 보드 — 그로잉업팀 #1 미션 직결.

기존 브랜드의 어떤 SKU 를 확장할지 데이터 기반 의사결정 도구:
  1. 옵션 패턴 분석 — 색상/사이즈/타입별 매출·판매수량 ranking
  2. 가격대 갭 — 현 라인업 vs 시장 가격대 분포
  3. SKU 효율 — 광고비 대비 매출, ROAS 별 top/bottom
  4. 옵션 조합 매트릭스 — '잘 팔리는 조합' 패턴 발견
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.ui import (
    setup_page, BRAND_COLORS, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT,
    BORDER_SUBTLE, BG_CARD,
)


setup_page(
    page_title="롤라루 SKU 확장 보드",
    page_icon="🎯",
    header_title="🎯 롤라루 SKU 확장 보드",
    header_subtitle="옵션별 성과 · 가격대 갭 · ROAS 기반 다음 SKU 후보 도출",
)


ROOT = Path(__file__).parent.parent


# ==========================================================
# 데이터 로드
# ==========================================================
@st.cache_data(ttl=600, show_spinner="📊 데이터 로드 중...")
def _load_orders() -> pd.DataFrame:
    p = ROOT / "data" / "orders.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(p, encoding="utf-8", encoding_errors="replace")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    # 브랜드 자동 추출 (store 컬럼 기준)
    df["brand"] = df["store"].apply(_infer_brand)
    return df


@st.cache_data(ttl=600)
def _load_ads() -> pd.DataFrame:
    p = ROOT / "data" / "ads.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(p, encoding="utf-8", encoding_errors="replace")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["brand"] = df["store"].apply(_infer_brand)
    return df


def _infer_brand(store: str) -> str:
    """store 문자열 → 브랜드 정규화."""
    if not isinstance(store, str):
        return "기타"
    s = store.replace(" ", "")
    if "똑똑" in s:
        return "똑똑연구소"
    if "롤라루" in s:
        return "롤라루"
    if "루티니" in s:
        return "루티니스트"
    return "기타"


# ==========================================================
# 옵션 파싱
# ==========================================================
OPT_KEY_NORMALIZE = {
    "색상": "컬러",
    "크기": "사이즈",
}


def _parse_option(opt_str: str) -> dict[str, str]:
    """옵션 문자열 → {옵션키: 값}.

    예: '타입: 확장형 / 컬러: 다크그린 / 사이즈: 51cm(20인치)'
        → {'타입': '확장형', '컬러': '다크그린', '사이즈': '51cm(20인치)'}
    """
    if not isinstance(opt_str, str) or not opt_str.strip():
        return {}
    out: dict[str, str] = {}
    for part in re.split(r"\s*/\s*", opt_str):
        if ":" in part:
            k, v = part.split(":", 1)
            k = OPT_KEY_NORMALIZE.get(k.strip(), k.strip())
            v = v.strip()
            if k and v:
                out[k] = v
    return out


@st.cache_data(ttl=600)
def _expand_options(df: pd.DataFrame) -> pd.DataFrame:
    """option 컬럼을 옵션키별 컬럼으로 확장."""
    if df.empty:
        return df
    parsed = df["option"].apply(_parse_option)
    keys = set()
    for d in parsed:
        keys.update(d.keys())
    out = df.copy()
    for k in keys:
        out[f"opt_{k}"] = parsed.apply(lambda d, kk=k: d.get(kk, ""))
    return out


orders = _load_orders()
ads = _load_ads()

if orders.empty:
    st.warning("📭 주문 데이터가 없습니다. (data/orders.csv 확인)")
    st.stop()


# ==========================================================
# 사이드바 필터
# ==========================================================
st.sidebar.markdown("#### 🔎 필터")

# 브랜드 선택 — 기본 롤라루 (옵션 데이터가 가장 풍부)
brand_options_all = sorted(orders["brand"].unique().tolist())
# 롤라루를 맨 앞으로
brand_options = (
    (["롤라루"] if "롤라루" in brand_options_all else [])
    + [b for b in brand_options_all if b != "롤라루"]
    + ["전체"]
)
selected_brand = st.sidebar.radio(
    "브랜드",
    brand_options,
    index=0,
    help="롤라루(캐리어)가 옵션 데이터가 가장 풍부합니다",
)

# 기간 필터
max_date = orders["date"].max().date()
min_date = orders["date"].min().date()
period_label = st.sidebar.radio(
    "기간",
    ["전체", "최근 30일", "최근 90일", "최근 180일", "올해"],
    index=2,
)
if period_label == "최근 30일":
    start_date = max_date - timedelta(days=30)
elif period_label == "최근 90일":
    start_date = max_date - timedelta(days=90)
elif period_label == "최근 180일":
    start_date = max_date - timedelta(days=180)
elif period_label == "올해":
    start_date = datetime(max_date.year, 1, 1).date()
else:
    start_date = min_date


# 필터 적용
mask = (orders["date"].dt.date >= start_date) & (orders["date"].dt.date <= max_date)
if selected_brand != "전체":
    mask &= orders["brand"] == selected_brand
filtered = orders[mask].copy()

if filtered.empty:
    st.warning("선택한 조건에 매출이 없습니다.")
    st.stop()


# ==========================================================
# 상단 KPI 카드
# ==========================================================
total_rev = filtered["revenue"].sum()
total_qty = filtered["quantity"].sum()
unique_products = filtered["product"].nunique()
unique_options = filtered["option"].dropna().nunique()
avg_price = total_rev / total_qty if total_qty > 0 else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("📅 기간 매출", f"₩{int(total_rev):,}")
k2.metric("📦 총 판매수량", f"{int(total_qty):,}")
k3.metric("🏷 판매 SKU", f"{unique_products}")
k4.metric("🎨 판매 옵션", f"{unique_options}")
k5.metric("💰 평균 단가", f"₩{int(avg_price):,}")

st.markdown("---")


# ==========================================================
# 탭 구성
# ==========================================================
tab_opt, tab_price, tab_roas, tab_combo = st.tabs([
    "🎨 옵션 패턴",
    "💵 가격대 갭",
    "📊 SKU 효율 (ROAS)",
    "🧩 옵션 조합 매트릭스",
])


# ==========================================================
# TAB 1 — 옵션 패턴 (색상/사이즈/타입별 매출)
# ==========================================================
with tab_opt:
    st.markdown("##### 🎨 옵션 키별 성과 ranking")
    st.caption("어떤 색상/사이즈/타입이 잘 팔리는지 → 신 SKU 디자인 가이드")

    expanded = _expand_options(filtered)
    opt_cols = [c for c in expanded.columns if c.startswith("opt_")]

    if not opt_cols:
        st.info(
            "이 브랜드에는 옵션 데이터가 충분치 않습니다. "
            "(주로 롤라루 캐리어에 옵션 정보가 풍부)"
        )
    else:
        # 옵션키 선택
        opt_key_labels = [c.replace("opt_", "") for c in opt_cols]
        selected_opt_key = st.radio(
            "분석할 옵션",
            opt_key_labels,
            index=opt_key_labels.index("컬러") if "컬러" in opt_key_labels else 0,
            horizontal=True,
            key="rollaru_opt_key",
        )

        col = f"opt_{selected_opt_key}"
        opt_df = expanded[expanded[col] != ""].copy()

        if opt_df.empty:
            st.info(f"'{selected_opt_key}' 데이터 없음")
        else:
            agg = (
                opt_df.groupby(col)
                .agg(
                    매출=("revenue", "sum"),
                    판매수량=("quantity", "sum"),
                    주문건수=("order_id", "nunique"),
                )
                .reset_index()
                .rename(columns={col: selected_opt_key})
                .sort_values("매출", ascending=False)
            )
            agg["매출비중%"] = (agg["매출"] / agg["매출"].sum() * 100).round(1)
            agg["평균단가"] = (agg["매출"] / agg["판매수량"]).round(0)

            # 차트 — 매출 기준 horizontal bar
            top_n = min(15, len(agg))
            chart_df = agg.head(top_n)
            fig = px.bar(
                chart_df,
                x="매출",
                y=selected_opt_key,
                orientation="h",
                text="매출비중%",
                color="매출",
                color_continuous_scale=["#fef3c7", "#f59e0b", "#b45309"],
            )
            fig.update_layout(
                yaxis={"categoryorder": "total ascending"},
                xaxis=dict(tickformat=",", title="매출 (원)"),
                height=max(350, top_n * 30),
                margin=dict(l=10, r=10, t=30, b=10),
                showlegend=False,
                coloraxis_showscale=False,
            )
            fig.update_traces(
                texttemplate="%{text}%",
                textposition="outside",
                hovertemplate="%{y}<br>매출: ₩%{x:,.0f}원<br>비중: %{text}%",
            )
            st.plotly_chart(fig, use_container_width=True)

            # 표
            st.markdown("**상세 표**")
            display_agg = agg.copy()
            display_agg["매출"] = display_agg["매출"].apply(lambda v: f"₩{v:,.0f}")
            display_agg["평균단가"] = display_agg["평균단가"].apply(lambda v: f"₩{v:,.0f}")
            display_agg["판매수량"] = display_agg["판매수량"].apply(lambda v: f"{int(v):,}")
            st.dataframe(
                display_agg,
                width="stretch",
                hide_index=True,
                height=min(400, 60 + len(display_agg) * 36),
            )

            # 인사이트 박스
            top_share = agg.iloc[0]["매출비중%"]
            top_value = agg.iloc[0][selected_opt_key]
            bottom_n = (agg["매출비중%"] < 2).sum()
            st.info(
                f"💡 **인사이트** — "
                f"`{top_value}` 가 {selected_opt_key} 매출의 {top_share}% 차지. "
                f"전체 {len(agg)}개 옵션 중 {bottom_n}개는 매출비중 2% 미만 → "
                f"리소스 재배분 후보."
            )


# ==========================================================
# TAB 2 — 가격대 갭 (현 라인업이 어떤 가격대를 안 채우고 있나)
# ==========================================================
with tab_price:
    st.markdown("##### 💵 가격대별 매출 분포 + 미진출 갭")
    st.caption(
        "가격대별 판매 건수와 매출 → 어느 가격대에 진출 안 했는지 발견"
    )

    # 가격대 binning
    price_per_unit = filtered.copy()
    price_per_unit = price_per_unit[price_per_unit["quantity"] > 0].copy()
    price_per_unit["unit_price"] = (
        price_per_unit["revenue"] / price_per_unit["quantity"]
    )
    # 0원 또는 비정상가 제외
    price_per_unit = price_per_unit[
        (price_per_unit["unit_price"] > 1000)
        & (price_per_unit["unit_price"] < 1_000_000)
    ]

    if price_per_unit.empty:
        st.warning("가격 데이터 부족")
    else:
        # bin 너비 자동 결정
        p99 = price_per_unit["unit_price"].quantile(0.99)
        p_min = price_per_unit["unit_price"].min()
        bin_width = max(5000, round((p99 - p_min) / 20 / 5000) * 5000)
        max_price = int(((p99 // bin_width) + 1) * bin_width)

        bins = list(range(0, max_price + bin_width, bin_width))
        labels = [
            f"{b:,}~{b+bin_width:,}원"
            for b in bins[:-1]
        ]
        price_per_unit["가격대"] = pd.cut(
            price_per_unit["unit_price"], bins=bins, labels=labels,
            include_lowest=True,
        )

        price_agg = (
            price_per_unit.groupby("가격대", observed=True)
            .agg(
                매출=("revenue", "sum"),
                판매수량=("quantity", "sum"),
                고유SKU=("product", "nunique"),
            )
            .reset_index()
        )
        price_agg["매출비중%"] = (
            price_agg["매출"] / price_agg["매출"].sum() * 100
        ).round(1)

        # 차트 — 가격대 분포
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=price_agg["가격대"].astype(str),
            y=price_agg["매출"],
            name="매출",
            marker_color="#2563eb",
            text=price_agg["매출비중%"].apply(lambda v: f"{v}%"),
            textposition="outside",
            yaxis="y",
        ))
        fig.add_trace(go.Scatter(
            x=price_agg["가격대"].astype(str),
            y=price_agg["고유SKU"],
            mode="lines+markers",
            name="진출 SKU 수",
            line=dict(color="#f59e0b", width=3),
            yaxis="y2",
        ))
        fig.update_layout(
            height=400,
            xaxis=dict(title="가격대 (원)"),
            yaxis=dict(title="매출 (원)", side="left", tickformat=","),
            yaxis2=dict(title="진출 SKU 수", overlaying="y", side="right"),
            margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # 갭 분석 — "매출비중 큰데 SKU 수 적음" = 확장 기회
        opp = price_agg[price_agg["매출비중%"] >= 5].copy()
        opp["SKU당평균매출"] = (opp["매출"] / opp["고유SKU"]).round(0)
        opp = opp.sort_values("SKU당평균매출", ascending=False)

        st.markdown("**🎯 확장 기회 가격대 (SKU 당 매출이 높은 가격대)**")
        if not opp.empty:
            opp_display = opp.copy()
            opp_display["매출"] = opp_display["매출"].apply(
                lambda v: f"₩{int(v):,}"
            )
            opp_display["SKU당평균매출"] = opp_display["SKU당평균매출"].apply(
                lambda v: f"₩{int(v):,}"
            )
            opp_display["판매수량"] = opp_display["판매수량"].apply(
                lambda v: f"{int(v):,}"
            )
            st.dataframe(
                opp_display[["가격대", "매출", "매출비중%", "고유SKU",
                             "판매수량", "SKU당평균매출"]],
                width="stretch", hide_index=True,
            )
            top_opp = opp.iloc[0]
            st.success(
                f"💡 **`{top_opp['가격대']}` 가격대** — "
                f"SKU {int(top_opp['고유SKU'])}개로 매출 {top_opp['매출비중%']}% 차지 → "
                f"SKU 당 평균 매출 ₩{int(top_opp['SKU당평균매출']):,}. "
                f"이 가격대 신규 SKU 추가 시 ROI 가장 큼."
            )


# ==========================================================
# TAB 3 — SKU 효율 (ROAS = 매출 / 광고비)
# ==========================================================
with tab_roas:
    st.markdown("##### 📊 SKU 효율 — 광고비 대비 매출 (ROAS)")
    st.caption("광고 안 태웠는데도 잘 팔리는 옵션 = 검증된 확장 후보")

    # 옵션 단위로 매출 + (브랜드별 광고비 단순 배분)
    if ads.empty:
        st.info("광고 데이터가 없어 ROAS 계산 불가")
    else:
        # 브랜드별 광고비 합
        ads_filtered = ads[
            (ads["date"].dt.date >= start_date)
            & (ads["date"].dt.date <= max_date)
        ].copy()
        if selected_brand != "전체":
            ads_filtered = ads_filtered[ads_filtered["brand"] == selected_brand]

        brand_ad_spend = (
            ads_filtered.groupby("brand")["spend"].sum().to_dict()
        )

        # 제품별 매출
        prod_agg = (
            filtered.groupby(["brand", "product"])
            .agg(매출=("revenue", "sum"), 판매수량=("quantity", "sum"))
            .reset_index()
        )
        # 브랜드 매출 비중으로 광고비 배분 (proxy)
        brand_rev = (
            filtered.groupby("brand")["revenue"].sum().to_dict()
        )
        def _est_spend(row):
            br = row["brand"]
            br_rev = brand_rev.get(br, 0)
            if br_rev <= 0:
                return 0
            return brand_ad_spend.get(br, 0) * (row["매출"] / br_rev)

        prod_agg["추정광고비"] = prod_agg.apply(_est_spend, axis=1)
        prod_agg["ROAS"] = np.where(
            prod_agg["추정광고비"] > 0,
            prod_agg["매출"] / prod_agg["추정광고비"],
            np.nan,
        )
        # 광고 0원이지만 매출이 있는 케이스 → "오가닉 매출"
        prod_agg["오가닉여부"] = prod_agg["추정광고비"] < (prod_agg["매출"] * 0.01)

        # 상위 매출 N개만 노이즈 제거
        prod_agg = prod_agg.sort_values("매출", ascending=False).head(30)

        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**🏆 매출 TOP — 광고 효율 좋은 SKU**")
            top = prod_agg.nlargest(8, "매출")[
                ["product", "매출", "판매수량", "ROAS"]
            ].copy()
            top["매출"] = top["매출"].apply(lambda v: f"₩{int(v):,}")
            top["판매수량"] = top["판매수량"].apply(lambda v: f"{int(v):,}")
            top["ROAS"] = top["ROAS"].apply(
                lambda v: f"{v:,.1f}x" if pd.notna(v) and v > 0 else "—"
            )
            st.dataframe(top, hide_index=True, width="stretch")

        with col_r:
            st.markdown("**🌱 오가닉 강자 — 광고 없어도 잘 팔리는 SKU**")
            organic = prod_agg[prod_agg["오가닉여부"]].nlargest(8, "매출")[
                ["product", "매출", "판매수량"]
            ].copy()
            if organic.empty:
                st.caption("(추정 광고비 0원인 SKU 없음)")
            else:
                organic["매출"] = organic["매출"].apply(
                    lambda v: f"₩{int(v):,}"
                )
                organic["판매수량"] = organic["판매수량"].apply(
                    lambda v: f"{int(v):,}"
                )
                st.dataframe(organic, hide_index=True, width="stretch")

        st.caption(
            "ℹ️ ROAS 는 브랜드 광고비를 매출 비중으로 배분한 추정치 (정확한 "
            "SKU 단위 광고비가 아닌 proxy). 오가닉 강자는 신 SKU 확장 시 "
            "광고 없이도 자력 판매 가능성 높음 — 우선순위 후보."
        )


# ==========================================================
# TAB 4 — 옵션 조합 분석 (잘 팔리는 조합 찾기)
# ==========================================================
with tab_combo:
    st.markdown("##### 🧩 옵션 조합 분석 — '어떤 조합이 잘 팔리는지' 한눈에")
    st.caption(
        "두 옵션을 교차해서 베스트셀러 조합을 찾고, 각 메인 옵션별 "
        "최강 서브 옵션을 자동으로 추출합니다."
    )

    expanded = _expand_options(filtered)
    opt_cols = [c for c in expanded.columns if c.startswith("opt_")]

    if len(opt_cols) < 2:
        st.info("두 개 이상의 옵션 키가 있는 데이터가 필요합니다.")
    else:
        opt_labels = [c.replace("opt_", "") for c in opt_cols]
        c1, c2 = st.columns(2)
        with c1:
            main_key = st.radio(
                "메인 옵션 (X축)",
                opt_labels,
                index=opt_labels.index("컬러") if "컬러" in opt_labels else 0,
                help="가로축에 배치할 옵션",
                horizontal=True,
                key="rollaru_combo_main",
            )
        with c2:
            remaining = [l for l in opt_labels if l != main_key]
            sub_key = st.radio(
                "서브 옵션 (색상 구분)",
                remaining,
                index=remaining.index("사이즈") if "사이즈" in remaining else 0,
                help="누적 막대를 색으로 구분할 옵션",
                horizontal=True,
                key="rollaru_combo_sub",
            )

        main_col, sub_col = f"opt_{main_key}", f"opt_{sub_key}"
        combo_df = expanded[
            (expanded[main_col] != "") & (expanded[sub_col] != "")
        ].copy()

        if combo_df.empty:
            st.info("두 옵션 모두 값이 있는 데이터 없음")
        else:
            # 조합별 집계
            combo_agg = (
                combo_df.groupby([main_col, sub_col])
                .agg(매출=("revenue", "sum"),
                     판매수량=("quantity", "sum"))
                .reset_index()
            )
            total_combo_rev = combo_agg["매출"].sum()
            combo_agg["매출비중%"] = (
                combo_agg["매출"] / total_combo_rev * 100
            ).round(1)

            # 메인 옵션별 총 매출 (X축 정렬 기준)
            main_totals = (
                combo_agg.groupby(main_col)["매출"]
                .sum()
                .sort_values(ascending=False)
            )
            main_order = main_totals.index.tolist()

            # ============================================
            # ① Stacked Bar — 메인 옵션별 서브 옵션 매출 누적
            # ============================================
            st.markdown(f"**📊 {main_key}별 매출 구성 (색 = {sub_key})**")
            st.caption(
                f"각 막대 = 한 {main_key} 의 총 매출. "
                f"색 영역 크기 = 그 {main_key} 안에서 각 {sub_key} 의 매출 기여."
            )

            fig = px.bar(
                combo_agg,
                x=main_col, y="매출",
                color=sub_col,
                category_orders={main_col: main_order},
                color_discrete_sequence=px.colors.qualitative.Set2,
                hover_data={"판매수량": True, "매출비중%": True},
                labels={main_col: main_key, sub_col: sub_key},
            )
            fig.update_layout(
                height=450,
                margin=dict(l=10, r=10, t=20, b=10),
                yaxis=dict(title="매출 (원)", tickformat=","),
                xaxis=dict(title=main_key),
                legend=dict(
                    orientation="v",
                    title=sub_key,
                    yanchor="top", y=1, xanchor="left", x=1.02,
                ),
            )
            fig.update_traces(
                hovertemplate=(
                    f"<b>%{{x}}</b><br>"
                    f"{sub_key}: %{{fullData.name}}<br>"
                    "매출: ₩%{y:,.0f}<br>"
                    "판매수량: %{customdata[0]:,}<br>"
                    "비중: %{customdata[1]:.1f}%<extra></extra>"
                ),
            )
            st.plotly_chart(fig, use_container_width=True)

            # ============================================
            # ② 메인 옵션별 BEST 서브 옵션 자동 추출
            # ============================================
            st.markdown(f"**🏆 {main_key}별 베스트 {sub_key}**")
            st.caption(
                f"각 {main_key} 에서 가장 매출 높은 {sub_key} 자동 추출 → "
                f"신규 SKU 디자인 가이드 (예: '블랙 컬러는 어떤 사이즈를 같이 내야 하는가')"
            )

            best_per_main = (
                combo_agg.sort_values("매출", ascending=False)
                .groupby(main_col)
                .head(1)
                .sort_values("매출", ascending=False)
            )
            best_display = best_per_main[[main_col, sub_col, "매출",
                                          "판매수량", "매출비중%"]].copy()
            best_display.columns = [
                main_key, f"베스트 {sub_key}", "매출", "판매수량", "전체매출 비중%",
            ]
            best_display["매출"] = best_display["매출"].apply(
                lambda v: f"₩{int(v):,}"
            )
            best_display["판매수량"] = best_display["판매수량"].apply(
                lambda v: f"{int(v):,}"
            )
            st.dataframe(
                best_display, hide_index=True, width="stretch",
                height=min(400, 60 + len(best_display) * 36),
            )

            # ============================================
            # ③ Top 10 전체 조합 ranking (어떤 조합이 절대 매출 큰지)
            # ============================================
            st.markdown(f"**🥇 전체 조합 Top 10 — 절대 매출 ranking**")
            top10 = combo_agg.sort_values("매출", ascending=False).head(10).copy()
            top10["조합"] = (
                top10[main_col].astype(str) + " × " + top10[sub_col].astype(str)
            )
            top10_chart = top10[["조합", "매출", "매출비중%"]].copy()

            fig_top = px.bar(
                top10_chart.iloc[::-1],
                x="매출", y="조합",
                orientation="h",
                text="매출비중%",
                color="매출",
                color_continuous_scale=["#fef3c7", "#f59e0b", "#b45309"],
            )
            fig_top.update_layout(
                height=max(350, len(top10_chart) * 35),
                margin=dict(l=10, r=10, t=20, b=10),
                showlegend=False,
                coloraxis_showscale=False,
                xaxis=dict(title="매출 (원)", tickformat=","),
                yaxis=dict(title=""),
            )
            fig_top.update_traces(
                texttemplate="%{text}%",
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>매출: ₩%{x:,.0f}원<extra></extra>",
            )
            st.plotly_chart(fig_top, use_container_width=True)

            # ============================================
            # ④ 인사이트 박스
            # ============================================
            top_combo = top10.iloc[0]
            # 카니발리제이션 시그널: 같은 메인 옵션에 서브 옵션 여러 개가 비슷한 매출
            cannibal_warn = ""
            for main_val, grp in combo_agg.groupby(main_col):
                if len(grp) >= 3:
                    sorted_grp = grp.sort_values("매출", ascending=False)
                    if (sorted_grp["매출"].iloc[1] /
                            max(sorted_grp["매출"].iloc[0], 1)) > 0.7:
                        # 1, 2위 매출 차이 30% 미만 → 카니발 가능성
                        cannibal_warn = (
                            f"\n\n⚠️ **카니발리제이션 시그널** — "
                            f"`{main_val}` 안에서 "
                            f"`{sorted_grp[sub_col].iloc[0]}` 와 "
                            f"`{sorted_grp[sub_col].iloc[1]}` 가 "
                            f"매출 차이 30% 미만 → 두 옵션이 서로 잠식 가능성"
                        )
                        break

            st.success(
                f"💡 **베스트 조합** — `{top_combo[main_col]} × {top_combo[sub_col]}` "
                f"매출 ₩{int(top_combo['매출']):,} (전체의 {top_combo['매출비중%']}%)\n\n"
                f"이 조합 기반으로 비슷한 옵션 확장 검토. "
                f"위 '베스트 {sub_key}' 표를 참고하면 각 {main_key}별 최강 조합 한눈에 보여요."
                f"{cannibal_warn}"
            )


# ==========================================================
# 하단 — 액션 가이드
# ==========================================================
st.markdown("---")
with st.expander("📘 이 보드 활용법", expanded=False):
    st.markdown("""
**① 옵션 패턴**
- 잘 팔리는 컬러/사이즈/타입 → 신 SKU 디자인 가이드
- 매출비중 2% 미만 옵션 → 단종 검토 또는 마케팅 강화

**② 가격대 갭**
- SKU 당 평균 매출이 높은 가격대 → 신 SKU 추가 시 ROI 큰 가격대
- SKU 가 많은데 매출비중 낮은 가격대 → 카니발리제이션 가능성

**③ ROAS**
- 오가닉 강자 = 광고 없이도 잘 팔리는 SKU → 확장 우선순위
- 광고 의존도 높은 SKU → 광고 끊으면 매출 떨어질 risk

**④ 옵션 조합 매트릭스**
- "컬러 × 사이즈" 매트릭스에서 hot spot = 검증된 조합
- 인접 셀 (비슷한 조합) 우선 확장
    """)
