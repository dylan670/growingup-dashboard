"""똑똑연구소 SKU 확장 의사결정 보드.

유아식(김/떡뻥/번들) 브랜드 특성 반영 — 옵션이 없는 대신 카테고리·맛·번들
조합으로 SKU 확장 후보 도출.

탭 구성:
  1. 🍱 카테고리 분포  — 김/떡뻥/번들/이유식 매출·SKU 갭
  2. 👅 맛 선호도     — 담백한/백미/조미 등 맛별 ranking
  3. 📦 번들 vs 단품  — 번들 매출 vs 같은 SKU 단품 매출 비교
  4. 📊 SKU 효율(ROAS) — 광고비 대비 매출 + 오가닉 강자

초안 — 사용자 피드백 받으며 점진 개선 예정.
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
    page_title="똑똑연구소 SKU 보드",
    page_icon="🍱",
    header_title="🍱 똑똑연구소 SKU 확장 보드",
    header_subtitle="카테고리·맛·번들 패턴 → 유아식 신 SKU 후보 도출",
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
    df["brand"] = df["store"].apply(_brand_of_store)
    return df[df["brand"] == "똑똑연구소"].copy()


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
    df["brand"] = df["store"].apply(_brand_of_store)
    return df[df["brand"] == "똑똑연구소"].copy()


def _brand_of_store(store: str) -> str:
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
# 분류 함수 — 제품명 키워드 기반
# ==========================================================
def _category(p: str) -> str:
    """제품명 → 카테고리 추정."""
    if not isinstance(p, str):
        return "기타"
    pl = p
    if "+" in pl or "번들" in pl or "세트" in pl:
        return "번들/세트"
    if "김" in pl and "떡뻥" not in pl:
        return "김"
    if "떡뻥" in pl or "쌀과자" in pl:
        return "떡뻥/쌀과자"
    if "분유" in pl:
        return "분유"
    if "이유식" in pl:
        return "이유식"
    if "과자" in pl or "간식" in pl:
        return "기타 간식"
    return "기타"


# 맛 키워드 사전 (우선순위 — 긴 것부터)
FLAVOR_KEYWORDS = [
    "담백한맛", "백미맛", "야채맛", "단호박맛", "시금치맛",
    "당근맛", "블루베리맛", "김치맛", "구운 조미", "구운조미",
    "조미", "오리지널", "구운", "저염",
]


def _flavors(p: str) -> list[str]:
    """제품명 → 맛 리스트 (복수 매칭 허용)."""
    if not isinstance(p, str):
        return []
    found = []
    remaining = p
    for kw in FLAVOR_KEYWORDS:
        if kw in remaining:
            found.append(kw)
            remaining = remaining.replace(kw, " ")  # 중복 매칭 방지
    return found


def _primary_flavor(p: str) -> str:
    """제품명에서 발견된 첫 맛만 반환 (분석용)."""
    fl = _flavors(p)
    return fl[0] if fl else "(맛 정보 없음)"


def _is_bundle(p: str) -> bool:
    """번들 여부."""
    if not isinstance(p, str):
        return False
    return ("+" in p) or ("번들" in p) or ("세트" in p)


orders = _load_orders()
ads = _load_ads()

if orders.empty:
    st.warning("📭 똑똑연구소 주문 데이터가 없습니다.")
    st.stop()


# ==========================================================
# 사이드바 필터 — 기간만 (브랜드는 똑똑연구소 고정)
# ==========================================================
st.sidebar.markdown("#### 🔎 필터")

max_date = orders["date"].max().date()
min_date = orders["date"].min().date()
period_label = st.sidebar.selectbox(
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

mask = (orders["date"].dt.date >= start_date) & (orders["date"].dt.date <= max_date)
filtered = orders[mask].copy()

if filtered.empty:
    st.warning("선택한 기간에 매출이 없습니다.")
    st.stop()

# 카테고리·맛·번들 분류 컬럼 추가
filtered["category"] = filtered["product"].apply(_category)
filtered["primary_flavor"] = filtered["product"].apply(_primary_flavor)
filtered["is_bundle"] = filtered["product"].apply(_is_bundle)


# ==========================================================
# 상단 KPI 카드
# ==========================================================
total_rev = filtered["revenue"].sum()
total_qty = filtered["quantity"].sum()
unique_products = filtered["product"].nunique()
unique_cats = filtered["category"].nunique()
avg_price = total_rev / total_qty if total_qty > 0 else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("📅 기간 매출", f"₩{int(total_rev):,}")
k2.metric("📦 총 판매수량", f"{int(total_qty):,}")
k3.metric("🏷 판매 SKU", f"{unique_products}")
k4.metric("🍱 카테고리", f"{unique_cats}")
k5.metric("💰 평균 단가", f"₩{int(avg_price):,}")

st.markdown("---")


# ==========================================================
# 탭 구성
# ==========================================================
tab_cat, tab_flavor, tab_bundle, tab_roas = st.tabs([
    "🍱 카테고리 분포",
    "👅 맛 선호도",
    "📦 번들 vs 단품",
    "📊 SKU 효율 (ROAS)",
])


# ==========================================================
# TAB 1 — 카테고리 분포 (김/떡뻥/번들 갭)
# ==========================================================
with tab_cat:
    st.markdown("##### 🍱 카테고리별 매출 + SKU 분포")
    st.caption(
        "어느 카테고리가 잘 팔리는지 + SKU 수가 적은데 매출 큰 카테고리 = 확장 기회"
    )

    cat_agg = (
        filtered.groupby("category")
        .agg(
            매출=("revenue", "sum"),
            판매수량=("quantity", "sum"),
            SKU수=("product", "nunique"),
            구매자수=("customer_id", "nunique"),
        )
        .reset_index()
        .sort_values("매출", ascending=False)
    )
    cat_agg["매출비중%"] = (
        cat_agg["매출"] / cat_agg["매출"].sum() * 100
    ).round(1)
    cat_agg["SKU당평균매출"] = (cat_agg["매출"] / cat_agg["SKU수"]).round(0)

    # 차트 — 매출 bar + SKU 수 line
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=cat_agg["category"],
        y=cat_agg["매출"],
        name="매출",
        marker_color="#2563eb",
        text=cat_agg["매출비중%"].apply(lambda v: f"{v}%"),
        textposition="outside",
        yaxis="y",
    ))
    fig.add_trace(go.Scatter(
        x=cat_agg["category"],
        y=cat_agg["SKU수"],
        mode="lines+markers+text",
        text=cat_agg["SKU수"],
        textposition="top center",
        name="진출 SKU 수",
        line=dict(color="#f59e0b", width=3),
        yaxis="y2",
    ))
    fig.update_layout(
        height=420,
        xaxis=dict(title="카테고리"),
        yaxis=dict(title="매출 (원)", side="left", tickformat=","),
        yaxis2=dict(title="SKU 수", overlaying="y", side="right"),
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", y=1.1),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 표
    st.markdown("**상세 표**")
    display_cat = cat_agg.copy()
    display_cat["매출"] = display_cat["매출"].apply(lambda v: f"₩{int(v):,}")
    display_cat["SKU당평균매출"] = display_cat["SKU당평균매출"].apply(
        lambda v: f"₩{int(v):,}"
    )
    display_cat["판매수량"] = display_cat["판매수량"].apply(lambda v: f"{int(v):,}")
    st.dataframe(
        display_cat[["category", "매출", "매출비중%", "SKU수",
                     "SKU당평균매출", "판매수량", "구매자수"]],
        width="stretch", hide_index=True,
    )

    # 인사이트 — SKU 적은데 SKU당 매출 큰 카테고리
    opp = cat_agg.sort_values("SKU당평균매출", ascending=False)
    top_opp = opp.iloc[0]
    underdeveloped = cat_agg[
        (cat_agg["매출비중%"] >= 3) & (cat_agg["SKU수"] <= 2)
    ]
    if not underdeveloped.empty:
        u = underdeveloped.iloc[0]
        st.success(
            f"💡 **미개척 카테고리** — `{u['category']}` 는 "
            f"SKU {int(u['SKU수'])}개로 매출 {u['매출비중%']}% 차지. "
            f"SKU 당 평균 매출 ₩{int(u['SKU당평균매출']):,}. "
            f"신규 SKU 추가 시 ROI 가장 큼."
        )
    else:
        st.info(
            f"💡 **현재 잘 팔리는 카테고리** — `{top_opp['category']}` "
            f"SKU 당 평균 매출 ₩{int(top_opp['SKU당평균매출']):,}. "
            f"이 카테고리 변형 SKU 검토 권장."
        )


# ==========================================================
# TAB 2 — 맛 선호도
# ==========================================================
with tab_flavor:
    st.markdown("##### 👅 맛별 매출 ranking")
    st.caption(
        "제품명에서 자동 추출한 맛 키워드 기반. "
        "잘 팔리는 맛 = 신 SKU 의 안전한 시작점."
    )

    flavor_df = filtered[filtered["primary_flavor"] != "(맛 정보 없음)"].copy()
    if flavor_df.empty:
        st.info("맛 정보가 추출 가능한 제품이 없습니다.")
    else:
        flavor_agg = (
            flavor_df.groupby("primary_flavor")
            .agg(
                매출=("revenue", "sum"),
                판매수량=("quantity", "sum"),
                SKU수=("product", "nunique"),
            )
            .reset_index()
            .sort_values("매출", ascending=False)
        )
        flavor_agg["매출비중%"] = (
            flavor_agg["매출"] / flavor_agg["매출"].sum() * 100
        ).round(1)
        flavor_agg = flavor_agg.rename(columns={"primary_flavor": "맛"})

        # horizontal bar
        fig = px.bar(
            flavor_agg.head(15),
            x="매출", y="맛",
            orientation="h",
            text="매출비중%",
            color="매출",
            color_continuous_scale=["#dbeafe", "#2563eb", "#1e3a8a"],
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            xaxis=dict(tickformat=",", title="매출 (원)"),
            height=max(350, len(flavor_agg) * 35),
            margin=dict(l=10, r=10, t=20, b=10),
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
        display_fl = flavor_agg.copy()
        display_fl["매출"] = display_fl["매출"].apply(lambda v: f"₩{int(v):,}")
        display_fl["판매수량"] = display_fl["판매수량"].apply(lambda v: f"{int(v):,}")
        st.dataframe(
            display_fl[["맛", "매출", "매출비중%", "판매수량", "SKU수"]],
            width="stretch", hide_index=True,
        )

        # 인사이트
        top_flavor = flavor_agg.iloc[0]
        bottom_flavors = flavor_agg[flavor_agg["매출비중%"] < 3]
        st.success(
            f"💡 **베스트 맛** — `{top_flavor['맛']}` 가 {top_flavor['매출비중%']}% "
            f"차지 (매출 ₩{int(top_flavor['매출']):,}). "
            f"신 SKU 디자인 시 이 맛 기반으로 변형 검토."
            + (
                f"\n\n⚠️ {len(bottom_flavors)}개 맛은 매출비중 3% 미만 — "
                f"단종 또는 마케팅 강화 검토."
                if len(bottom_flavors) > 0 else ""
            )
        )


# ==========================================================
# TAB 3 — 번들 vs 단품
# ==========================================================
with tab_bundle:
    st.markdown("##### 📦 번들 vs 단품 매출 비교")
    st.caption(
        "번들/세트 SKU 의 매출 효율과 단품 SKU 의 매출 효율 비교. "
        "번들 SKU 가 적은데 매출 좋으면 번들 라인 확장 기회."
    )

    bundle_agg = (
        filtered.groupby("is_bundle")
        .agg(
            매출=("revenue", "sum"),
            판매수량=("quantity", "sum"),
            SKU수=("product", "nunique"),
            구매자수=("customer_id", "nunique"),
        )
        .reset_index()
    )
    bundle_agg["타입"] = bundle_agg["is_bundle"].map(
        {True: "📦 번들/세트", False: "🍙 단품"}
    )
    bundle_agg["SKU당평균매출"] = (
        bundle_agg["매출"] / bundle_agg["SKU수"]
    ).round(0)
    bundle_agg["객단가"] = (
        bundle_agg["매출"] / bundle_agg["구매자수"]
    ).round(0)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**💰 매출 비교**")
        fig = px.bar(
            bundle_agg, x="타입", y="매출",
            text="매출",
            color="타입",
            color_discrete_map={
                "📦 번들/세트": "#7c3aed", "🍙 단품": "#2563eb",
            },
        )
        fig.update_traces(
            texttemplate="₩%{text:,.0f}",
            textposition="outside",
        )
        fig.update_layout(
            height=350, margin=dict(l=10, r=10, t=20, b=10),
            yaxis=dict(tickformat=",", title="매출 (원)"),
            xaxis=dict(title=""),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("**📊 효율 지표 (SKU 당 / 객단가)**")
        comp_data = []
        for _, row in bundle_agg.iterrows():
            comp_data.append({
                "타입": row["타입"],
                "지표": "SKU당 평균매출",
                "값": row["SKU당평균매출"],
            })
            comp_data.append({
                "타입": row["타입"],
                "지표": "객단가",
                "값": row["객단가"],
            })
        comp_df = pd.DataFrame(comp_data)
        fig = px.bar(
            comp_df, x="지표", y="값", color="타입",
            color_discrete_map={
                "📦 번들/세트": "#7c3aed", "🍙 단품": "#2563eb",
            },
            barmode="group",
            text="값",
        )
        fig.update_traces(texttemplate="₩%{text:,.0f}", textposition="outside")
        fig.update_layout(
            height=350, margin=dict(l=10, r=10, t=20, b=10),
            yaxis=dict(tickformat=",", title="원"),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

    # 표
    st.markdown("**상세 비교**")
    display_b = bundle_agg.copy()
    display_b["매출"] = display_b["매출"].apply(lambda v: f"₩{int(v):,}")
    display_b["SKU당평균매출"] = display_b["SKU당평균매출"].apply(
        lambda v: f"₩{int(v):,}"
    )
    display_b["객단가"] = display_b["객단가"].apply(lambda v: f"₩{int(v):,}")
    display_b["판매수량"] = display_b["판매수량"].apply(lambda v: f"{int(v):,}")
    st.dataframe(
        display_b[["타입", "매출", "SKU수", "SKU당평균매출",
                   "판매수량", "구매자수", "객단가"]],
        width="stretch", hide_index=True,
    )

    # 인사이트
    bundle_row = bundle_agg[bundle_agg["is_bundle"]]
    single_row = bundle_agg[~bundle_agg["is_bundle"]]
    if not bundle_row.empty and not single_row.empty:
        b = bundle_row.iloc[0]
        s = single_row.iloc[0]
        if b["SKU수"] < s["SKU수"] * 0.5 and b["SKU당평균매출"] >= s["SKU당평균매출"]:
            st.success(
                f"💡 **번들 확장 기회** — 번들 SKU {int(b['SKU수'])}개 vs "
                f"단품 {int(s['SKU수'])}개. 그런데 SKU 당 평균 매출은 "
                f"번들 ₩{int(b['SKU당평균매출']):,} ≥ 단품 ₩{int(s['SKU당평균매출']):,}. "
                f"번들 라인 추가 시 ROI 높음."
            )
        elif b["객단가"] > s["객단가"] * 1.2:
            st.success(
                f"💡 **번들이 객단가 ↑** — 번들 객단가 ₩{int(b['객단가']):,} vs "
                f"단품 ₩{int(s['객단가']):,} ({(b['객단가']/s['객단가']-1)*100:.0f}% 높음). "
                f"번들 SKU 추가 검토."
            )
        else:
            st.info(
                f"단품 위주 매출 구조 — 번들 {int(b['SKU수'])}개 / 단품 {int(s['SKU수'])}개. "
                f"번들 라인 확장 시 신규 매출 발생 가능성."
            )


# ==========================================================
# TAB 4 — SKU 효율 (ROAS)
# ==========================================================
with tab_roas:
    st.markdown("##### 📊 SKU 효율 — 광고비 대비 매출 (ROAS)")
    st.caption(
        "광고 안 태웠는데도 잘 팔리는 SKU = 검증된 확장 후보 (오가닉 강자)"
    )

    if ads.empty:
        st.info("광고 데이터가 없어 ROAS 계산 불가")
    else:
        ads_filtered = ads[
            (ads["date"].dt.date >= start_date)
            & (ads["date"].dt.date <= max_date)
        ].copy()
        brand_ad_spend = ads_filtered["spend"].sum()
        brand_rev = filtered["revenue"].sum()

        prod_agg = (
            filtered.groupby("product")
            .agg(매출=("revenue", "sum"), 판매수량=("quantity", "sum"))
            .reset_index()
        )

        def _est_spend(row):
            if brand_rev <= 0:
                return 0
            return brand_ad_spend * (row["매출"] / brand_rev)

        prod_agg["추정광고비"] = prod_agg.apply(_est_spend, axis=1)
        prod_agg["ROAS"] = np.where(
            prod_agg["추정광고비"] > 0,
            prod_agg["매출"] / prod_agg["추정광고비"],
            np.nan,
        )
        # 광고비 거의 안 쓰는데 매출 나는 SKU = 오가닉 강자
        prod_agg["오가닉여부"] = (
            prod_agg["추정광고비"] < (prod_agg["매출"] * 0.01)
        )
        prod_agg = prod_agg.sort_values("매출", ascending=False).head(20)

        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**🏆 매출 TOP 8**")
            top = prod_agg.nlargest(8, "매출")[
                ["product", "매출", "판매수량", "ROAS"]
            ].copy()
            top["매출"] = top["매출"].apply(lambda v: f"₩{int(v):,}")
            top["판매수량"] = top["판매수량"].apply(lambda v: f"{int(v):,}")
            top["ROAS"] = top["ROAS"].apply(
                lambda v: f"{v:,.1f}x" if pd.notna(v) and v > 0 else "—"
            )
            st.dataframe(
                top, hide_index=True, width="stretch",
                column_config={
                    "product": st.column_config.TextColumn("제품명", width="large"),
                },
            )

        with col_r:
            st.markdown("**🌱 오가닉 강자**")
            organic = prod_agg[prod_agg["오가닉여부"]].nlargest(8, "매출")[
                ["product", "매출", "판매수량"]
            ].copy()
            if organic.empty:
                st.caption("(추정 광고비 0원인 SKU 없음)")
            else:
                organic["매출"] = organic["매출"].apply(lambda v: f"₩{int(v):,}")
                organic["판매수량"] = organic["판매수량"].apply(
                    lambda v: f"{int(v):,}"
                )
                st.dataframe(
                    organic, hide_index=True, width="stretch",
                    column_config={
                        "product": st.column_config.TextColumn(
                            "제품명", width="large",
                        ),
                    },
                )

        st.caption(
            "ℹ️ ROAS 는 브랜드 광고비를 매출 비중으로 배분한 추정치. "
            "오가닉 강자는 신 SKU 확장 시 광고 없이도 자력 판매 가능성 높음."
        )


# ==========================================================
# 하단 — 활용 가이드
# ==========================================================
st.markdown("---")
with st.expander("📘 이 보드 활용법 (초안 — 피드백 받습니다)", expanded=False):
    st.markdown("""
**① 카테고리 분포**
- 김(13 SKU) 위주 → 떡뻥(9개) / 번들(1개) 라인 확장 여지
- SKU 적은데 매출 큰 카테고리 = ROI 최대

**② 맛 선호도**
- 담백한맛 압도 → 신 SKU 도 담백한맛 위주가 안전
- 매출비중 3% 미만 맛 = 단종 또는 마케팅 강화 후보

**③ 번들 vs 단품**
- 번들 SKU 적은데 객단가 ↑ → 번들 라인 확장 검토
- 단품 합 vs 번들 매출 비교로 카니발리제이션 risk 확인

**④ ROAS**
- 오가닉 강자 = 광고 없이도 잘 팔리는 SKU → 확장 우선순위
- 광고 의존도 높은 SKU = 광고 끊으면 매출 떨어질 risk

**📌 점진적 개선 후보**
- 연령대 매칭 (4~6m / 7~12m / 12~24m) — 제품명 키워드 추가 필요
- 구독 전환율 — 정기구독 데이터 컬럼 추가 필요
- 재구매 사이클 — customer_id 채널 통합 후 분석 가능
""")
