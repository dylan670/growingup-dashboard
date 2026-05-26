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
    page_title="SKU 확장 보드",
    page_icon="🎯",
    header_title="🎯 SKU 확장 의사결정 보드",
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

# 브랜드 선택
brand_options = ["전체"] + sorted(orders["brand"].unique().tolist())
selected_brand = st.sidebar.selectbox(
    "브랜드",
    brand_options,
    index=brand_options.index("롤라루") if "롤라루" in brand_options else 0,
    help="롤라루(캐리어)가 옵션 데이터가 가장 풍부합니다",
)

# 기간 필터
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
k1.metric("📅 기간 매출", f"₩{total_rev/1e6:.1f}M")
k2.metric("📦 총 판매수량", f"{int(total_qty):,}")
k3.metric("🏷 판매 SKU", f"{unique_products}")
k4.metric("🎨 판매 옵션", f"{unique_options}")
k5.metric("💰 평균 단가", f"₩{avg_price:,.0f}")

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
        selected_opt_key = st.selectbox(
            "분석할 옵션",
            opt_key_labels,
            index=opt_key_labels.index("컬러") if "컬러" in opt_key_labels else 0,
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
                height=max(350, top_n * 30),
                margin=dict(l=10, r=10, t=30, b=10),
                showlegend=False,
                coloraxis_showscale=False,
            )
            fig.update_traces(
                texttemplate="%{text}%",
                textposition="outside",
                hovertemplate="%{y}<br>매출: ₩%{x:,.0f}<br>비중: %{text}%",
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
            f"{b/1000:.0f}~{(b+bin_width)/1000:.0f}K"
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
            xaxis=dict(title="가격대 (1K=1천원)"),
            yaxis=dict(title="매출", side="left"),
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
                lambda v: f"₩{v/1e6:.1f}M"
            )
            opp_display["SKU당평균매출"] = opp_display["SKU당평균매출"].apply(
                lambda v: f"₩{v/1e6:.1f}M"
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
                f"SKU 당 평균 매출 ₩{top_opp['SKU당평균매출']/1e6:.1f}M. "
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
            top["매출"] = top["매출"].apply(lambda v: f"₩{v/1e6:.2f}M")
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
                    lambda v: f"₩{v/1e6:.2f}M"
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
# TAB 4 — 옵션 조합 매트릭스 (컬러 X 사이즈 등)
# ==========================================================
with tab_combo:
    st.markdown("##### 🧩 옵션 조합 매트릭스 — '잘 팔리는 조합' 발견")
    st.caption("두 옵션을 교차해서 어떤 조합이 가장 잘 팔리는지 시각화")

    expanded = _expand_options(filtered)
    opt_cols = [c for c in expanded.columns if c.startswith("opt_")]

    if len(opt_cols) < 2:
        st.info("두 개 이상의 옵션 키가 있는 데이터가 필요합니다.")
    else:
        opt_labels = [c.replace("opt_", "") for c in opt_cols]
        c1, c2 = st.columns(2)
        with c1:
            x_key = st.selectbox(
                "X축 옵션",
                opt_labels,
                index=opt_labels.index("컬러") if "컬러" in opt_labels else 0,
            )
        with c2:
            remaining = [l for l in opt_labels if l != x_key]
            y_key = st.selectbox(
                "Y축 옵션",
                remaining,
                index=remaining.index("사이즈") if "사이즈" in remaining else 0,
            )

        x_col, y_col = f"opt_{x_key}", f"opt_{y_key}"
        combo_df = expanded[
            (expanded[x_col] != "") & (expanded[y_col] != "")
        ].copy()

        if combo_df.empty:
            st.info("두 옵션 모두 값이 있는 데이터 없음")
        else:
            heatmap_data = (
                combo_df.groupby([y_col, x_col])
                .agg(매출=("revenue", "sum"))
                .reset_index()
            )
            pivot = heatmap_data.pivot(
                index=y_col, columns=x_col, values="매출",
            ).fillna(0)

            fig = px.imshow(
                pivot.values,
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                color_continuous_scale=["#ffffff", "#fef3c7", "#f59e0b", "#b45309"],
                aspect="auto",
                labels=dict(color="매출"),
            )
            # 셀에 매출 표시
            text_matrix = [
                [f"₩{v/1e6:.1f}M" if v > 0 else "" for v in row]
                for row in pivot.values
            ]
            fig.update_traces(
                text=text_matrix,
                texttemplate="%{text}",
                textfont={"size": 11},
            )
            fig.update_layout(
                height=max(350, len(pivot.index) * 50),
                margin=dict(l=10, r=10, t=30, b=10),
                xaxis=dict(title=x_key),
                yaxis=dict(title=y_key),
            )
            st.plotly_chart(fig, use_container_width=True)

            # 최고 조합 강조
            stacked = heatmap_data.sort_values("매출", ascending=False)
            top = stacked.head(5)
            if not top.empty:
                top_combo = top.iloc[0]
                st.success(
                    f"💡 **베스트 조합** — "
                    f"`{top_combo[y_col]} × {top_combo[x_col]}` "
                    f"매출 ₩{top_combo['매출']/1e6:.1f}M. "
                    f"이 조합 기반으로 비슷한 옵션 확장 검토 권장."
                )

                # Top 5 표
                with st.expander("📋 상위 5개 조합 상세", expanded=False):
                    top_display = top.copy()
                    top_display["매출"] = top_display["매출"].apply(
                        lambda v: f"₩{v/1e6:.2f}M"
                    )
                    top_display.columns = [y_key, x_key, "매출"]
                    st.dataframe(
                        top_display, hide_index=True, width="stretch",
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
