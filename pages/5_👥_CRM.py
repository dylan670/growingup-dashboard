"""CRM (재구매 트래커) — 브랜드 탭 × 스토어 필터 × 재구매 리마인더 대상."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.data import load_orders
from utils.metrics import calc_repurchase
from utils.products import (
    filter_orders_by_brand, BRAND_ORDER_STORES, store_display_name,
)
from utils.ui import setup_page


setup_page(
    page_title="CRM",
    page_icon="👥",
    header_title="👥 재구매 트래커 (CRM)",
    header_subtitle="브랜드별 재구매 주기 관리 — 유아식 30일 · 여행용품 90일 권장",
)

orders = load_orders()


def render_crm(
    orders_df: pd.DataFrame,
    brand: str | None = None,
    default_cycle: int = 30,
):
    """브랜드별 CRM 렌더링."""
    brand_label = brand if brand else "전체"

    # ---------- 필터: 스토어 + 재구매 주기 ----------
    has_store = "store" in orders_df.columns
    # 선택지: 내부 store 값 리스트, 표시용 라벨은 format_func 으로
    store_options_raw: list[str | None] = [None]  # None = 전체
    if has_store:
        if brand:
            brand_stores = BRAND_ORDER_STORES.get(brand, [])
            available = [s for s in brand_stores
                         if s in orders_df["store"].dropna().unique().tolist()]
        else:
            available = sorted(orders_df["store"].dropna().unique().tolist())
        store_options_raw += available

    def _fmt_store(s):
        if s is None:
            return "전체"
        return store_display_name(s, brand_context=brand)

    c_flt, c_cyc = st.columns([1, 2])
    with c_flt:
        selected_store_raw = st.selectbox(
            "스토어 필터", store_options_raw,
            format_func=_fmt_store,
            key=f"crm_store_{brand_label}",
            help="탭 내에서 특정 스토어로 더 좁혀 볼 수 있습니다.",
        )
        selected_store = _fmt_store(selected_store_raw)
    with c_cyc:
        cycle = st.slider(
            "재구매 주기 (일)", 14, 180,
            default_cycle, 1,
            key=f"crm_cycle_{brand_label}",
            help=(
                "똑똑연구소: 30일 권장 (떡뻥/김 주기). "
                "롤라루: 90~180일 권장 (여행용품 반복구매 주기)."
            ),
        )

    filtered = orders_df.copy()
    if selected_store_raw is not None and has_store:
        filtered = filtered[filtered["store"] == selected_store_raw]

    if len(filtered) == 0:
        st.warning(f"'{brand_label} · {selected_store}' 범위에 주문 데이터가 없습니다.")
        return

    cust = calc_repurchase(filtered, cycle_days=cycle)

    active = cust[~cust["cycle_reached"] & ~cust["churned"]]
    reached = cust[cust["cycle_reached"]]
    churned = cust[cust["churned"]]

    # ---------- 고객 세그먼트 KPI ----------
    st.markdown(f"#### 📊 고객 세그먼트 · {brand_label} / {selected_store}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("전체 고객", f"{len(cust):,}명")
    c2.metric(f"활성 ({cycle}일 이내)", f"{len(active):,}명")
    c3.metric(f"재구매 타이밍 ({cycle}~{cycle * 2}일)",
              f"{len(reached):,}명", delta="CRM 대상")
    c4.metric(f"이탈 위험 ({cycle * 2}일+)", f"{len(churned):,}명")

    repurchaser_rate = cust["is_repurchaser"].mean() * 100 if len(cust) > 0 else 0
    c1, c2 = st.columns(2)
    c1.metric(
        "재구매율 (2회 이상)", f"{repurchaser_rate:.1f}%",
        help="유아동 간식 30~50%가 건강 · 여행용품은 10~20% 가 현실적 목표",
    )
    c2.metric("고객당 평균 주문", f"{cust['total_orders'].mean():.2f}회")

    st.divider()

    # ---------- 스토어별 재구매율 (전체 보기 시만) ----------
    if selected_store_raw is None and has_store and cust["store"].nunique() > 1:
        st.markdown(f"#### 🏪 {brand_label} 스토어별 재구매율")
        by_store = cust.groupby("store").agg(
            customers=("customer_id", "count"),
            repurchasers=("is_repurchaser", "sum"),
            avg_orders=("total_orders", "mean"),
            avg_revenue=("total_revenue", "mean"),
        ).reset_index()
        by_store["repurchase_rate"] = (
            by_store["repurchasers"] / by_store["customers"] * 100
        ).round(1)
        by_store["avg_orders"] = by_store["avg_orders"].round(2)
        by_store["avg_revenue"] = by_store["avg_revenue"].round(0)
        # 표시용 라벨 컬럼 추가 (차트/표 모두에 사용)
        by_store["스토어"] = by_store["store"].apply(
            lambda s: store_display_name(s, brand_context=brand)
        )

        col1, col2 = st.columns([2, 3])
        with col1:
            fig = px.bar(
                by_store, x="스토어", y="repurchase_rate",
                labels={"스토어": "스토어", "repurchase_rate": "재구매율 (%)"},
                color="repurchase_rate", color_continuous_scale="Blues",
            )
            fig.update_layout(height=320, showlegend=False,
                              coloraxis_showscale=False,
                              margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, width="stretch",
                            key=f"crm_chart_{brand_label}")
        with col2:
            st.dataframe(
                by_store.drop(columns=["store"]).rename(columns={
                    "customers": "고객수",
                    "repurchasers": "재구매자", "avg_orders": "평균주문",
                    "avg_revenue": "평균매출(원)", "repurchase_rate": "재구매율(%)",
                }),
                width="stretch", hide_index=True,
            )
        st.divider()

    # ---------- CRM 리마인더 대상 ----------
    st.markdown(f"#### 📨 CRM 리마인더 대상 ({len(reached):,}명)")
    if len(reached) > 0:
        expected = int(reached["total_revenue"].mean() * len(reached) * 0.3)
        st.success(f"예상 재구매 매출: 약 **{expected:,}원** (30% 전환 가정)")

        display_cols = ["customer_id", "days_since_last",
                        "total_orders", "total_revenue", "last_order"]
        rename_map = {
            "customer_id": "고객ID",
            "days_since_last": "경과일",
            "total_orders": "총주문",
            "total_revenue": "총매출(원)",
            "last_order": "마지막주문",
        }
        reached_out = reached.copy()
        if has_store and selected_store_raw is None:
            reached_out["스토어"] = reached_out["store"].apply(
                lambda s: store_display_name(s, brand_context=brand)
            )
            display_cols.insert(1, "스토어")

        display = reached_out.sort_values("days_since_last")[display_cols].rename(columns=rename_map)
        st.dataframe(display, width="stretch", hide_index=True)

        csv = display.to_csv(index=False, encoding="utf-8-sig")
        fn = f"crm_{brand_label}_{selected_store}_{cycle}d.csv"
        st.download_button(
            "CRM 대상 CSV 다운로드", csv, fn, "text/csv",
            key=f"crm_dl_{brand_label}",
        )

        st.info(
            "**활용 방법:** 이 CSV 를 카카오 알림톡 / 이메일 마케팅 (스티비, 메일침프 등) / "
            "자사몰 CRM 기능에 업로드해 재구매 쿠폰 + 신제품 소식을 발송. "
            "30% 는 보수적 가정이며, 쿠폰 제공 시 40~50% 전환율도 기대 가능."
        )
    else:
        st.info(f"'{brand_label} · {selected_store}' 에 현재 {cycle}~{cycle * 2}일 경과 고객이 없습니다.")


# ==========================================================
# 브랜드 탭
# ==========================================================
tab_all, tab_ddok, tab_rolla = st.tabs([
    "📊 전체", "🍙 똑똑연구소", "🧳 롤라루",
])

with tab_all:
    render_crm(orders, brand=None, default_cycle=30)

with tab_ddok:
    render_crm(
        filter_orders_by_brand(orders, "똑똑연구소"),
        brand="똑똑연구소", default_cycle=30,
    )

with tab_rolla:
    render_crm(
        filter_orders_by_brand(orders, "롤라루"),
        brand="롤라루", default_cycle=90,
    )
