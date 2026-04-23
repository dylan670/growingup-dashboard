"""알림 센터 — 브랜드 탭(전체/똑똑연구소/롤라루) × 자동 감지 신호 모음."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.data import load_ads, load_orders
from utils.alerts import check_all_alerts
from utils.products import filter_ads_by_brand, filter_orders_by_brand
from utils.ui import setup_page


setup_page(
    page_title="알림 센터",
    page_icon="🚦",
    header_title="🚦 알림 센터",
    header_subtitle="광고·매출 데이터 자동 감지 의사결정 신호 (브랜드별 분리)",
)

ads = load_ads()
orders = load_orders()


def render_alerts(
    ads_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    brand: str | None = None,
):
    """브랜드별 알림 렌더링."""
    brand_label = brand if brand else "전체"
    alerts = check_all_alerts(ads_df, orders_df)

    if not alerts:
        st.success(f"{brand_label}: 현재 감지된 알림이 없습니다. 좋은 상태를 유지 중입니다.")
        return

    urgent = [a for a in alerts if a["severity"] == "urgent"]
    opportunity = [a for a in alerts if a["severity"] == "opportunity"]
    info = [a for a in alerts if a["severity"] == "info"]

    c1, c2, c3 = st.columns(3)
    c1.metric("🔴 긴급 대응", f"{len(urgent)}건")
    c2.metric("🟢 기회 포착", f"{len(opportunity)}건")
    c3.metric("🔵 참고", f"{len(info)}건")

    st.divider()

    if urgent:
        st.markdown(f"#### 🔴 {brand_label} 긴급 대응 필요")
        for a in urgent:
            with st.container(border=True):
                st.error(f"**{a['title']}**")
                st.write(a["message"])
                st.caption(f"근거: {a['evidence']}")

    if opportunity:
        st.markdown(f"#### 🟢 {brand_label} 기회 포착")
        for a in opportunity:
            with st.container(border=True):
                st.success(f"**{a['title']}**")
                st.write(a["message"])
                st.caption(f"근거: {a['evidence']}")

    if info:
        st.markdown(f"#### 🔵 {brand_label} 참고")
        for a in info:
            with st.container(border=True):
                st.info(f"**{a['title']}**")
                st.write(a["message"])
                st.caption(f"근거: {a['evidence']}")


# ==========================================================
# 브랜드 탭
# ==========================================================
tab_all, tab_ddok, tab_rolla = st.tabs([
    "📊 전체", "🍙 똑똑연구소", "🧳 롤라루",
])

with tab_all:
    render_alerts(ads, orders, brand=None)

with tab_ddok:
    render_alerts(
        filter_ads_by_brand(ads, "똑똑연구소"),
        filter_orders_by_brand(orders, "똑똑연구소"),
        brand="똑똑연구소",
    )

with tab_rolla:
    render_alerts(
        filter_ads_by_brand(ads, "롤라루"),
        filter_orders_by_brand(orders, "롤라루"),
        brand="롤라루",
    )


# ==========================================================
# 감지 룰 목록
# ==========================================================
st.divider()
st.markdown("#### 📋 감지 룰 목록")
st.markdown("""
| 룰 | 감지 조건 | 제안 액션 |
|----|-----------|-----------|
| **예산 증액** | 채널 ROAS 3일 연속 목표 × 1.3 이상 + 전환 증가 | 해당 채널 20% 증액 검토 |
| **예산 축소** | 채널 ROAS 3일 연속 목표 미달 | 소재 교체 또는 일시 정지 |
| **소재 피로도** | 최근 7일 CTR 이 직전 7일 대비 30%+ 하락 | 크리에이티브 교체 |
| **채널 재배분** | 채널 간 목표 대비 격차 100%p 이상 | 부진 채널 → 우량 채널로 예산 이동 |
| **재구매 기회** | 지난 주문 후 30~60일 경과 고객 존재 | CRM 리마인더 발송 |
| **주문 감소** | 최근 7일 주문수 직전 7일 대비 20%+ 감소 | 외부 요인/광고 효율 점검 |
""")

st.caption("임계값은 `utils/alerts.py` 에서 직접 수정해 실제 운영 수치에 맞출 수 있습니다.")
