"""입고관리 — 발주요청 / 생산중 / 금주·차주 입고 / 쿠팡 발주.

Ozkiz BrandBoard '입고 현황' 페이지 미러:
  - 상단 KPI 5개 (발주요청 / 생산중 / 금주 입고 / 차주 입고 / 쿠팡 발주)
  - 금주 제품별 입고량 막대 차트
  - 제품 리스트 (수량 / 비중)
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.ui import setup_page, BRAND_COLORS
from api.easyadmin_csv import load_inventory


setup_page(
    page_title="입고관리",
    page_icon="📥",
    header_title="📥 입고관리",
    header_subtitle="발주요청 · 생산중 · 금주 입고 · 차주 입고 · 쿠팡 발주",
)


def _flatten_html(html: str) -> str:
    return "".join(ln.strip() for ln in html.strip().split("\n"))


inv = load_inventory()

if inv.empty or "incoming" not in inv.columns:
    st.markdown(
        _flatten_html("""
<div style="background:#fef3c7; border:1px solid #fcd34d; border-radius:10px; padding:20px 24px; margin-top:20px;">
    <div style="font-size:1.05rem; font-weight:700; color:#78350f;">📥 재고 데이터 없음</div>
    <div style="font-size:0.88rem; color:#92400e; margin-top:8px;">
        <b>📤 CSV 업로드 → 📦 이지어드민 재고</b> 탭에서 이지어드민 재고 파일을 업로드하세요.<br>
        입고관리는 이지어드민의 '입고대기' 컬럼을 기반으로 동작합니다.
    </div>
</div>
        """),
        unsafe_allow_html=True,
    )
    st.stop()

# 입고 예정만 추출 (incoming > 0)
incoming_df = inv[inv["incoming"] > 0].copy()
incoming_df = incoming_df.sort_values("incoming", ascending=False)


# ============================================================
# 검색
# ============================================================
search_q = st.text_input(
    "🔍 제품명 · 상품코드 · 시즌 · 유형 검색",
    placeholder="검색어 입력...",
    label_visibility="collapsed",
)
if search_q.strip():
    q = search_q.strip().lower()
    mask = (
        incoming_df["product"].astype(str).str.lower().str.contains(q, na=False)
        | incoming_df["sku"].astype(str).str.lower().str.contains(q, na=False)
        | incoming_df["category"].astype(str).str.lower().str.contains(q, na=False)
    )
    incoming_df = incoming_df[mask]


# ============================================================
# 상단 KPI 5개 (Ozkiz 패턴 — 발주요청 / 생산중 / 금주 / 차주 / 쿠팡)
# ============================================================
# 단순화: incoming 의 절대값으로 그룹 분류
# 이지어드민에는 진행 단계가 없어서 임의 분류 (실 운영 시 발주 일정 컬럼 필요)
total_incoming_qty = int(incoming_df["incoming"].sum())
total_incoming_sku = len(incoming_df)
estimated_value = int((incoming_df["incoming"] * incoming_df["price"]).sum())

# 브랜드별 합계
brand_incoming = (
    incoming_df.groupby("brand")["incoming"].sum().sort_values(ascending=False)
)


def _kpi_card(icon: str, label: str, value: str, sub: str = "") -> str:
    return _flatten_html(f"""
<div style="background:white; border:1px solid #e2e8f0; border-radius:14px; padding:16px 18px; box-shadow:0 1px 2px rgba(15,23,42,0.03);">
    <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:10px;">
        <div style="font-size:0.78rem; color:#64748b; font-weight:600;">{icon} {label}</div>
        <div style="font-size:0.68rem; color:#94a3b8;">클릭하여 보기</div>
    </div>
    <div style="font-size:1.7rem; font-weight:800; color:#0f172a; line-height:1;">{value}</div>
    <div style="font-size:0.72rem; color:#94a3b8; margin-top:5px;">{sub}</div>
</div>
    """)


cols = st.columns(5)
cols[0].markdown(
    _kpi_card("📋", "발주요청", f"{total_incoming_sku}개",
              f"{total_incoming_qty:,}수량 · ₩{estimated_value/10000:.0f}만원"),
    unsafe_allow_html=True,
)

# 브랜드별 분할
for i, (brand, qty) in enumerate(brand_incoming.head(4).items()):
    qty_int = int(qty)
    sku_n = int((incoming_df["brand"] == brand).sum())
    brand_value = int(
        (incoming_df[incoming_df["brand"] == brand]["incoming"]
         * incoming_df[incoming_df["brand"] == brand]["price"]).sum()
    )
    bc = BRAND_COLORS.get(brand, {})
    emoji = "🏪"
    if brand == "롤라루":
        emoji = "🧳"
    elif brand == "똑똑연구소":
        emoji = "🍙"
    elif brand == "루티니스트":
        emoji = "👟"
    cols[i + 1].markdown(
        _kpi_card(emoji, brand, f"{sku_n}개",
                  f"{qty_int:,}수량 · ₩{brand_value/10000:.0f}만원"),
        unsafe_allow_html=True,
    )

st.markdown("---")


# ============================================================
# 금주 제품별 입고량 (상위 12)
# ============================================================
st.markdown("### 📊 입고 예정 제품별 수량")
st.caption(f"입고대기 수량 기준 상위 12 · 총 {total_incoming_qty:,}개 · ₩{estimated_value:,}원")

top12 = incoming_df.head(12).copy()
top12["display"] = top12["product"].fillna("") + " · " + top12["option"].fillna("").str[:15]

if not top12.empty:
    fig = px.bar(
        x=top12["display"].tolist(),
        y=top12["incoming"].tolist(),
        color=top12["brand"].tolist(),
        color_discrete_map={
            b: BRAND_COLORS.get(b, {}).get("primary", "#94a3b8")
            for b in top12["brand"].unique()
        },
    )
    fig.update_layout(
        height=380, margin=dict(l=10, r=10, t=10, b=80),
        xaxis=dict(title="", tickangle=-35, tickfont=dict(size=10)),
        yaxis=dict(title="입고 예정 수량", tickformat=",",
                   showgrid=True, gridcolor="#f1f5f9"),
        plot_bgcolor="white",
        legend=dict(title="", orientation="h", y=1.08),
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# 제품 리스트
# ============================================================
st.markdown("### 📋 제품 리스트")

display_show = incoming_df[[
    "sku", "brand", "category", "product", "option",
    "stock", "incoming", "price",
]].copy()
display_show.columns = [
    "SKU", "브랜드", "카테고리", "상품명", "옵션",
    "현재고", "입고예정", "판매가",
]
display_show["입고예정금액"] = (
    incoming_df["incoming"] * incoming_df["price"]
).astype(int).apply(lambda v: f"₩{v:,}")
display_show["판매가"] = display_show["판매가"].apply(lambda v: f"₩{int(v):,}")
display_show["비중"] = (
    incoming_df["incoming"] / total_incoming_qty * 100
    if total_incoming_qty > 0 else 0
).round(1).apply(lambda v: f"{v}%")

st.dataframe(
    display_show, hide_index=True, width="stretch",
    height=min(600, 60 + len(display_show) * 36),
)
