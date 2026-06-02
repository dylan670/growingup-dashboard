"""재고관리 — 이지어드민 업로드 기반 통합 재고 화면.

Ozkiz BrandBoard '재고관리' 페이지 미러:
  - 상단 KPI (총 재고 / 본사 vs 매장 비중 등)
  - 카테고리별 재고총액 막대 + 시즌(브랜드) 재고 비율 도넛
  - 검색 + 상세 표 (재고 정렬 + 무이동 필터)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.ui import (
    setup_page, BRAND_COLORS, TEXT_MAIN, TEXT_MUTED, TEXT_FAINT,
)
from api.easyadmin_csv import load_inventory


setup_page(
    page_title="재고관리",
    page_icon="🏪",
    header_title="🏪 재고관리",
    header_subtitle="재고량·재고액 추이와 회전율을 추적하고 무이동 재고를 가려냅니다",
)


def _flatten_html(html: str) -> str:
    return "".join(ln.strip() for ln in html.strip().split("\n"))


inv = load_inventory()

if inv.empty:
    st.markdown(
        _flatten_html("""
<div style="background:#fef3c7; border:1px solid #fcd34d; border-radius:10px; padding:20px 24px; margin-top:20px;">
    <div style="font-size:1.05rem; font-weight:700; color:#78350f;">📥 재고 데이터 없음</div>
    <div style="font-size:0.88rem; color:#92400e; margin-top:8px;">
        <b>📤 CSV 업로드 → 📦 이지어드민 재고</b> 탭에서 이지어드민 재고 파일을 업로드하세요.
    </div>
</div>
        """),
        unsafe_allow_html=True,
    )
    st.stop()


# ============================================================
# 상단 KPI
# ============================================================
total_qty = int(inv["stock"].sum())
total_value = int((inv["stock"] * inv["price"]).sum())
sku_count = len(inv)
brands = inv["brand"].value_counts().to_dict()
incoming_total = int(inv["incoming"].sum())
critical = int((inv["days_left"] <= 7).sum())

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("📦 현재 재고", f"{total_qty:,}개", f"{sku_count} SKU")
k2.metric("💰 재고액 (판매가)", f"₩{total_value:,}")
k3.metric("📥 입고 예정", f"{incoming_total:,}개")
k4.metric("🚨 7일 내 품절", f"{critical}건")
k5.metric("🏷 브랜드", f"{len(brands)}개")

st.markdown("---")


# ============================================================
# 2단 차트 — 브랜드별 재고 비율 도넛 + 카테고리별 재고총액 막대
# ============================================================
chart_l, chart_r = st.columns(2)

with chart_l:
    st.markdown(
        _flatten_html("""
<div style="font-size:1rem; font-weight:700; color:#0f172a; margin-bottom:2px;">브랜드별 재고 비율</div>
<div style="font-size:0.78rem; color:#94a3b8; margin-bottom:12px;">재고액 (재고수량 × 판매가) 기준</div>
        """),
        unsafe_allow_html=True,
    )

    by_brand = inv.copy()
    by_brand["value"] = by_brand["stock"] * by_brand["price"]
    brand_agg = by_brand.groupby("brand")["value"].sum().sort_values(ascending=False)
    if not brand_agg.empty:
        colors = [
            BRAND_COLORS.get(b, {}).get("primary", "#94a3b8")
            for b in brand_agg.index
        ]
        fig = go.Figure(go.Pie(
            labels=brand_agg.index.tolist(),
            values=brand_agg.values.tolist(),
            hole=0.62,
            marker=dict(colors=colors, line=dict(color="white", width=2)),
            textinfo="label+percent",
            textposition="outside",
            sort=False,
        ))
        total_val = int(brand_agg.sum())
        fig.update_layout(
            height=340, margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            annotations=[dict(
                text=(
                    f"<span style='font-size:0.72rem; color:#94a3b8;'>총 재고액</span><br>"
                    f"<span style='font-size:1.0rem; font-weight:700; color:#0f172a;'>"
                    f"₩{total_val:,}</span>"
                ),
                x=0.5, y=0.5, showarrow=False,
            )],
        )
        st.plotly_chart(fig, use_container_width=True)

with chart_r:
    st.markdown(
        _flatten_html("""
<div style="font-size:1rem; font-weight:700; color:#0f172a; margin-bottom:2px;">카테고리별 재고총액</div>
<div style="font-size:0.78rem; color:#94a3b8; margin-bottom:12px;">상위 12개</div>
        """),
        unsafe_allow_html=True,
    )

    cat_agg = (
        by_brand.groupby("category")["value"].sum()
        .sort_values(ascending=False).head(12)
    )
    if not cat_agg.empty:
        fig = px.bar(
            x=cat_agg.index.tolist(),
            y=cat_agg.values.tolist(),
            color=cat_agg.values.tolist(),
            color_continuous_scale=["#fef3c7", "#f59e0b", "#b45309"],
        )
        fig.update_layout(
            height=340, margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(title="", tickangle=-30, tickfont=dict(size=10)),
            yaxis=dict(title="재고액 (원)", tickformat=",",
                       showgrid=True, gridcolor="#f1f5f9"),
            plot_bgcolor="white",
            showlegend=False,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ============================================================
# 상세 표 — 검색 + 정렬
# ============================================================
st.markdown("### 🔍 재고 상세")

search_q = st.text_input(
    "상품명 · SKU · 옵션 검색", placeholder="검색어 입력...",
    label_visibility="collapsed",
)

display = inv.copy()
display["value"] = display["stock"] * display["price"]
display = display.sort_values("value", ascending=False)

if search_q.strip():
    q = search_q.strip().lower()
    mask = (
        display["product"].astype(str).str.lower().str.contains(q, na=False)
        | display["sku"].astype(str).str.lower().str.contains(q, na=False)
        | display["option"].astype(str).str.lower().str.contains(q, na=False)
    )
    display = display[mask]

st.caption(f"전체 {len(inv)}건 중 **{len(display)}건** 표시")

# 표 — 핵심 컬럼만
display_show = display[[
    "sku", "brand", "category", "product", "option",
    "stock", "incoming", "safety_stock", "days_left", "price", "value",
]].copy()
display_show.columns = [
    "SKU", "브랜드", "카테고리", "상품명", "옵션",
    "재고", "입고예정", "안전재고", "소진예상(일)", "판매가", "재고액",
]
display_show["판매가"] = display_show["판매가"].apply(lambda v: f"₩{int(v):,}")
display_show["재고액"] = display_show["재고액"].apply(lambda v: f"₩{int(v):,}")
display_show["소진예상(일)"] = display_show["소진예상(일)"].apply(
    lambda v: f"{int(v)}일" if v < 9999 else "—"
)

st.dataframe(
    display_show, hide_index=True, width="stretch",
    height=min(600, 60 + len(display_show) * 36),
)
