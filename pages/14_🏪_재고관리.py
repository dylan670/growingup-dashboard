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
    format_won_compact,
)
from api.easyadmin_csv import load_inventory, value_basis


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
_vcol, _vlabel = value_basis(inv)
total_qty = int(inv["stock"].sum())
total_value = int((inv["stock"] * inv[_vcol]).sum())
sku_count = len(inv)
brands = inv["brand"].value_counts().to_dict()
incoming_total = int(inv["incoming"].sum())
critical = int((inv["days_left"] <= 7).sum())

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("📦 현재 재고", f"{total_qty:,}개", f"{sku_count} SKU")
k2.metric(f"💰 재고액 ({_vlabel})", format_won_compact(total_value),
          help=f"정확한 금액: ₩{total_value:,}")
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
        _flatten_html(f"""
<div style="font-size:1rem; font-weight:700; color:#0f172a; margin-bottom:2px;">브랜드별 재고 비율</div>
<div style="font-size:0.78rem; color:#94a3b8; margin-bottom:12px;">재고액 (재고수량 × {_vlabel}) 기준</div>
        """),
        unsafe_allow_html=True,
    )

    by_brand = inv.copy()
    by_brand["value"] = by_brand["stock"] * by_brand[_vcol]
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
# 재고 베스트 10 — 이미지 카드 (재고액 상위, 옵션 포함)
# ============================================================
if not inv.empty:
    st.markdown("### 📦 재고 베스트 10")
    st.caption(f"재고액 (재고수량 × {_vlabel}) 기준 상위 10 · 회수·할인 후보")

    from utils.product_images import load_image_cache, find_image_by_brand
    _img_cache = load_image_cache()

    def _flat14(h: str) -> str:
        return "".join(ln.strip() for ln in h.strip().split("\n"))

    _inv_top = inv.copy()
    _inv_top["value"] = _inv_top["stock"] * _inv_top[_vcol]
    _inv_top = _inv_top.sort_values("value", ascending=False).head(10)

    _ncol = 5
    for _ri in range(0, len(_inv_top), _ncol):
        _scols = st.columns(_ncol)
        for _ci, (_, _sku) in enumerate(
            _inv_top.iloc[_ri:_ri + _ncol].iterrows()
        ):
            _rank = _ri + _ci + 1
            _prod = str(_sku["product"])
            _brand = _sku.get("brand", "기타")
            _bc = BRAND_COLORS.get(_brand, {})
            _pri = _bc.get("primary", "#64748b")
            _soft = _bc.get("bg_soft", "#f8fafc")
            _txtc = _bc.get("text", "#0f172a")
            try:
                _url = find_image_by_brand(
                    _prod, _brand, _img_cache, min_ratio=0.5) or ""
            except Exception:
                _url = ""
            _img = (
                f'<img src="{_url}" style="width:100%; height:110px; '
                f'object-fit:cover; border-radius:8px;" />'
                if _url else
                f'<div style="width:100%; height:110px; background:{_soft}; '
                f'border-radius:8px; display:flex; align-items:center; '
                f'justify-content:center; font-size:1.8rem; '
                f'color:{_pri};">📦</div>'
            )
            _opt = str(_sku.get("option", "") or "").strip()
            if _opt.lower() in ("nan", "none"):
                _opt = ""
            _ps = _prod[:20] + "..." if len(_prod) > 20 else _prod
            _os = _opt[:26] + "..." if len(_opt) > 26 else _opt
            _oh = (
                f'<div style="font-size:0.66rem; color:#64748b; '
                f'line-height:1.2; height:16px; overflow:hidden;" '
                f'title="{_opt}">🔖 {_os}</div>'
                if _opt else '<div style="height:16px;"></div>'
            )
            with _scols[_ci]:
                st.markdown(_flat14(f"""
<div style="background:white; border:1px solid #e2e8f0; border-radius:12px; padding:10px; min-height:236px; box-shadow:0 1px 3px rgba(15,23,42,0.04);">
    <div style="position:relative; margin-bottom:8px;">
        {_img}
        <div style="position:absolute; top:6px; left:6px; background:#f59e0b; color:white; border-radius:999px; width:24px; height:24px; display:flex; align-items:center; justify-content:center; font-size:0.74rem; font-weight:700;">{_rank}</div>
    </div>
    <div style="font-size:0.66rem; color:{_txtc}; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;">{_brand}</div>
    <div style="font-size:0.74rem; color:#0f172a; font-weight:600; line-height:1.3; margin-top:3px; height:30px; overflow:hidden;" title="{_prod}">{_ps}</div>
    {_oh}
    <div style="margin-top:6px; padding-top:6px; border-top:1px solid #f1f5f9;">
        <div style="display:flex; justify-content:space-between; font-size:0.7rem;">
            <span style="color:#94a3b8;">재고</span>
            <span style="color:{_txtc}; font-weight:700;">{int(_sku['stock']):,}개</span>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-top:2px;">
            <span style="color:#94a3b8;">재고액</span>
            <span style="color:#b45309; font-weight:700;">₩{int(_sku['value']/10000):,}만</span>
        </div>
    </div>
</div>
                """), unsafe_allow_html=True)

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
display["value"] = display["stock"] * display[_vcol]
display = display.sort_values("value", ascending=False)

if search_q.strip():
    q = search_q.strip().lower()
    mask = (
        display["product"].astype(str).str.lower().str.contains(q, na=False)
        | display["sku"].astype(str).str.lower().str.contains(q, na=False)
        | display["option"].astype(str).str.lower().str.contains(q, na=False)
    )
    display = display[mask]

st.caption(f"전체 {len(inv)}건 중 **{len(display)}건** 표시 · 재고액 = 재고수량 × {_vlabel}")

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
