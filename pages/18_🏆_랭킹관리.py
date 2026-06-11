"""랭킹관리 — 주간/누적 베스트 SKU 통합 ranking.

3개 브랜드 통합 베스트 SKU + 재고 매칭 + 회전율.
Ozkiz BrandBoard '주간 베스트 / 재고 베스트' 페이지 미러.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.ui import setup_page, BRAND_COLORS
from api.easyadmin_csv import load_inventory, value_basis


setup_page(
    page_title="랭킹관리",
    page_icon="🏆",
    header_title="🏆 랭킹관리",
    header_subtitle="주간 베스트 · 재고 베스트 · 운영 분류별 ranking",
)


def _flatten_html(html: str) -> str:
    return "".join(ln.strip() for ln in html.strip().split("\n"))


ROOT = Path(__file__).parent.parent


# ============================================================
# 데이터 로드
# ============================================================
@st.cache_data(ttl=600)
def _load_orders() -> pd.DataFrame:
    p = ROOT / "data" / "orders.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, encoding="utf-8-sig", encoding_errors="replace")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=300)
def _load_images():
    """이미지 캐시 DataFrame (정교한 fuzzy 매칭용 — utils.product_images)."""
    from utils.product_images import load_image_cache
    return load_image_cache()


def _brand_of(p: str) -> str:
    pn = str(p).replace(" ", "")
    if any(k in pn for k in ["똑똑", "김똑똑", "떡뻥"]):
        return "똑똑연구소"
    if any(k in pn for k in ["롤라루", "캐리어", "여행", "기내용", "백팩"]):
        return "롤라루"
    if any(k in pn for k in ["루티니", "러닝", "운동조끼", "장갑"]):
        return "루티니스트"
    return "기타"


def _find_image(name: str, cache_df) -> str:
    """utils.product_images 의 fuzzy 매칭 (SKU 토큰 보정) 사용.

    단순 substring 대신 SequenceMatcher + 모델명/SKU 키워드 보정 →
    제각각인 채널별 제품명도 매칭률 상승.
    """
    from utils.product_images import find_image
    try:
        return find_image(str(name), cache_df, min_ratio=0.4) or ""
    except Exception:
        return ""


orders = _load_orders()
img_map = _load_images()
inv = load_inventory()

if orders.empty:
    st.warning("주문 데이터 없음")
    st.stop()

orders["brand"] = orders["product"].apply(_brand_of)


# ============================================================
# 기간 토글
# ============================================================
mode = st.radio(
    "기준",
    ["📅 최근 7일", "📆 최근 30일", "📊 누적 전체"],
    horizontal=True,
)

max_date = orders["date"].max()
if mode == "📅 최근 7일":
    cutoff = max_date - timedelta(days=7)
    period_days = 7
elif mode == "📆 최근 30일":
    cutoff = max_date - timedelta(days=30)
    period_days = 30
else:
    cutoff = orders["date"].min()
    period_days = (max_date - cutoff).days + 1

period_df = orders[orders["date"] >= cutoff].copy()


# ============================================================
# 상단 KPI
# ============================================================
total_rev = int(period_df["revenue"].sum())
total_qty = int(period_df["quantity"].sum())
unique_sku = period_df["product"].nunique()
avg_per_sku = total_rev / unique_sku if unique_sku else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 기간 매출", f"₩{total_rev:,}", f"{period_days}일")
k2.metric("📦 총 판매수량", f"{total_qty:,}개")
k3.metric("🏷 판매 SKU", f"{unique_sku}개")
k4.metric("📊 SKU당 평균매출", f"₩{int(avg_per_sku):,}")

st.markdown("---")


# ============================================================
# 베스트 SKU TOP 10 — 이미지 카드 그리드 (Ozkiz 스타일)
# ============================================================
st.markdown("### 🏆 베스트 SKU TOP 10")
st.caption(f"{mode} · 판매수량 기준 · 브랜드별 색상 구분")

top10 = (
    period_df.groupby("product")
    .agg(qty=("quantity", "sum"), rev=("revenue", "sum"),
         orders=("order_id", "nunique"))
    .reset_index()
    .sort_values("qty", ascending=False)
    .head(10)
)
top10["brand"] = top10["product"].apply(_brand_of)
# 재고 매칭
if not inv.empty:
    stock_map = {
        str(p): int(s) for p, s in zip(inv["product"], inv["stock"])
    }
    top10["stock"] = top10["product"].apply(
        lambda p: stock_map.get(p, sum(
            int(v) for k, v in stock_map.items()
            if isinstance(k, str) and k and k in str(p)
        )) or 0,
    )
else:
    top10["stock"] = 0

# 5x2 그리드
n_cols = 5
for row_idx in range(0, len(top10), n_cols):
    sku_cols = st.columns(n_cols)
    for col_i, (_, sku) in enumerate(
        top10.iloc[row_idx:row_idx + n_cols].iterrows()
    ):
        rank = row_idx + col_i + 1
        product = str(sku["product"])
        brand = sku["brand"]
        bc = BRAND_COLORS.get(brand, {})
        primary = bc.get("primary", "#64748b")
        soft = bc.get("bg_soft", "#f8fafc")
        text_c = bc.get("text", "#0f172a")
        url = _find_image(product, img_map)
        img_html = (
            f'<img src="{url}" style="width:100%; height:120px; '
            f'object-fit:cover; border-radius:8px;" />'
            if url else
            f'<div style="width:100%; height:120px; background:{soft}; '
            f'border-radius:8px; display:flex; align-items:center; '
            f'justify-content:center; font-size:2rem; color:{primary};">📦</div>'
        )
        short = product[:24] + "..." if len(product) > 24 else product
        with sku_cols[col_i]:
            st.markdown(
                _flatten_html(f"""
<div style="background:white; border:1px solid #e2e8f0; border-radius:12px; padding:10px; min-height:240px; box-shadow:0 1px 3px rgba(15,23,42,0.04);">
    <div style="position:relative; margin-bottom:8px;">
        {img_html}
        <div style="position:absolute; top:6px; left:6px; background:{primary}; color:white; border-radius:999px; width:24px; height:24px; display:flex; align-items:center; justify-content:center; font-size:0.74rem; font-weight:700; box-shadow:0 1px 3px rgba(0,0,0,0.15);">{rank}</div>
    </div>
    <div style="font-size:0.66rem; color:{text_c}; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;">{brand}</div>
    <div style="font-size:0.78rem; color:#0f172a; font-weight:600; line-height:1.3; margin-top:3px; height:34px; overflow:hidden;" title="{product}">{short}</div>
    <div style="margin-top:6px; padding-top:6px; border-top:1px solid #f1f5f9;">
        <div style="display:flex; justify-content:space-between; font-size:0.7rem;">
            <span style="color:#94a3b8;">판매</span>
            <span style="color:{text_c}; font-weight:700;">{int(sku['qty']):,}개</span>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-top:2px;">
            <span style="color:#94a3b8;">매출</span>
            <span style="color:#64748b; font-weight:600;">₩{int(sku['rev']/10000):,}만</span>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-top:2px;">
            <span style="color:#94a3b8;">재고</span>
            <span style="color:{'#dc2626' if int(sku['stock']) < 10 else '#0f172a'}; font-weight:700;">{int(sku['stock']):,}개</span>
        </div>
    </div>
</div>
                """),
                unsafe_allow_html=True,
            )

st.markdown("---")


# ============================================================
# 재고 베스트 10 (재고액 기준) — 이지어드민 데이터 있을 때만
# ============================================================
if not inv.empty:
    st.markdown("### 📦 재고 베스트 10")
    _vc18, _vl18 = value_basis(inv)
    from utils.product_images import find_image_by_brand
    st.caption(f"재고액 (재고수량 × {_vl18}) 기준 상위 10 · 회수·할인 후보")

    inv_top = inv.copy()
    inv_top["value"] = inv_top["stock"] * inv_top[_vc18]
    inv_top = inv_top.sort_values("value", ascending=False).head(10)

    n_cols = 5
    for row_idx in range(0, len(inv_top), n_cols):
        s_cols = st.columns(n_cols)
        for col_i, (_, sku) in enumerate(
            inv_top.iloc[row_idx:row_idx + n_cols].iterrows()
        ):
            rank = row_idx + col_i + 1
            product = str(sku["product"])
            brand = sku.get("brand", "기타")
            bc = BRAND_COLORS.get(brand, {})
            primary = bc.get("primary", "#64748b")
            soft = bc.get("bg_soft", "#f8fafc")
            text_c = bc.get("text", "#0f172a")
            url = find_image_by_brand(product, brand, img_map, min_ratio=0.5) or ""
            img_html = (
                f'<img src="{url}" style="width:100%; height:110px; '
                f'object-fit:cover; border-radius:8px;" />'
                if url else
                f'<div style="width:100%; height:110px; background:{soft}; '
                f'border-radius:8px; display:flex; align-items:center; '
                f'justify-content:center; font-size:1.8rem; color:{primary};">📦</div>'
            )
            option = str(sku.get("option", "") or "").strip()
            if option.lower() in ("nan", "none"):
                option = ""
            short = product[:20] + "..." if len(product) > 20 else product
            opt_short = option[:26] + "..." if len(option) > 26 else option
            opt_html = (
                f'<div style="font-size:0.66rem; color:#64748b; '
                f'line-height:1.2; height:16px; overflow:hidden;" '
                f'title="{option}">🔖 {opt_short}</div>'
                if option else
                '<div style="height:16px;"></div>'
            )
            with s_cols[col_i]:
                st.markdown(
                    _flatten_html(f"""
<div style="background:white; border:1px solid #e2e8f0; border-radius:12px; padding:10px; min-height:236px; box-shadow:0 1px 3px rgba(15,23,42,0.04);">
    <div style="position:relative; margin-bottom:8px;">
        {img_html}
        <div style="position:absolute; top:6px; left:6px; background:#f59e0b; color:white; border-radius:999px; width:24px; height:24px; display:flex; align-items:center; justify-content:center; font-size:0.74rem; font-weight:700;">{rank}</div>
    </div>
    <div style="font-size:0.66rem; color:{text_c}; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;">{brand}</div>
    <div style="font-size:0.74rem; color:#0f172a; font-weight:600; line-height:1.3; margin-top:3px; height:30px; overflow:hidden;" title="{product}">{short}</div>
    {opt_html}
    <div style="margin-top:6px; padding-top:6px; border-top:1px solid #f1f5f9;">
        <div style="display:flex; justify-content:space-between; font-size:0.7rem;">
            <span style="color:#94a3b8;">재고</span>
            <span style="color:{text_c}; font-weight:700;">{int(sku['stock']):,}개</span>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:0.7rem; margin-top:2px;">
            <span style="color:#94a3b8;">재고액</span>
            <span style="color:#b45309; font-weight:700;">₩{int(sku['value']/10000):,}만</span>
        </div>
    </div>
</div>
                    """),
                    unsafe_allow_html=True,
                )
