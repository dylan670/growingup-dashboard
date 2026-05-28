"""루티니스트 1688 소싱 — 통과 후보 빠른 조회.

별도 도구(routinist-1688)가 1688 에서 수집한 후보를 Supabase 에 적재함.
이 페이지는 그 데이터를 읽어 통과 후보를 한 화면에서 빠르게 검토.

전체 기능(편집·북마크·심층 분석·자동 보강 등)은 별도 대시보드:
  https://routinist-1688-khbrbtfkarualexwpgyzfa.streamlit.app

데이터 소스: Supabase REST (anon publishable key).
"""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests
import streamlit as st

from utils.ui import (
    setup_page,
    TEXT_MAIN, TEXT_MUTED, TEXT_FAINT, BORDER_SUBTLE,
    BRAND_COLORS,
)


# ──────────────────────────────────────────────────────────
# 페이지 설정
# ──────────────────────────────────────────────────────────
setup_page(
    page_title="루티니스트 1688 소싱",
    page_icon="🛒",
    header_title="🛒 루티니스트 1688 소싱",
    header_subtitle="중국 1688 도매 소싱 후보 · 9 기준 자동 점수화 · A~D 등급",
)


# ──────────────────────────────────────────────────────────
# Supabase 연결 (Streamlit secrets 또는 환경변수)
# ──────────────────────────────────────────────────────────
def _get_supabase_creds() -> tuple[str, str]:
    """[supabase_1688] 섹션 또는 ROUTINIST_1688_SUPABASE_URL/KEY env 우선."""
    # 1) 전용 섹션
    try:
        sup = st.secrets.get("supabase_1688", {})
        url = (sup.get("url") or "").rstrip("/")
        key = sup.get("anon_key") or sup.get("key") or ""
        if url and key:
            return url, key
    except Exception:
        pass
    # 2) env 변수 (전용)
    url = os.getenv("ROUTINIST_1688_SUPABASE_URL", "").rstrip("/")
    key = os.getenv("ROUTINIST_1688_SUPABASE_KEY", "")
    if url and key:
        return url, key
    # 3) 일반 [supabase] 섹션 (다른 곳에서 이미 쓰는 경우)
    try:
        sup = st.secrets.get("supabase", {})
        url = (sup.get("url") or "").rstrip("/")
        key = sup.get("anon_key") or sup.get("key") or ""
        if url and key:
            return url, key
    except Exception:
        pass
    return "", ""


SB_URL, SB_KEY = _get_supabase_creds()

if not SB_URL or not SB_KEY:
    st.error(
        "❌ Supabase 접속 정보가 없습니다.\n\n"
        "Streamlit Cloud → Settings → Secrets 에 다음을 추가하세요:\n\n"
        "```toml\n"
        "[supabase_1688]\n"
        'url = "https://mkkkyxaiilmtnonibyrs.supabase.co"\n'
        'anon_key = "sb_publishable_xxxxx"\n'
        "```"
    )
    st.stop()


SB_HEADERS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
}


# ──────────────────────────────────────────────────────────
# 데이터 로드 (5분 캐시 — 우상단 새로고침 버튼으로 즉시 무효화 가능)
# ──────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="📊 1688 데이터 로드 중...")
def load_products() -> tuple[pd.DataFrame, str]:
    """Supabase REST 로 전체 products 조회 + 조회 시각."""
    from datetime import datetime
    try:
        r = requests.get(
            f"{SB_URL}/rest/v1/products",
            headers=SB_HEADERS,
            params={"select": "*", "order": "last_seen_at.desc"},
            timeout=20,
        )
        r.raise_for_status()
        rows = r.json()
        return pd.DataFrame(rows), datetime.now().strftime("%H:%M:%S")
    except Exception as e:
        st.error(f"Supabase 조회 실패: {e}")
        return pd.DataFrame(), datetime.now().strftime("%H:%M:%S")


@st.cache_data(ttl=300, show_spinner=False)
def load_bookmarks_count() -> int:
    try:
        r = requests.get(
            f"{SB_URL}/rest/v1/bookmarks",
            headers={**SB_HEADERS, "Prefer": "count=exact"},
            params={"select": "product_id"},
            timeout=10,
        )
        if r.status_code == 200:
            cr = r.headers.get("content-range", "0-0/0")
            return int(cr.split("/")[-1] or 0)
    except Exception:
        pass
    return 0


# ──────────────────────────────────────────────────────────
# 9 기준 점수 계산 (별도 도구의 analyzer 로직과 동일하게 in-page)
# ──────────────────────────────────────────────────────────
def _grade(total: int) -> tuple[str, str]:
    if total >= 75: return "A", "🟢"
    if total >= 55: return "B", "🔵"
    if total >= 35: return "C", "🟡"
    return "D", "🔴"


def _score_total_sold(s) -> int:
    if pd.isna(s) or s is None: return 0
    s = int(s)
    if s >= 10000: return 15
    if s >= 5000: return 12
    if s >= 1000: return 9
    if s >= 300: return 5
    return 2


def _score_reviews(r) -> int:
    if pd.isna(r) or r is None: return 0
    r = int(r)
    if r >= 500: return 13
    if r >= 100: return 10
    if r >= 50: return 7
    if r >= 10: return 3
    return 1


def _score_age(a) -> int:
    if pd.isna(a) or a is None: return 0
    a = int(a)
    if a >= 10: return 12
    if a >= 5: return 10
    if a >= 3: return 6
    return 2


def _score_badges(row) -> int:
    if row.get("is_super_factory"): return 25
    if row.get("is_powerful_merchant"): return 18
    if row.get("is_factory"): return 10
    return 0


def _score_price(price, cat_prices: list[float]) -> int:
    if pd.isna(price) or price is None: return 0
    if not cat_prices or len(cat_prices) < 3: return 5
    sorted_p = sorted(cat_prices)
    rank_below = sum(1 for x in sorted_p if x < price)
    pct = rank_below / len(sorted_p)
    if pct <= 0.25: return 10
    if pct <= 0.50: return 7
    if pct <= 0.75: return 3
    return 1


def _score_repurchase(r) -> int:
    if pd.isna(r) or r is None: return 0
    if r >= 70: return 10
    if r >= 50: return 7
    if r >= 30: return 4
    return 1


def _score_ontime(r) -> int:
    if pd.isna(r) or r is None: return 0
    if r >= 99: return 8
    if r >= 95: return 6
    if r >= 90: return 3
    return 1


def _score_service(s) -> int:
    if pd.isna(s) or s is None: return 0
    if s >= 4.5: return 4
    if s >= 4.0: return 3
    if s >= 3.5: return 1
    return 0


def _score_goodrate(r) -> int:
    if pd.isna(r) or r is None: return 0
    if r >= 98: return 3
    if r >= 95: return 2
    if r >= 90: return 1
    return 0


# 사용자 별점 (1-5) → 점수
def _score_user_image(s) -> int:
    if pd.isna(s) or not s: return 0
    s = int(s)
    if s >= 5: return 5
    if s >= 4: return 4
    if s >= 3: return 2
    if s >= 2: return 1
    return 0


def _score_user_review(s) -> int:
    if pd.isna(s) or not s: return 0
    s = int(s)
    if s >= 5: return 3
    if s >= 4: return 2
    if s >= 3: return 1
    return 0


def _score_user_product(s) -> int:
    if pd.isna(s) or not s: return 0
    s = int(s)
    if s >= 5: return 10
    if s >= 4: return 8
    if s >= 3: return 5
    if s >= 2: return 2
    return 0


def quick_score(row, cat_prices: list[float]) -> int:
    return (
        _score_total_sold(row.get("total_sold"))
        + _score_reviews(row.get("review_count"))
        + _score_age(row.get("shop_age_years"))
        + _score_badges(row)
        + _score_price(row.get("price_min"), cat_prices)
        + _score_repurchase(row.get("shop_repurchase_rate"))
        + _score_ontime(row.get("shop_on_time_rate"))
        + _score_service(row.get("shop_service_score"))
        + _score_goodrate(row.get("positive_review_ratio"))
        + _score_user_image(row.get("user_image_quality"))
        + _score_user_review(row.get("user_review_quality"))
        + _score_user_product(row.get("user_product_quality"))
    )


# 카테고리 한글 매핑 (1688 도구 정의와 동기)
CATEGORY_LABELS = {
    "belt": "러닝벨트",
    "apparel": "러닝복",
    "goggle": "러닝고글",
    "cap": "러닝모자",
    "socks": "러닝양말",
    "accessory": "악세사리",
    "unknown": "(미분류)",
}


def yuan_to_krw(y) -> float | None:
    if y is None or pd.isna(y): return None
    return y * 300 if y < 10 else y * 270 + 1000


# ──────────────────────────────────────────────────────────
# 새로고침 컨트롤 (마지막 갱신 시각 + 강제 새로고침 버튼)
# ──────────────────────────────────────────────────────────
ctrl_left, ctrl_right = st.columns([5, 1])
with ctrl_right:
    if st.button("🔄 새로고침", use_container_width=True,
                  help="풀 대시보드에서 갓 수집·편집한 데이터를 즉시 가져옵니다."):
        load_products.clear()
        load_bookmarks_count.clear()
        st.rerun()

# ──────────────────────────────────────────────────────────
# 데이터 로드 + 전처리
# ──────────────────────────────────────────────────────────
df_raw, fetched_at = load_products()
bookmarks_n = load_bookmarks_count()

with ctrl_left:
    st.caption(
        f"🕒 마지막 갱신: **{fetched_at}** · 자동 갱신 5분 간격 "
        f"(즉시 보고 싶으면 우측 🔄 새로고침)"
    )

if df_raw.empty:
    st.info("아직 수집된 1688 상품이 없습니다. 별도 수집 도구를 사용하세요.")
    st.link_button(
        "🚀 풀 1688 대시보드 열기 (수집·심층 분석·편집)",
        "https://routinist-1688-khbrbtfkarualexwpgyzfa.streamlit.app",
    )
    st.stop()


# 카테고리별 가격 분포 (가격 경쟁력 점수 계산용)
cat_prices_map: dict[str, list[float]] = {}
for _, p in df_raw.iterrows():
    pmin = p.get("price_min")
    if pmin is not None and not pd.isna(pmin):
        cat_prices_map.setdefault(p["category_id"], []).append(float(pmin))


# 점수 계산
df_raw["점수"] = df_raw.apply(
    lambda r: quick_score(r, cat_prices_map.get(r["category_id"], [])),
    axis=1,
)
df_raw["등급"] = df_raw["점수"].apply(lambda s: _grade(s)[0])
df_raw["등급_emoji"] = df_raw["점수"].apply(lambda s: f"{_grade(s)[1]} {_grade(s)[0]}")
df_raw["카테고리"] = df_raw["category_id"].map(lambda x: CATEGORY_LABELS.get(x, x))

PASS_SCORE = 35
total_n = len(df_raw)
passed_df = df_raw[df_raw["점수"] >= PASS_SCORE]
passed_n = len(passed_df)
url_n = int((df_raw.get("source") == "url").sum())


# ──────────────────────────────────────────────────────────
# KPI 4 카드 (루티니스트 그린 톤)
# ──────────────────────────────────────────────────────────
RT_GREEN = BRAND_COLORS["루티니스트"]["primary"]   # #16a34a
RT_GREEN_BG = BRAND_COLORS["루티니스트"]["bg"]     # #dcfce7
RT_GREEN_SOFT = BRAND_COLORS["루티니스트"]["bg_soft"]


def _kpi(icon: str, label: str, value: int, sub: str, accent: bool = False) -> str:
    bg = f"linear-gradient(135deg, {RT_GREEN_SOFT} 0%, #fff 70%)" if accent else "#fff"
    border = RT_GREEN_BG if accent else BORDER_SUBTLE
    val_color = RT_GREEN if accent else TEXT_MAIN
    return f"""
    <div style="background:{bg};border:1px solid {border};
                border-radius:14px;padding:14px 16px;height:100%;
                box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="display:flex;justify-content:space-between;
                  align-items:center;margin-bottom:6px">
        <div style="font-size:12px;color:{TEXT_MUTED};font-weight:700;
                    letter-spacing:-0.1px">{label}</div>
        <div style="background:{RT_GREEN_BG};color:{RT_GREEN};
                    width:30px;height:30px;border-radius:9px;
                    display:flex;align-items:center;justify-content:center;
                    font-size:15px">{icon}</div>
      </div>
      <div style="font-size:28px;font-weight:800;color:{val_color};
                  line-height:1;letter-spacing:-0.8px;
                  font-variant-numeric:tabular-nums">{value:,}</div>
      <div style="font-size:11.5px;color:{TEXT_FAINT};
                  font-weight:600;margin-top:6px">{sub}</div>
    </div>
    """


c1, c2, c3, c4 = st.columns(4)
c1.markdown(_kpi("📦", "전체 수집", total_n, "개 상품"), unsafe_allow_html=True)
c2.markdown(_kpi("🎯", "통과 후보", passed_n, f"점수 ≥ {PASS_SCORE}", accent=True),
            unsafe_allow_html=True)
c3.markdown(_kpi("⭐", "북마크", bookmarks_n, "즐겨찾기"), unsafe_allow_html=True)
c4.markdown(_kpi("🔗", "URL 등록", url_n, "심층 분석 대상"), unsafe_allow_html=True)

st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────
# 풀 대시보드 링크 (편집·심층 분석·자동 보강은 그쪽에서)
# ──────────────────────────────────────────────────────────
st.info(
    "📌 이 페이지는 **빠른 조회용**입니다. 편집·북마크·심층 분석·자동 보강은 "
    "[**풀 1688 대시보드**](https://routinist-1688-khbrbtfkarualexwpgyzfa.streamlit.app) "
    "에서 가능합니다 (별도 로그인)."
)


# ──────────────────────────────────────────────────────────
# 필터
# ──────────────────────────────────────────────────────────
col_f1, col_f2 = st.columns([3, 1])
with col_f1:
    cat_options = ["전체"] + sorted(df_raw["카테고리"].unique().tolist())
    sel_cat = st.selectbox("카테고리", options=cat_options, key="rt_cat")
with col_f2:
    only_pass = st.toggle(f"통과(점수 ≥ {PASS_SCORE})만", value=True, key="rt_only_pass")


df_show = df_raw.copy()
if sel_cat != "전체":
    df_show = df_show[df_show["카테고리"] == sel_cat]
if only_pass:
    df_show = df_show[df_show["점수"] >= PASS_SCORE]


# ──────────────────────────────────────────────────────────
# 등급별 통계 chip strip
# ──────────────────────────────────────────────────────────
grade_counts = df_show["등급"].value_counts().to_dict()
grade_meta = [
    ("A", "🟢", "강력추천", "#15803d", "#dcfce7"),
    ("B", "🔵", "검토 가치", "#1e40af", "#dbeafe"),
    ("C", "🟡", "보류",     "#b45309", "#fef3c7"),
    ("D", "🔴", "탈락 권장", "#b91c1c", "#fee2e2"),
]
chip_html = "".join(
    f'<span style="display:inline-flex;align-items:center;gap:6px;'
    f'padding:6px 12px;border-radius:999px;background:{bg};color:{fg};'
    f'font-size:12.5px;font-weight:700;letter-spacing:-0.1px;'
    f'margin-right:6px">'
    f'<span>{emoji}</span><span>{label}</span>'
    f'<span style="font-weight:800">{grade_counts.get(g, 0)}</span>'
    f'</span>'
    for g, emoji, label, fg, bg in grade_meta
)
st.markdown(
    f'<div style="margin:8px 0 14px">{chip_html}</div>',
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────
# 상품 표
# ──────────────────────────────────────────────────────────
if df_show.empty:
    st.warning("해당 조건의 상품이 없습니다.")
else:
    df_show = df_show.sort_values("점수", ascending=False).reset_index(drop=True)
    df_show["가격(¥)"] = df_show.apply(
        lambda r: f"¥{r['price_min']:.2f}"
        if pd.notna(r.get("price_min"))
            and r.get("price_min") == r.get("price_max")
        else (f"¥{r['price_min']:.2f}~¥{r['price_max']:.2f}"
              if pd.notna(r.get("price_min")) else "—"),
        axis=1,
    )
    df_show["가격(₩)"] = df_show["price_min"].apply(
        lambda y: f"₩{yuan_to_krw(y):,.0f}" if pd.notna(y) else "—"
    )
    df_show["뱃지"] = df_show.apply(
        lambda r: " ".join(filter(None, [
            "🏭" if r.get("is_factory") else None,
            "💪" if r.get("is_powerful_merchant") else None,
            "🌟" if r.get("is_super_factory") else None,
        ])) or "—",
        axis=1,
    )

    show_cols = [
        "main_image_url", "등급_emoji", "점수",
        "카테고리", "title_zh", "가격(¥)", "가격(₩)",
        "total_sold", "review_count",
        "shop_name", "shop_age_years", "뱃지", "product_url",
    ]
    rename = {
        "main_image_url": "이미지",
        "등급_emoji": "등급",
        "title_zh": "제품명 (中)",
        "total_sold": "누적 판매",
        "review_count": "리뷰",
        "shop_name": "공장명",
        "shop_age_years": "연혁(년)",
        "product_url": "1688 링크",
    }

    st.dataframe(
        df_show[show_cols].rename(columns=rename),
        use_container_width=True, hide_index=True,
        height=560,
        column_config={
            "이미지": st.column_config.ImageColumn("이미지", width="small"),
            "1688 링크": st.column_config.LinkColumn("1688 링크",
                                                    display_text="🔗 열기"),
            "누적 판매": st.column_config.NumberColumn(format="%d"),
            "리뷰": st.column_config.NumberColumn(format="%d"),
            "연혁(년)": st.column_config.NumberColumn(format="%d년"),
            "점수": st.column_config.ProgressColumn(
                "점수", format="%d", min_value=0, max_value=100,
            ),
            "등급": st.column_config.TextColumn("등급", width="small"),
        },
    )

    st.caption(
        f"💡 표시 중: **{len(df_show):,}개** / 전체 {total_n:,}개 · "
        f"통과 기준: 점수 ≥ {PASS_SCORE}"
    )
