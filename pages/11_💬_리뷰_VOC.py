"""리뷰 감성/키워드 VOC — 다음 SKU 의 정답지.

리뷰 텍스트에서:
  1. 감성 분류 (긍정/중립/부정) — 별점 + 키워드 기반
  2. 키워드 빈도 트렌드 (시간에 따른 변화)
  3. 부정/긍정 리뷰 분리 표시
  4. 상품별 별점/리뷰량 ranking
"""
from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.ui import (
    setup_page, BRAND_COLORS, channel_color,
    TEXT_MAIN, TEXT_MUTED, TEXT_FAINT, BORDER_SUBTLE,
)


setup_page(
    page_title="리뷰관리",
    page_icon="💬",
    header_title="💬 리뷰관리",
    header_subtitle="고객 목소리 → 다음 SKU 의 정답지 · 개선 우선순위",
)


ROOT = Path(__file__).parent.parent


# ==========================================================
# 데이터 로드
# ==========================================================
@st.cache_data(ttl=600, show_spinner="📊 리뷰 로드 중...")
def _load_reviews() -> pd.DataFrame:
    p = ROOT / "data" / "reviews.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(p, encoding="utf-8", encoding_errors="replace")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "text"])
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    # brand 컬럼이 CSV 에 직접 있으면 그대로 사용 (정확). 없으면 product 추론.
    if "brand" in df.columns:
        df["brand"] = df["brand"].fillna("기타").astype(str)
    else:
        df["brand"] = df["product"].apply(_infer_brand_from_product)
    return df


def _infer_brand_from_product(product: str) -> str:
    """제품명 → 브랜드 추정 (똑똑/롤라루/루티니스트 키워드 매칭)."""
    if not isinstance(product, str):
        return "기타"
    p = product.replace(" ", "")
    # 똑똑연구소 키워드
    for kw in ["똑똑", "김똑똑", "떡뻥", "분유", "이유식", "아기간식"]:
        if kw in p:
            return "똑똑연구소"
    # 롤라루 키워드
    for kw in ["롤라루", "캐리어", "여행", "기내용", "백팩"]:
        if kw in p:
            return "롤라루"
    # 루티니스트 키워드
    for kw in ["루티니", "런닝", "러닝", "운동복", "운동조끼",
               "트레일", "장갑", "마라톤"]:
        if kw in p:
            return "루티니스트"
    return "기타"


# ==========================================================
# 텍스트 분석 — 한국어 토큰화 (의존성 없는 간단 방식)
# ==========================================================

# 불용어 + 흔한 어미
STOPWORDS = {
    # 조사·어미
    "은", "는", "이", "가", "을", "를", "에", "의", "와", "과", "로", "으로",
    "도", "만", "에서", "께", "한테", "보다", "처럼", "같이", "마저", "조차",
    "부터", "까지", "이나", "나", "이라", "라고", "이라고", "에게", "한", "할",
    "하는", "해서", "하고", "하다", "했어요", "했습니다", "해요", "이에요",
    "이예요", "예요", "이고", "라서", "여서", "지만", "는데", "그리고",
    "그래서", "근데", "하지만", "그러나", "또", "또한", "정말", "진짜",
    "너무", "아주", "매우", "조금", "약간", "좀", "그냥", "되게", "엄청",
    # 자주 쓰이는 의미 약한 단어
    "있어요", "있습니다", "없어요", "없습니다", "있는", "없는", "있고", "없고",
    "그래도", "그런데", "이거", "저거", "그거", "이것", "저것", "그것", "이런",
    "저런", "그런", "있을", "없을", "거에요", "거예요", "건가요", "건지",
    "쓰는", "쓰는데", "쓰고", "쓸", "쓸때", "쓸수",
    # 영문 stop
    "the", "a", "and", "or", "of", "in", "to", "for", "is", "are", "this",
    "that", "with", "on", "at", "be", "it",
}


# 긍정/부정 키워드 사전 (한국어 도메인)
POSITIVE_KEYWORDS = {
    "좋아요", "좋네요", "좋은", "최고", "굿", "추천", "만족", "맛있어요",
    "맛있", "튼튼", "예쁘", "예뻐", "이뻐", "이쁘", "편해요", "편한", "편함",
    "가벼워", "가벼운", "부드러", "재구매", "단단", "고급", "퀄리티", "퀄",
    "OK", "ok", "굳", "최애", "꿀", "꿀템", "강추", "재주문", "다시살게요",
    "잘쓰고", "잘쓸게요", "감사", "감사해요", "감사합니다", "보들", "건강",
}

NEGATIVE_KEYWORDS = {
    "별로", "안좋", "안좋아요", "최악", "구려", "아쉬워요", "아쉬운", "아쉽",
    "불편", "불편해요", "약해요", "약함", "허접", "비싸", "비싼", "가격이",
    "작아요", "작은", "작네요", "커요", "큰", "크네요", "무겁", "무거워",
    "딱딱", "거칠", "뻣뻣", "안와요", "고장", "파손", "흠집", "스크래치",
    "환불", "반품", "교환", "실망", "후회", "별루", "낭패", "찢", "터졌",
    "냄새", "맛없", "이상해", "이상한", "심해", "엉망",
}


def _tokenize_kr(text: str) -> list[str]:
    """간단한 한국어 토큰화 — 2자+ 한글 / 영문/숫자 단어."""
    if not isinstance(text, str):
        return []
    # 한글 또는 영문/숫자 단어만 추출
    tokens = re.findall(r"[가-힣]+|[A-Za-z]+|\d+", text)
    out: list[str] = []
    for t in tokens:
        if len(t) < 2:
            continue
        # 어미/조사 제거 (간단)
        for suf in ["에요", "예요", "어요", "아요", "네요", "구요", "거든요",
                    "려고", "려구"]:
            if t.endswith(suf) and len(t) > len(suf) + 1:
                t = t[: -len(suf)]
                break
        if t in STOPWORDS:
            continue
        if len(t) < 2:
            continue
        out.append(t)
    return out


def _sentiment_score(text: str, rating: float | None) -> tuple[str, float]:
    """
    텍스트 + 별점 → (감성라벨, score).

    score: -1.0 (매우 부정) ~ +1.0 (매우 긍정)
    별점: 5점 만점에서 4점 이상=긍정, 3점=중립, 2점 이하=부정 (text 보강)
    """
    text_score = 0
    pos_hits, neg_hits = 0, 0
    if isinstance(text, str):
        low = text
        for kw in POSITIVE_KEYWORDS:
            if kw in low:
                pos_hits += 1
        for kw in NEGATIVE_KEYWORDS:
            if kw in low:
                neg_hits += 1
        text_score = pos_hits - neg_hits

    # 별점 점수
    rating_score = 0
    if rating is not None and not pd.isna(rating):
        if rating >= 4.5:
            rating_score = 2
        elif rating >= 4:
            rating_score = 1
        elif rating >= 3:
            rating_score = 0
        elif rating >= 2:
            rating_score = -1
        else:
            rating_score = -2

    final = rating_score + (text_score * 0.5)
    if final >= 1:
        label = "긍정"
    elif final <= -0.5:
        label = "부정"
    else:
        label = "중립"
    # normalize
    score = max(-1.0, min(1.0, final / 3.0))
    return label, score


@st.cache_data(ttl=600)
def _enrich_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """리뷰 데이터에 감성/토큰 컬럼 추가."""
    if df.empty:
        return df
    out = df.copy()
    sent = out.apply(
        lambda r: _sentiment_score(r["text"], r.get("rating")), axis=1,
    )
    out["감성"] = sent.apply(lambda x: x[0])
    out["감성점수"] = sent.apply(lambda x: x[1])
    out["tokens"] = out["text"].apply(_tokenize_kr)
    return out


reviews = _load_reviews()
if reviews.empty:
    st.warning("📭 리뷰 데이터가 없습니다. (data/reviews.csv 확인)")
    st.stop()

enriched = _enrich_reviews(reviews)


# ==========================================================
# 데모 데이터 배지 (실 sync 성공 시 자동 사라짐)
# ==========================================================
def _load_reviews_meta() -> dict:
    """data/reviews_meta.json 로드. 없으면 빈 dict."""
    import json
    p = ROOT / "data" / "reviews_meta.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


_meta = _load_reviews_meta()
_source = (_meta.get("source") or "").lower()

if _source == "demo":
    st.markdown(
        f"""
        <div style="background: linear-gradient(90deg, #fef3c7 0%, #fffbeb 100%);
                    border: 1px solid #f59e0b; border-left: 6px solid #f59e0b;
                    border-radius: 10px; padding: 14px 18px; margin-bottom: 18px;">
            <div style="display:flex; align-items:center; gap:10px;
                        font-size:0.95rem; font-weight:700; color:#b45309;">
                🚧 데모 데이터입니다 — 화면의 모든 숫자는 가상입니다
            </div>
            <div style="font-size:0.82rem; color:#92400e; margin-top:6px;
                        line-height:1.5;">
                네이버 커머스 API '상품 리뷰' 스코프 + 카페24
                <code>mall.read_community</code> scope 가 활성화되면
                <code>sync_reviews.py</code> 가 실데이터로 자동 교체하고 이 배지가 사라집니다.
                <br>
                <span style="font-size:0.74rem; color:#a16207;">
                    데모 생성일: {_meta.get('generated_at', 'unknown')[:19]}
                    · {_meta.get('count', '?')}건
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
elif _source == "live":
    last_sync = _meta.get("last_sync_at", "")[:19]
    total = _meta.get("last_sync_total", 0)
    st.markdown(
        f"""
        <div style="background: #dcfce7; border-left: 4px solid #16a34a;
                    border-radius: 8px; padding: 10px 16px; margin-bottom: 14px;
                    font-size: 0.85rem;">
            🟢 <b>실데이터</b> — 마지막 sync: {last_sync}
            <span style="color:#64748b; margin-left:8px;">
                ({total}건 수집)
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ==========================================================
# 채널 화이트리스트 — 실데이터 sync 가능한 채널만 노출
#   - 자사몰 (Cafe24): OAuth 재인증 완료, mall.read_community
#   - 네이버: 일반 셀러용 리뷰 API 미제공 → 제외
#   - 쿠팡: 셀러센터 리뷰 API 없음 → 제외
# 추후 채널 추가 시 이 리스트에 'naver' / '쿠팡' 추가하면 자동 노출됨.
# ==========================================================
ALLOWED_CHANNELS = ["자사몰"]
enriched = enriched[enriched["channel"].isin(ALLOWED_CHANNELS)].copy()

# ==========================================================
# 사이드바 필터
# ==========================================================
st.sidebar.markdown("#### 🔎 필터")

brand_options = ["전체"] + sorted(enriched["brand"].unique().tolist())
selected_brand = st.sidebar.selectbox("브랜드", brand_options, index=0)

# 채널 필터 (multi-select)
all_channels = sorted(enriched["channel"].dropna().unique().tolist())
# 기본 정렬: 네이버 → 자사몰 → 쿠팡 → 그 외
ch_order = {"네이버": 0, "자사몰": 1, "쿠팡": 2}
all_channels = sorted(all_channels, key=lambda c: ch_order.get(c, 99))
selected_channels = st.sidebar.multiselect(
    "채널",
    all_channels,
    default=all_channels,
    help="실데이터 sync 채널만 노출 (현재: 자사몰)",
)

# 기간
max_date = enriched["date"].max().date()
min_date = enriched["date"].min().date()
period_label = st.sidebar.selectbox(
    "기간",
    ["전체", "최근 30일", "최근 90일", "최근 180일"],
    index=0,
)
if period_label == "최근 30일":
    start_date = max_date - timedelta(days=30)
elif period_label == "최근 90일":
    start_date = max_date - timedelta(days=90)
elif period_label == "최근 180일":
    start_date = max_date - timedelta(days=180)
else:
    start_date = min_date

# 감성 필터
sent_filter = st.sidebar.multiselect(
    "감성",
    ["긍정", "중립", "부정"],
    default=["긍정", "중립", "부정"],
)

# 필터 적용
mask = (enriched["date"].dt.date >= start_date) & (
    enriched["date"].dt.date <= max_date
)
if selected_brand != "전체":
    mask &= enriched["brand"] == selected_brand
if selected_channels:
    mask &= enriched["channel"].isin(selected_channels)
if sent_filter:
    mask &= enriched["감성"].isin(sent_filter)
filtered = enriched[mask].copy()

if filtered.empty:
    st.warning("선택한 조건에 리뷰가 없습니다.")
    st.stop()


# ==========================================================
# 상단 KPI
# ==========================================================
total_reviews = len(filtered)
avg_rating = filtered["rating"].mean() if filtered["rating"].notna().any() else 0
pos_count = (filtered["감성"] == "긍정").sum()
neg_count = (filtered["감성"] == "부정").sum()
neu_count = (filtered["감성"] == "중립").sum()
pos_share = pos_count / total_reviews * 100 if total_reviews else 0
neg_share = neg_count / total_reviews * 100 if total_reviews else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("💬 총 리뷰", f"{total_reviews:,}")
k2.metric("⭐ 평균 별점", f"{avg_rating:.2f}")
k3.metric("😊 긍정", f"{pos_count}", delta=f"{pos_share:.1f}%")
k4.metric("😐 중립", f"{neu_count}")
k5.metric("😠 부정", f"{neg_count}",
          delta=f"{neg_share:.1f}%",
          delta_color="inverse")

st.markdown("---")


# ==========================================================
# 채널 × 브랜드 매트릭스 (가장 정밀한 분리)
# ==========================================================
st.markdown("##### 🔀 채널 × 브랜드 매트릭스")
st.caption(
    "각 채널별로 브랜드 리뷰가 어떻게 분포되는지 한눈에. "
    "셀 클릭은 아닌 시각화 — 사이드바 채널/브랜드 필터로 깊이 파고드세요."
)

# 채널 정렬 (네이버 → 자사몰 → 쿠팡)
ch_priority = {"네이버": 0, "자사몰": 1, "쿠팡": 2}
mtx_channels = sorted(
    filtered["channel"].dropna().unique().tolist(),
    key=lambda c: ch_priority.get(c, 99),
)
# 브랜드 정렬 (똑똑 → 롤라루 → 루티 → 기타)
br_priority = {"똑똑연구소": 0, "롤라루": 1, "루티니스트": 2, "기타": 99}
mtx_brands = sorted(
    filtered["brand"].dropna().unique().tolist(),
    key=lambda b: br_priority.get(b, 50),
)

# 채널별 row 생성
for ch in mtx_channels:
    ch_df = filtered[filtered["channel"] == ch]
    if ch_df.empty:
        continue

    # 채널 헤더 (채널 색상 적용)
    ch_color = channel_color(ch, default="#64748b")
    ch_total = len(ch_df)
    ch_avg = ch_df["rating"].mean() if ch_df["rating"].notna().any() else 0
    ch_pos = (ch_df["감성"] == "긍정").sum()
    ch_neg = (ch_df["감성"] == "부정").sum()

    st.markdown(
        f"""
        <div style="display:flex; align-items:baseline; gap:14px;
                    margin-top:14px; margin-bottom:8px;
                    padding:6px 0; border-bottom:2px solid {ch_color};">
            <div style="font-size:1rem; font-weight:700; color:{ch_color};">
                📡 {ch}
            </div>
            <div style="font-size:0.82rem; color:#64748b;">
                {ch_total}건 · ⭐{ch_avg:.2f}
                · 😊{ch_pos/ch_total*100:.0f}%
                · 😠{ch_neg/ch_total*100:.0f}%
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 브랜드별 셀 (가로 3개)
    cell_cols = st.columns(len(mtx_brands))
    for i, b in enumerate(mtx_brands):
        cell = ch_df[ch_df["brand"] == b]
        bcolor = BRAND_COLORS.get(b, {})
        primary = bcolor.get("primary", "#64748b")
        bg_soft = bcolor.get("bg_soft", "#f8fafc")
        text_col = bcolor.get("text", "#0f172a")

        with cell_cols[i]:
            if cell.empty:
                # 빈 셀 — 회색 처리
                st.markdown(
                    f"""
                    <div style="background:#f1f5f9; border:1px dashed #cbd5e1;
                                border-radius:8px; padding:14px 14px;
                                opacity:0.6; min-height:118px;
                                display:flex; flex-direction:column;
                                justify-content:center; align-items:center;">
                        <div style="font-size:0.82rem; color:#94a3b8;
                                    font-weight:600;">{b}</div>
                        <div style="font-size:1.2rem; color:#cbd5e1;
                                    margin-top:6px;">— 0건 —</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                continue

            c_total = len(cell)
            c_avg = cell["rating"].mean() if cell["rating"].notna().any() else 0
            c_pos = int((cell["감성"] == "긍정").sum())
            c_neu = int((cell["감성"] == "중립").sum())
            c_neg = int((cell["감성"] == "부정").sum())
            c_sku = cell["product"].nunique()
            pos_pct = c_pos / c_total * 100
            neu_pct = c_neu / c_total * 100
            neg_pct = c_neg / c_total * 100
            pos_pct_label = c_pos / c_total * 100
            neg_pct_label = c_neg / c_total * 100

            st.markdown(
                f"""
                <div style="background:{bg_soft}; border-left:4px solid {primary};
                            border-radius:8px; padding:12px 14px;
                            min-height:118px;">
                    <div style="display:flex; justify-content:space-between;
                                align-items:baseline;">
                        <div style="font-size:0.85rem; font-weight:700;
                                    color:{text_col};">
                            {b}
                        </div>
                        <div style="font-size:0.7rem; color:#94a3b8;">
                            SKU {c_sku}
                        </div>
                    </div>
                    <div style="font-size:1.4rem; font-weight:700;
                                color:{primary}; line-height:1.2;
                                margin-top:2px;">
                        {c_total}건
                    </div>
                    <div style="font-size:0.74rem; color:#64748b;
                                margin-top:2px;">
                        ⭐ {c_avg:.2f}
                    </div>
                    <div style="display:flex; height:5px; margin-top:8px;
                                border-radius:3px; overflow:hidden;">
                        <div style="width:{pos_pct}%; background:#16a34a;"
                             title="긍정 {c_pos}건"></div>
                        <div style="width:{neu_pct}%; background:#94a3b8;"
                             title="중립 {c_neu}건"></div>
                        <div style="width:{neg_pct}%; background:#dc2626;"
                             title="부정 {c_neg}건"></div>
                    </div>
                    <div style="font-size:0.7rem; color:#64748b;
                                margin-top:4px; display:flex;
                                justify-content:space-between;">
                        <span style="color:#16a34a;">😊 {pos_pct_label:.0f}%</span>
                        <span style="color:#dc2626;">😠 {neg_pct_label:.0f}%</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

st.markdown("---")


# ==========================================================
# 브랜드별 요약 카드 (한눈에 보기)
# ==========================================================
st.markdown("##### 🏷 브랜드별 리뷰 요약")

brand_summary = (
    filtered.groupby("brand")
    .agg(
        리뷰수=("text", "count"),
        평균별점=("rating", "mean"),
        긍정=("감성", lambda s: (s == "긍정").sum()),
        중립=("감성", lambda s: (s == "중립").sum()),
        부정=("감성", lambda s: (s == "부정").sum()),
        고유SKU=("product", "nunique"),
    )
    .reset_index()
)
brand_summary["긍정률%"] = (brand_summary["긍정"] / brand_summary["리뷰수"] * 100).round(1)
brand_summary["부정률%"] = (brand_summary["부정"] / brand_summary["리뷰수"] * 100).round(1)

# 우선순위 정렬: 똑똑연구소 → 롤라루 → 루티니스트 → 기타
brand_order = {"똑똑연구소": 0, "롤라루": 1, "루티니스트": 2, "기타": 99}
brand_summary["_order"] = brand_summary["brand"].map(brand_order).fillna(50)
brand_summary = brand_summary.sort_values("_order").reset_index(drop=True)

# 브랜드별 카드 (BRAND_COLORS 사용)
brand_cols = st.columns(max(1, len(brand_summary)))
for i, row in brand_summary.iterrows():
    bname = row["brand"]
    bcolor = BRAND_COLORS.get(bname, {})
    primary = bcolor.get("primary", "#64748b")
    bg_soft = bcolor.get("bg_soft", "#f1f5f9")
    text_color = bcolor.get("text", "#0f172a")

    # 감성 비율 막대 (긍정/중립/부정 가로 바)
    total = row["리뷰수"] or 1
    pos_pct = row["긍정"] / total * 100
    neu_pct = row["중립"] / total * 100
    neg_pct = row["부정"] / total * 100

    with brand_cols[i]:
        st.markdown(
            f"""
            <div style="background:{bg_soft}; border-left:4px solid {primary};
                        border-radius:8px; padding:14px 16px;">
                <div style="font-size:0.95rem; font-weight:700;
                            color:{text_color}; margin-bottom:6px;">
                    {bname}
                </div>
                <div style="font-size:1.6rem; font-weight:700;
                            color:{primary}; line-height:1.2;">
                    {row['리뷰수']}건
                </div>
                <div style="font-size:0.8rem; color:#64748b;
                            margin-top:4px;">
                    ⭐ {row['평균별점']:.2f} · SKU {int(row['고유SKU'])}개
                </div>
                <div style="display:flex; height:6px; margin-top:10px;
                            border-radius:3px; overflow:hidden;">
                    <div style="width:{pos_pct}%; background:#16a34a;"
                         title="긍정 {int(row['긍정'])}건"></div>
                    <div style="width:{neu_pct}%; background:#94a3b8;"
                         title="중립 {int(row['중립'])}건"></div>
                    <div style="width:{neg_pct}%; background:#dc2626;"
                         title="부정 {int(row['부정'])}건"></div>
                </div>
                <div style="font-size:0.74rem; color:#64748b;
                            margin-top:6px; display:flex;
                            justify-content:space-between;">
                    <span style="color:#16a34a;">😊 {row['긍정률%']}%</span>
                    <span style="color:#dc2626;">😠 {row['부정률%']}%</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("---")


# ==========================================================
# 탭 구성
# ==========================================================
tab_sent, tab_kw, tab_neg, tab_prod = st.tabs([
    "📊 감성 분포",
    "🔠 키워드 빈도",
    "🚨 부정 리뷰 분석",
    "🏷 상품별 ranking",
])


# ==========================================================
# TAB 1 — 감성 분포 + 시간 트렌드
# ==========================================================
with tab_sent:
    c1, c2 = st.columns([1, 1.5])

    with c1:
        st.markdown("##### 🎭 감성 비중")
        sent_count = filtered["감성"].value_counts()
        colors_map = {"긍정": "#16a34a", "중립": "#94a3b8", "부정": "#dc2626"}
        fig = go.Figure(go.Pie(
            labels=sent_count.index,
            values=sent_count.values,
            marker_colors=[colors_map.get(l, "#94a3b8") for l in sent_count.index],
            hole=0.5,
            textinfo="label+percent",
            textfont=dict(size=13),
        ))
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0), showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("##### 📈 일별 감성 트렌드")
        daily = (
            filtered.groupby([filtered["date"].dt.date, "감성"])
            .size()
            .reset_index(name="count")
        )
        if not daily.empty:
            fig = px.area(
                daily, x="date", y="count", color="감성",
                color_discrete_map={
                    "긍정": "#16a34a", "중립": "#94a3b8", "부정": "#dc2626",
                },
            )
            fig.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="", yaxis_title="리뷰 수",
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig, use_container_width=True)

    # 별점 분포
    st.markdown("##### ⭐ 별점 × 감성 매트릭스")
    if filtered["rating"].notna().any():
        rating_sent = (
            filtered.groupby(["rating", "감성"]).size().reset_index(name="count")
        )
        fig = px.bar(
            rating_sent, x="rating", y="count", color="감성",
            color_discrete_map={
                "긍정": "#16a34a", "중립": "#94a3b8", "부정": "#dc2626",
            },
            barmode="stack",
        )
        fig.update_layout(
            height=300, margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="별점", yaxis_title="리뷰 수",
        )
        st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# TAB 2 — 키워드 빈도 + 트렌드
# ==========================================================
with tab_kw:
    st.markdown("##### 🔠 키워드 빈도 — Top N")

    # 감성별 분리해서 보기
    view = st.radio(
        "감성 필터",
        ["전체", "긍정만", "부정만"],
        horizontal=True,
    )
    if view == "긍정만":
        kw_df = filtered[filtered["감성"] == "긍정"]
    elif view == "부정만":
        kw_df = filtered[filtered["감성"] == "부정"]
    else:
        kw_df = filtered

    # 토큰 카운트
    all_tokens: list[str] = []
    for toks in kw_df["tokens"]:
        all_tokens.extend(toks)
    counter = Counter(all_tokens)
    top_kw = counter.most_common(30)

    if not top_kw:
        st.info("키워드 없음")
    else:
        kw_df_show = pd.DataFrame(top_kw, columns=["키워드", "빈도"])
        # 베이스 색상
        base_color = (
            "#16a34a" if view == "긍정만"
            else "#dc2626" if view == "부정만" else "#2563eb"
        )
        fig = px.bar(
            kw_df_show.head(20),
            x="빈도", y="키워드",
            orientation="h",
            text="빈도",
            color="빈도",
            color_continuous_scale=["#e2e8f0", base_color],
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=500, margin=dict(l=10, r=10, t=20, b=10),
            coloraxis_showscale=False,
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 키워드 전체 표 (Top 30)", expanded=False):
            st.dataframe(
                kw_df_show, hide_index=True, width="stretch",
                height=min(600, 60 + len(kw_df_show) * 36),
            )

    # 키워드 트렌드 — 시간에 따른 변화
    st.markdown("---")
    st.markdown("##### 📈 키워드 시간 트렌드")
    target_keywords = st.multiselect(
        "추적할 키워드 (Top 30 중 선택)",
        [k for k, _ in top_kw[:30]] if top_kw else [],
        default=[k for k, _ in top_kw[:5]] if top_kw else [],
        help="시간에 따른 언급 변화 추적 — 트렌드 변화 조기 감지",
    )

    if target_keywords:
        kw_df["주차"] = kw_df["date"].dt.to_period("W").dt.start_time
        trend_rows = []
        for week, grp in kw_df.groupby("주차"):
            text_blob = " ".join(grp["text"].astype(str))
            for kw in target_keywords:
                trend_rows.append({
                    "주차": week, "키워드": kw,
                    "언급": text_blob.count(kw),
                })
        trend_df = pd.DataFrame(trend_rows)
        if not trend_df.empty:
            fig = px.line(
                trend_df, x="주차", y="언급", color="키워드", markers=True,
            )
            fig.update_layout(
                height=350, margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="", yaxis_title="언급 횟수",
            )
            st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# TAB 3 — 부정 리뷰 분석 (개선 우선순위)
# ==========================================================
with tab_neg:
    st.markdown("##### 🚨 부정 리뷰 — 개선 우선순위 도출")
    neg_reviews = filtered[filtered["감성"] == "부정"].copy()

    if neg_reviews.empty:
        st.success("🎉 부정 리뷰 없음 (선택 조건 내)")
    else:
        # 부정 키워드 빈도
        neg_tokens: list[str] = []
        for toks in neg_reviews["tokens"]:
            neg_tokens.extend(toks)
        neg_counter = Counter(neg_tokens)
        neg_top = neg_counter.most_common(15)

        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("**🔥 부정 키워드 Top 15**")
            if neg_top:
                neg_kw_df = pd.DataFrame(neg_top, columns=["키워드", "빈도"])
                fig = px.bar(
                    neg_kw_df.head(15),
                    x="빈도", y="키워드", orientation="h",
                    color="빈도",
                    color_continuous_scale=["#fecaca", "#dc2626"],
                )
                fig.update_layout(
                    yaxis={"categoryorder": "total ascending"},
                    height=400, margin=dict(l=10, r=10, t=10, b=10),
                    coloraxis_showscale=False,
                )
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("**📦 상품별 부정 리뷰 수**")
            prod_neg = (
                neg_reviews.groupby("product")
                .size()
                .reset_index(name="부정리뷰수")
                .sort_values("부정리뷰수", ascending=False)
                .head(10)
            )
            if not prod_neg.empty:
                fig = px.bar(
                    prod_neg,
                    x="부정리뷰수", y="product", orientation="h",
                    color="부정리뷰수",
                    color_continuous_scale=["#fef3c7", "#ea580c"],
                )
                fig.update_layout(
                    yaxis={"categoryorder": "total ascending", "title": ""},
                    height=400, margin=dict(l=10, r=10, t=10, b=10),
                    coloraxis_showscale=False,
                )
                st.plotly_chart(fig, use_container_width=True)

        # 부정 리뷰 원문 (최근순)
        st.markdown("**📝 부정 리뷰 원문 (최신순)**")
        neg_display = neg_reviews.sort_values("date", ascending=False)[
            ["date", "channel", "product", "rating", "text"]
        ].copy()
        neg_display["date"] = neg_display["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(
            neg_display, hide_index=True, width="stretch",
            height=min(500, 60 + len(neg_display) * 38),
            column_config={
                "text": st.column_config.TextColumn("내용", width="large"),
            },
        )

        if neg_top:
            top_neg_kw = neg_top[0][0]
            top_neg_count = neg_top[0][1]
            st.error(
                f"💡 **개선 우선순위 #1** — `{top_neg_kw}` 키워드가 "
                f"{top_neg_count}회 언급. 이 영역 (제품 사양 또는 정책) "
                f"우선 점검 필요."
            )


# ==========================================================
# TAB 4 — 상품별 ranking (브랜드별 분리)
# ==========================================================
with tab_prod:
    st.markdown("##### 🏷 상품별 별점 & 리뷰량 ranking (브랜드별)")

    # 채널 분리 토글
    show_channel_split = st.toggle(
        "📡 채널별 분리해서 보기",
        value=False,
        help="ON 시: 같은 상품도 네이버/자사몰/쿠팡 따로 표시 → 채널별 별점 차이 확인",
        key="prod_rank_channel_split",
    )

    group_cols = (
        ["brand", "channel", "product"] if show_channel_split
        else ["brand", "product"]
    )
    prod_agg = (
        filtered.groupby(group_cols)
        .agg(
            리뷰수=("text", "count"),
            평균별점=("rating", "mean"),
            긍정수=("감성", lambda s: (s == "긍정").sum()),
            중립수=("감성", lambda s: (s == "중립").sum()),
            부정수=("감성", lambda s: (s == "부정").sum()),
            감성점수=("감성점수", "mean"),
        )
        .reset_index()
    )
    prod_agg["긍정률%"] = (
        prod_agg["긍정수"] / prod_agg["리뷰수"] * 100
    ).round(1)
    prod_agg["부정률%"] = (
        prod_agg["부정수"] / prod_agg["리뷰수"] * 100
    ).round(1)
    prod_agg["평균별점"] = prod_agg["평균별점"].round(2)
    prod_agg["감성점수"] = prod_agg["감성점수"].round(2)

    # ============================================
    # 브랜드별 sub-table — 명확한 시각적 구분
    # ============================================
    brand_order_local = {"똑똑연구소": 0, "롤라루": 1, "루티니스트": 2, "기타": 99}
    brands_in_data = sorted(
        prod_agg["brand"].unique(),
        key=lambda b: brand_order_local.get(b, 50),
    )

    for b in brands_in_data:
        sub = prod_agg[prod_agg["brand"] == b].sort_values(
            ["리뷰수", "평균별점"], ascending=[False, False]
        )
        if sub.empty:
            continue

        bcolor = BRAND_COLORS.get(b, {})
        primary = bcolor.get("primary", "#64748b")
        bg_soft = bcolor.get("bg_soft", "#f1f5f9")

        # 브랜드 헤더
        b_total = int(sub["리뷰수"].sum())
        b_avg = sub["평균별점"].mean()
        st.markdown(
            f"""
            <div style="background:{bg_soft}; border-left:5px solid {primary};
                        border-radius:8px; padding:10px 14px; margin-top:14px;
                        margin-bottom:8px;">
                <div style="font-size:1.05rem; font-weight:700;
                            color:{primary};">
                    🏷 {b}
                    <span style="font-size:0.82rem; color:#64748b;
                                 font-weight:500; margin-left:8px;">
                        · {len(sub)}개 SKU · 총 {b_total}건 리뷰
                        · 평균 ⭐{b_avg:.2f}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 채널 분리 모드면 channel 컬럼 포함
        display_cols = (
            ["channel", "product", "리뷰수", "평균별점",
             "긍정수", "중립수", "부정수", "긍정률%", "부정률%"]
            if show_channel_split else
            ["product", "리뷰수", "평균별점",
             "긍정수", "중립수", "부정수", "긍정률%", "부정률%"]
        )
        display_sub = sub[display_cols].copy()
        if show_channel_split:
            # 채널 정렬: 네이버 → 자사몰 → 쿠팡
            ch_priority2 = {"네이버": 0, "자사몰": 1, "쿠팡": 2}
            display_sub["_ch_order"] = display_sub["channel"].map(
                ch_priority2,
            ).fillna(99)
            display_sub = display_sub.sort_values(
                ["_ch_order", "리뷰수"], ascending=[True, False],
            ).drop(columns=["_ch_order"])

        col_cfg = {
            "product": st.column_config.TextColumn("제품명", width="large"),
            "긍정률%": st.column_config.ProgressColumn(
                "긍정률", format="%.1f%%", min_value=0, max_value=100,
            ),
            "부정률%": st.column_config.ProgressColumn(
                "부정률", format="%.1f%%", min_value=0, max_value=100,
            ),
        }
        if show_channel_split:
            col_cfg["channel"] = st.column_config.TextColumn(
                "채널", width="small",
            )

        st.dataframe(
            display_sub,
            hide_index=True, width="stretch",
            height=min(450, 50 + len(display_sub) * 36),
            column_config=col_cfg,
        )

    st.markdown("---")

    # ============================================
    # 통합 산점도 — 브랜드별 색상 구분
    # ============================================
    if len(prod_agg) > 1:
        st.markdown("##### 📊 리뷰량 × 별점 산점도 (브랜드별 색상)")
        prod_agg_sorted = prod_agg.sort_values("리뷰수", ascending=False)
        fig = px.scatter(
            prod_agg_sorted, x="리뷰수", y="평균별점",
            size="리뷰수", color="brand",
            color_discrete_map={
                b: BRAND_COLORS.get(b, {}).get("primary", "#94a3b8")
                for b in prod_agg_sorted["brand"].unique()
            },
            hover_data={"product": True, "긍정률%": True, "부정률%": True,
                        "리뷰수": True, "brand": False},
            size_max=40,
        )
        fig.update_layout(
            height=450, margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="리뷰 수", yaxis_title="평균 별점",
            legend=dict(title="브랜드", orientation="h", y=1.1),
        )
        # 기준선
        fig.add_hline(y=4.0, line_dash="dot", line_color="#94a3b8",
                      annotation_text="별점 4.0 기준")
        fig.add_vline(
            x=prod_agg["리뷰수"].median(),
            line_dash="dot", line_color="#cbd5e1",
            annotation_text="리뷰 수 중앙값",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "💡 **사분면 해석** · "
            "**우상단** = 베스트셀러 (리뷰 많고 별점 높음, 확장 후보) · "
            "**우하단** = 개선 시급 (매출은 나는데 만족도 낮음) · "
            "**좌상단** = 잠재력 (별점 좋은데 노출 부족, 마케팅 강화) · "
            "**좌하단** = 단종 검토"
        )


# ==========================================================
# 하단 — 액션 가이드
# ==========================================================
st.markdown("---")
with st.expander("📘 이 페이지 활용법", expanded=False):
    st.markdown("""
**① 감성 분포**
- 부정 비율 시간 트렌드 → 품질 이슈 조기 감지
- 별점 × 감성 매트릭스: 별점은 높은데 부정 키워드 있음 → 잠재 risk

**② 키워드 빈도**
- 긍정 키워드 = 마케팅 메시지로 활용 ("XX 가 좋아요" 강조)
- 부정 키워드 = 개선 우선순위

**③ 부정 리뷰 분석**
- 부정 키워드 top → 제품/서비스 개선 우선순위
- 상품별 부정 리뷰 수 → 어느 SKU 부터 손 봐야 하는지

**④ 상품별 ranking**
- 우상단 (리뷰 많고 별점 높음) = 베스트셀러 → 옵션 확장 후보
- 우하단 = 즉시 개선 시급 (매출은 나는데 만족도 낮음)

**감성 분류 로직 (참고)**
- 별점 4점 이상 + 긍정 키워드 → 긍정
- 별점 2점 이하 + 부정 키워드 → 부정
- 별점 3점 또는 키워드 충돌 → 중립
- (별점 없는 리뷰는 키워드만으로 판단)
    """)
