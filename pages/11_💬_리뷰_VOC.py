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
    page_title="리뷰 VOC",
    page_icon="💬",
    header_title="💬 리뷰 감성 · 키워드 VOC",
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
    for kw in ["롤라루", "캐리어", "여행", "기내용"]:
        if kw in p:
            return "롤라루"
    # 루티니스트 키워드
    for kw in ["루티니", "런닝", "러닝", "운동복"]:
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
# 사이드바 필터
# ==========================================================
st.sidebar.markdown("#### 🔎 필터")

brand_options = ["전체"] + sorted(enriched["brand"].unique().tolist())
selected_brand = st.sidebar.selectbox("브랜드", brand_options, index=0)

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
# TAB 4 — 상품별 ranking (별점/리뷰 수)
# ==========================================================
with tab_prod:
    st.markdown("##### 🏷 상품별 별점 & 리뷰량 ranking")

    prod_agg = (
        filtered.groupby(["brand", "product"])
        .agg(
            리뷰수=("text", "count"),
            평균별점=("rating", "mean"),
            긍정수=("감성", lambda s: (s == "긍정").sum()),
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

    prod_agg = prod_agg.sort_values(
        ["리뷰수", "평균별점"], ascending=[False, False]
    )

    # 표시
    st.dataframe(
        prod_agg[
            ["brand", "product", "리뷰수", "평균별점",
             "긍정수", "부정수", "긍정률%", "부정률%"]
        ],
        hide_index=True, width="stretch",
        height=min(500, 60 + len(prod_agg) * 36),
        column_config={
            "긍정률%": st.column_config.ProgressColumn(
                "긍정률", format="%.1f%%", min_value=0, max_value=100,
            ),
            "부정률%": st.column_config.ProgressColumn(
                "부정률", format="%.1f%%", min_value=0, max_value=100,
            ),
        },
    )

    # 그래프 — 리뷰 수 × 평균 별점 산점도
    if len(prod_agg) > 1:
        st.markdown("##### 📊 리뷰량 × 별점 산점도")
        fig = px.scatter(
            prod_agg, x="리뷰수", y="평균별점",
            size="리뷰수", color="brand",
            color_discrete_map={
                b: BRAND_COLORS.get(b, {}).get("primary", "#94a3b8")
                for b in prod_agg["brand"].unique()
            },
            hover_data=["product", "긍정률%", "부정률%"],
        )
        fig.update_layout(
            height=400, margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="리뷰 수", yaxis_title="평균 별점",
        )
        # 기준선
        fig.add_hline(y=4.0, line_dash="dot", line_color="#94a3b8",
                      annotation_text="별점 4.0")
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "💡 우상단 = 리뷰 많고 별점 높음 (베스트셀러) · "
            "우하단 = 리뷰 많지만 별점 낮음 (개선 시급) · "
            "좌측 = 리뷰 적은 SKU (마케팅 부족 또는 신상품)"
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
