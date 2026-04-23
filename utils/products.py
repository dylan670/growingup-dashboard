"""제품 × 브랜드 매칭 + 집계 유틸.

제품명과 광고그룹명을 키워드 규칙으로 브랜드에 매핑합니다.
추후 '설정' 페이지에서 규칙 UI 조정 가능하게 확장 예정.
"""
from __future__ import annotations

import pandas as pd


# ==========================================================
# 브랜드 분류 규칙
#   - 순서대로 평가 (먼저 매칭되는 것 우선)
#   - keywords 중 하나라도 포함되면 해당 brand로 분류
# ==========================================================
BRAND_RULES: list[tuple[list[str], str]] = [
    (["번들"], "번들 상품"),
    (["김똑똑"], "김똑똑 (어린이김)"),
    (["떡뻥"], "똑똑떡뻥 (쌀과자)"),
    (["똑똑연구소"], "똑똑연구소 기타"),
    (["롤라루"], "롤라루"),
    (["캐리어", "백팩", "여행"], "롤라루 여행용품"),
]

# 제품 라인 → 운영 브랜드(우산) 매핑
UMBRELLA_BRANDS: dict[str, str] = {
    "김똑똑 (어린이김)": "똑똑연구소",
    "똑똑떡뻥 (쌀과자)": "똑똑연구소",
    "똑똑연구소 기타": "똑똑연구소",
    "번들 상품": "똑똑연구소",
    "롤라루": "롤라루",
    "롤라루 여행용품": "롤라루",
}


# ==========================================================
# 브랜드 ↔ 스토어(orders/ads의 store 컬럼 값) 매핑
# 홈 화면의 브랜드별 탭 필터링에 사용
# ==========================================================
BRAND_ORDER_STORES: dict[str, list[str]] = {
    # 똑똑연구소: 네이버 스마트스토어(똑똑) + 자사몰(똑똑) + 쿠팡(로켓그로스, 똑똑 제품)
    #   '쿠팡' 은 구버전 하위호환 (브랜드 분류 전 데이터)
    "똑똑연구소": ["똑똑연구소", "자사몰_똑똑연구소", "쿠팡", "쿠팡_똑똑연구소"],
    # 롤라루: 네이버(롤라) + 자사몰(롤라) + 쿠팡(로켓그로스, 롤라 제품)
    "롤라루":     ["롤라루", "자사몰_롤라루", "쿠팡_롤라루"],
    # 루티니스트: 현재 API 수집 없음 (구글 시트에서만 매출 가져옴)
    "루티니스트": [],
}

BRAND_AD_STORES: dict[str, list[str]] = {
    # 네이버 검색광고 (네이버_*) + Meta (자사몰_*) + 쿠팡 광고 (쿠팡_*)
    "똑똑연구소":  [
        "네이버", "네이버_똑똑연구소",
        "자사몰_똑똑연구소",
        "쿠팡_똑똑연구소",  # 쿠팡 광고 CSV
    ],
    "롤라루":      [
        "네이버_롤라루",
        "자사몰_롤라루",
        "쿠팡_롤라루",       # 쿠팡 광고 CSV (AI 광고)
    ],
    "루티니스트":  [],  # 광고 데이터 없음
}


# ==========================================================
# 네이버 검색광고 캠페인/광고그룹 → 브랜드 분류 규칙
# 사용자 운영 컨벤션 기준:
#   - KNS_ prefix = 똑똑연구소 (Kim똑똑 Naver Search 약자)
#   - RNS_ prefix = 롤라루 (Rolla Naver Search 약자)
#   - 그 외 키워드 매칭
# ==========================================================
NAVER_BRAND_KEYWORDS: dict[str, list[str]] = {
    # 순서 무관 (우선순위 동등). 매칭되는 첫 브랜드 반환.
    "똑똑연구소": ["김똑똑", "똑똑떡뻥", "떡뻥", "똑똑연구소", "KNS_"],
    "롤라루":     ["롤라루", "캐리어", "백팩", "여행용", "RNS_"],
}


# ==========================================================
# 쿠팡 광고 캠페인 → 브랜드 분류
# 사용자 제공 매핑:
#   - AI 광고 → 롤라루
#   - 김똑똑 로켓그로스 광고 세팅 260224_hans → 똑똑연구소
#   - 똑똑떡뻥 260122 → 똑똑연구소
#   - 김똑똑 어린이김 260114 → 똑똑연구소
# ==========================================================
COUPANG_AD_BRAND_KEYWORDS: dict[str, list[str]] = {
    # 키워드 매칭 기반 (대소문자 구분 X)
    "똑똑연구소": ["김똑똑", "똑똑떡뻥", "떡뻥", "똑똑연구소", "로켓그로스"],
    "롤라루":     ["AI 광고", "AI광고", "롤라루", "캐리어", "백팩"],
}


def classify_coupang_ad_to_brand(campaign_name: str) -> str:
    """쿠팡 광고 캠페인명 → 운영 브랜드(똑똑연구소/롤라루/공통).

    대소문자 무시. 공백 정규화 후 키워드 매칭.
    """
    n = (campaign_name or "").strip()
    if not n:
        return "공통"
    # 소문자 + 공백 정규화
    n_norm = n.lower().replace(" ", "")
    for brand, keywords in COUPANG_AD_BRAND_KEYWORDS.items():
        for kw in keywords:
            if kw.lower().replace(" ", "") in n_norm:
                return brand
    return "공통"


def classify_naver_to_brand(combined_name: str) -> str:
    """네이버 검색광고 캠페인+광고그룹 이름 → 운영 브랜드(똑똑연구소/롤라루/공통).

    Args:
        combined_name: '캠페인명 / 광고그룹명' 형태로 합친 문자열.
                       (둘 중 하나만 있어도 OK)
    Returns:
        '똑똑연구소' | '롤라루' | '공통' (매칭 실패)
    """
    n = (combined_name or "").strip()
    if not n:
        return "공통"
    for brand, keywords in NAVER_BRAND_KEYWORDS.items():
        if any(k in n for k in keywords):
            return brand
    return "공통"

# ==========================================================
# 월 매출 목표 — Google Sheets '2026 그로잉업팀 일간통계' 2026/04 기준 동기화
# 출처: https://docs.google.com/spreadsheets/d/1df0x5sTv5J2jw_GXiMcunMAtfVrjZ6rYQVYBRCdSO_0
#
# 주의: 대시보드는 자사몰(Cafe24) + 네이버 스마트스토어 + 쿠팡 만 API 로 수집합니다.
#       무신사 · 오프라인 · 이지웰 · 오늘의집 은 데이터 소스 미연결이므로
#       달성률 계산 대상에서 제외 (참고용 시트 원본 목표는 아래 _REFERENCE 에 보관).
# ==========================================================
BRAND_STORE_MONTHLY_TARGETS: dict[str, dict[str, int]] = {
    "똑똑연구소": {
        "똑똑연구소":        4_674_900,    # 네이버 스마트스토어 (스스)
        "자사몰_똑똑연구소":  6_405_000,    # Cafe24 자사몰
        "쿠팡_똑똑연구소":    6_118_290,    # 쿠팡 로켓그로스 (똑똑 제품)
    },
    "롤라루": {
        "롤라루":          40_092_000,    # 네이버 스마트스토어 (스스)
        "자사몰_롤라루":   66_306_000,    # Cafe24 자사몰 (주력)
        "쿠팡_롤라루":     10_794_000,    # 쿠팡 로켓배송 풀필먼트 (롤라 제품)
    },
    "루티니스트": {
        # API 수집 채널 없음 — 시트 데이터 기반으로만 집계
    },
}

# 브랜드별 월 매출 총 목표 (달성률 계산용 — 대시보드 수집 가능 채널만 합산)
BRAND_MONTHLY_TARGETS: dict[str, int] = {
    "똑똑연구소": sum(BRAND_STORE_MONTHLY_TARGETS["똑똑연구소"].values()),  # 17,198,190원
    "롤라루":     sum(BRAND_STORE_MONTHLY_TARGETS["롤라루"].values()),     # 117,192,000원
    "루티니스트": 3_000_000,  # 자사몰 월 300만 (시트 기준)
}

# 참고: 시트에 있지만 현재 대시보드 미수집 채널 (달성률 계산 제외)
BRAND_STORE_MONTHLY_TARGETS_REFERENCE: dict[str, dict[str, int]] = {
    "롤라루": {
        "무신사":      9_000_000,
        "오프라인":   15_420_000,
        "이지웰":     20_046_000,
        "오늘의집":    6_000_000,
    },
}


# ==========================================================
# 스토어 표시명 통일 (Google Sheets 용어 매칭)
#   시트 용어  →  대시보드 표시
#   자사몰 일 목표  →  자사몰       (Cafe24)
#   스스 일 목표    →  네이버 스마트스토어
#   쿠팡 일 목표    →  쿠팡
# ==========================================================
_STORE_DISPLAY_BRAND_SCOPED: dict[str, str] = {
    # 브랜드 탭 안에서 쓰는 간결한 이름 (브랜드 정보 생략)
    "자사몰_똑똑연구소":    "자사몰",
    "자사몰_롤라루":        "자사몰",
    "똑똑연구소":           "네이버 스마트스토어",
    "롤라루":               "네이버 스마트스토어",
    "쿠팡":                 "쿠팡",              # 구버전 호환
    "쿠팡_똑똑연구소":      "쿠팡",
    "쿠팡_롤라루":          "쿠팡",
}

_STORE_DISPLAY_GLOBAL: dict[str, str] = {
    # 전체 탭 등 브랜드 구분이 필요한 문맥
    "자사몰_똑똑연구소":    "자사몰 (똑똑)",
    "자사몰_롤라루":        "자사몰 (롤라루)",
    "똑똑연구소":           "네이버 스마트스토어 (똑똑)",
    "롤라루":               "네이버 스마트스토어 (롤라루)",
    "쿠팡":                 "쿠팡 (분류 전)",     # 구버전 호환
    "쿠팡_똑똑연구소":      "쿠팡 (똑똑)",
    "쿠팡_롤라루":          "쿠팡 (롤라루)",
}


def store_display_name(store: str, brand_context: str | None = None) -> str:
    """내부 store 값 → UI 표시명.

    Args:
        store: ads.csv / orders.csv 의 store 값 (예: '자사몰_똑똑연구소')
        brand_context: 현재 렌더링 중인 브랜드 탭 이름. None 이면 전체 탭.
    Returns:
        사용자 용어 기준 간결한 이름.
    """
    if brand_context:
        return _STORE_DISPLAY_BRAND_SCOPED.get(store, store)
    return _STORE_DISPLAY_GLOBAL.get(store, store)


def filter_orders_by_brand(orders_df: pd.DataFrame, brand: str) -> pd.DataFrame:
    """orders DataFrame을 운영 브랜드(똑똑연구소/롤라루/루티니스트)로 필터링.

    브랜드 매핑이 빈 리스트면 (예: 루티니스트) 빈 DataFrame 반환.
    """
    stores = BRAND_ORDER_STORES.get(brand, [])
    if brand in BRAND_ORDER_STORES and not stores:
        # 의도적 빈 매핑 (API 수집 대상 아님) → 빈 DF
        return orders_df.iloc[0:0]
    if not stores:
        return orders_df
    return orders_df[orders_df["store"].isin(stores)]


def filter_ads_by_brand(ads_df: pd.DataFrame, brand: str) -> pd.DataFrame:
    """ads DataFrame을 운영 브랜드로 필터링."""
    stores = BRAND_AD_STORES.get(brand, [])
    if brand in BRAND_AD_STORES and not stores:
        return ads_df.iloc[0:0]
    if not stores:
        return ads_df
    return ads_df[ads_df["store"].isin(stores)]


def classify_product(name: str | None) -> tuple[str, str]:
    """제품명 → (제품 라인, 운영 브랜드) 반환."""
    n = (name or "").strip()
    if not n:
        return "미분류", "미분류"
    for keywords, brand in BRAND_RULES:
        if any(k in n for k in keywords):
            return brand, UMBRELLA_BRANDS.get(brand, "미분류")
    return "기타", "기타"


def classify_adgroup(ag_name: str | None) -> tuple[str, str]:
    """네이버 광고그룹 이름 → (제품 라인, 운영 브랜드). 매칭 안 되면 ('공통', '공통')."""
    n = (ag_name or "").strip()
    if not n:
        return "공통", "공통"

    # 광고그룹도 동일 키워드 규칙 + 네이버 계정 prefix 고려
    # KNS_, GNS_, BNS_, RNS_ 등 prefix는 의미가 있지만 제품 라인은 뒤 이름에서 판단
    for keywords, brand in BRAND_RULES:
        if any(k in n for k in keywords):
            return brand, UMBRELLA_BRANDS.get(brand, "공통")

    return "공통", "공통"


def classify_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """orders DataFrame에 brand, umbrella 컬럼 추가해서 반환."""
    df = orders_df.copy()
    if "product" in df.columns:
        brand_series = df["product"].apply(classify_product)
        df["brand"] = brand_series.apply(lambda x: x[0])
        df["umbrella"] = brand_series.apply(lambda x: x[1])
    else:
        df["brand"] = "미분류"
        df["umbrella"] = "미분류"
    return df


def aggregate_by_product(orders_df: pd.DataFrame) -> pd.DataFrame:
    """제품별 집계 (주문, 수량, 매출, 고객수, 판매 채널)."""
    df = classify_orders(orders_df)
    if df.empty:
        return pd.DataFrame()

    agg = df.groupby(["umbrella", "brand", "product"]).agg(
        orders=("order_id", "count"),
        quantity=("quantity", "sum"),
        revenue=("revenue", "sum"),
        customers=("customer_id", "nunique"),
        channels=("channel", lambda x: ", ".join(sorted(set(x)))),
    ).reset_index().sort_values("revenue", ascending=False)
    return agg


def aggregate_by_umbrella(orders_df: pd.DataFrame) -> pd.DataFrame:
    """운영 브랜드(우산) 레벨 집계."""
    df = classify_orders(orders_df)
    if df.empty:
        return pd.DataFrame()

    agg = df.groupby("umbrella").agg(
        orders=("order_id", "count"),
        revenue=("revenue", "sum"),
        customers=("customer_id", "nunique"),
        products=("product", lambda x: x.nunique()),
    ).reset_index().sort_values("revenue", ascending=False)
    return agg


def attribute_naver_ad_spend(adgroup_df: pd.DataFrame) -> pd.DataFrame:
    """네이버 광고그룹 집계 DataFrame에 brand/umbrella 컬럼 추가.

    Args:
        adgroup_df: naver_insights.fetch_breakdown('adgroup', ...) 결과
                    ('이름', '비용', '노출', '클릭' 등 컬럼 포함)
    """
    df = adgroup_df.copy()
    if "이름" in df.columns:
        brand_series = df["이름"].apply(classify_adgroup)
        df["brand"] = brand_series.apply(lambda x: x[0])
        df["umbrella"] = brand_series.apply(lambda x: x[1])
    else:
        df["brand"] = "공통"
        df["umbrella"] = "공통"
    return df
