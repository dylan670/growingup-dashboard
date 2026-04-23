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


# ==========================================================
# 대시보드 집계에서 제외할 제품 키워드 (타 브랜드 제품 등)
# 제품명에 이 중 하나라도 포함되면 orders.csv 병합 시 스킵
# ==========================================================
PRODUCT_BLOCKLIST_KEYWORDS: list[str] = [
    "오즈키즈",      # OZKIZ — 그로잉업팀 운영 아닌 타 브랜드
]


def is_blocked_product(name: str | None) -> bool:
    """상품명이 blocklist 에 해당하면 True — 데이터 병합 시 스킵 용.

    기본 PRODUCT_BLOCKLIST_KEYWORDS + 설정 페이지 Override 통합.
    """
    n = (name or "").strip()
    if not n:
        return False
    all_keywords = set(PRODUCT_BLOCKLIST_KEYWORDS) | set(_load_blocklist_override())
    return any(kw in n for kw in all_keywords)


# ==========================================================
# 제품명 정규화 규칙 — 채널별로 흩어진 상품명을 모델명으로 통일
#   예: "롤라루 오프너 확장형 기내용 캐리어 다크그린 20" → "오프너"
#       "똑똑연구소 김똑똑 어린이김 30개 담백한맛 2g" → "김똑똑 어린이김 30봉"
#
# 동작: 패턴(substring)이 상품명에 포함되면 canonical 로 치환.
# 순서: 긴/구체적 패턴 먼저 → 짧은 키워드는 fallback
# ==========================================================
PRODUCT_NAME_RULES: list[tuple[str, str]] = [
    # ---- 똑똑연구소 긴 문구 우선 (구체적 → 일반 순서) ----
    # 모양김 세트 (긴 수식구 먼저)
    ("똑똑연구소 김똑똑 저염 조미 도시락김 맛있는 구운 모양김 동물모양 알파벳 한글모양", "김똑똑 어린이 모양김 세트"),
    ("똑똑연구소 김똑똑 저염 조미 도시락김 맛있는 구운 모양김", "김똑똑 어린이 모양김 세트"),
    # 김똑똑 어린이김 N봉
    ("똑똑연구소 김똑똑 어린이김 240개 담백한맛 2g", "김똑똑 어린이김 240봉(1박스)"),
    ("똑똑연구소 김똑똑 어린이김 60개 담백한맛 2g", "김똑똑 어린이김 60봉"),
    ("똑똑연구소 김똑똑 어린이김 30개 담백한맛 2g", "김똑똑 어린이김 30봉"),
    ("김똑똑 어린이김 저염 밥태기 아기반찬 구운 도시락 아기김 2g 60봉", "김똑똑 어린이김 60봉"),
    ("김똑똑 어린이김 저염 밥태기 아기반찬 구운 도시락 아기김 2g, 60개", "김똑똑 어린이김 60봉"),
    ("김똑똑 맛있는 아기김 구운 조미 어린이 저염 유아 김 2gx30봉", "김똑똑 어린이김 240봉(1박스)"),
    ("김똑똑 맛있는 아기김 구운 조미 어린이 저염 유아김 2g, 30개", "김똑똑 어린이김 30봉"),
    ("김똑똑 어린이 미니 도시락김 저염 아기김 2gX60봉", "김똑똑 어린이김 60봉"),
    # 똑똑떡뻥 4봉 (긴 variant 먼저)
    ("똑똑연구소 유기농 쌀과자 똑똑떡뻥 30gX4봉 무첨가 아기과자 백미 떡뻥", "똑똑떡뻥 4봉"),
    ("똑똑연구소 유기농 쌀과자 똑똑떡뻥 30g 무첨가 아기과자 백미 떡뻥 30g 4개 백미맛", "똑똑떡뻥 4봉"),
    ("똑똑연구소 유기농 쌀과자 똑똑떡뻥 30g 무첨가 아기과자 백미 떡뻥", "똑똑떡뻥 4봉"),
    ("똑똑연구소 유기농 쌀과자 똑똑 떡뻥 30g, 4개", "똑똑떡뻥 4봉"),
    ("유기농 쌀과자 똑똑떡뻥 4봉", "똑똑떡뻥 4봉"),
    # ---- 롤라루 긴 문구 → 모델명 ----
    ("기내용 캐리어 여행 전면오픈 기내반입 가벼운 튼튼한 소형 51cm(20인치)", "오프너"),
    ("71cm(28인치) 81cm(32인치) 캐리어 대형 수화물 여행용 특대형 화물용 중대형", "큐보이드"),
    ("여행용 캐리어 51cm(20인치) 기내용 캐리어 기내 반입 여행 66cm(26인치) 수화물용", "스마트"),
    ("기내용 전면오픈 55cm(20인치) 확장형 튼튼한 여행용 캐리어 65cm(24인치)", "인딥"),
    ("여행용 백팩 캐리어 기내반입 크로스백 남자 노트북 방수 기내용 가방 보부상 백패킹 46cm", "플렉스"),
    ("기내용캐리어 55cm(20인치) 65cm(24인치) 전면오픈 원터치 캐리어", "스파클링"),
    ("롤라루 기내용 캐리어 50cm(20인치) 수하물 알루미늄 캐리어 여행용", "프레임"),
    ("여행용 대형 컵홀더 캐리어 수화물용 PC캐리어 60cm(22인치) 66cm(26인치) 75cm(30인치)", "퀘스트"),
    ("롤라루 구름쿠션 캐리어 바퀴커버", "구름쿠션 바퀴커버"),
    ("롤라루 캐리어네임택 러기지택 캐리어 택 이름표 꼬리표 가방", "러기지택"),
    # ---- 짧은 키워드 fallback (오타 보정 + 모델명 표준화) ----
    ("큐포이드", "큐보이드"),          # 오타 보정
    ("큐보이드", "큐보이드"),
    ("스파클링", "스파클링"),
    ("플렉스", "플렉스"),
    ("오프너", "오프너"),
    ("스마트", "스마트"),
    ("플라이더", "플라이더"),
    ("인딥", "인딥"),
    ("이지프레스", "이지프레스"),
    ("롤링프레스", "롤링프레스"),
]


# ==========================================================
# Override 규칙 로드 — 설정 페이지에서 편집 가능
# (data/product_name_rules_override.csv 가 있으면 PRODUCT_NAME_RULES 앞에 삽입)
# ==========================================================
_OVERRIDE_CACHE: dict = {"rules": None, "mtime": 0}


def _load_override_rules() -> list[tuple[str, str]]:
    """사용자가 설정 페이지에서 저장한 규칙 로드 (CSV 기반).

    mtime 기반 간이 캐시 — 파일 수정 시 자동 재로드.
    """
    from pathlib import Path as _Path
    p = _Path(__file__).parent.parent / "data" / "product_name_rules_override.csv"
    if not p.exists():
        return []
    try:
        mtime = p.stat().st_mtime
        if _OVERRIDE_CACHE["mtime"] == mtime and _OVERRIDE_CACHE["rules"] is not None:
            return _OVERRIDE_CACHE["rules"]
        df = pd.read_csv(p)
        rules = []
        for _, r in df.iterrows():
            pat = str(r.get("pattern", "")).strip()
            can = str(r.get("canonical", "")).strip()
            if pat and can:
                rules.append((pat, can))
        _OVERRIDE_CACHE["rules"] = rules
        _OVERRIDE_CACHE["mtime"] = mtime
        return rules
    except Exception:
        return []


def _load_blocklist_override() -> list[str]:
    """설정 페이지에서 저장한 차단 키워드 로드."""
    from pathlib import Path as _Path
    p = _Path(__file__).parent.parent / "data" / "product_blocklist_override.csv"
    if not p.exists():
        return []
    try:
        df = pd.read_csv(p)
        return [str(k).strip() for k in df["keyword"].dropna().tolist() if str(k).strip()]
    except Exception:
        return []


def normalize_product_name(name: str | None) -> str:
    """상품명 정규화 — Override 규칙 → 기본 규칙 순서로 매칭.

    매칭되지 않으면 원본 반환 (빈 문자열/None 은 그대로).
    """
    if not name:
        return name or ""
    s = str(name)
    # 1) 사용자 Override 규칙 우선 (설정 페이지에서 편집)
    for pattern, canonical in _load_override_rules():
        if pattern in s:
            return canonical
    # 2) 코드 내장 기본 규칙
    for pattern, canonical in PRODUCT_NAME_RULES:
        if pattern in s:
            return canonical
    return s

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
    # 쿠팡 벤더 발주 (로켓배송 B2B — 실제 소비자 판매 아님)
    "쿠팡_똑똑연구소_벤더": "쿠팡 벤더 발주",
    "쿠팡_롤라루_벤더":     "쿠팡 벤더 발주",
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
    "쿠팡_똑똑연구소_벤더": "쿠팡 벤더 발주 (똑똑)",
    "쿠팡_롤라루_벤더":     "쿠팡 벤더 발주 (롤라루)",
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
    """orders DataFrame에 brand, umbrella 컬럼 추가해서 반환.

    - 제품명 정규화 (PRODUCT_NAME_RULES) 자동 적용 → product 컬럼 치환
    - 제품명 키워드 매칭 실패 시(기타/미분류) store 기반 fallback 적용
    """
    df = orders_df.copy()
    if "product" in df.columns:
        # 1) 원본 product 로 브랜드 분류 (롤라루/똑똑연구소/캐리어 등 키워드 풍부)
        brand_series = df["product"].apply(classify_product)
        df["brand"] = brand_series.apply(lambda x: x[0])
        df["umbrella"] = brand_series.apply(lambda x: x[1])
        # 2) 분류 후 product 이름만 정규화 (표시·집계용 — 모델명 기준 통일)
        df["product"] = df["product"].apply(normalize_product_name)
    else:
        df["brand"] = "미분류"
        df["umbrella"] = "미분류"

    # ---- store → umbrella fallback ----
    # 매칭 실패한 행에 대해 store 값으로 umbrella 유추
    if "store" in df.columns:
        # BRAND_ORDER_STORES 역방향 매핑 + 벤더 발주 store 추가
        store_to_umbrella: dict[str, str] = {}
        for umb, stores in BRAND_ORDER_STORES.items():
            for s in stores:
                store_to_umbrella[s] = umb
        # 쿠팡 벤더 발주 store (BRAND_ORDER_STORES 에 없지만 브랜드 분류 필요)
        store_to_umbrella["쿠팡_똑똑연구소_벤더"] = "똑똑연구소"
        store_to_umbrella["쿠팡_롤라루_벤더"] = "롤라루"

        mask = df["umbrella"].isin(["기타", "미분류", "공통"])
        for idx in df[mask].index:
            store = df.at[idx, "store"]
            if store in store_to_umbrella:
                df.at[idx, "umbrella"] = store_to_umbrella[store]
                if df.at[idx, "brand"] in ["기타", "미분류", "공통"]:
                    df.at[idx, "brand"] = f"{store_to_umbrella[store]} 옵션/기타"
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
