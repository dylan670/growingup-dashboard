"""제품 이미지 캐시 관리.

네이버 커머스 API로 각 스토어의 상품을 조회해 이름 ↔ 이미지 URL 매핑을 캐시합니다.
대시보드는 주문 데이터의 product 이름으로 이 캐시를 fuzzy match.
"""
from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd


DATA_DIR = Path(__file__).parent.parent / "data"
IMAGES_CSV = DATA_DIR / "product_images.csv"


def extract_image_rows(products: list[dict], store: str) -> list[dict]:
    """네이버 Commerce products API 응답 → (name, image_url) 행 리스트."""
    rows: list[dict] = []
    for p in products:
        for cp in p.get("channelProducts", []):
            name = (cp.get("name") or "").strip()
            img_obj = cp.get("representativeImage") or {}
            img_url = img_obj.get("url") if isinstance(img_obj, dict) else ""
            if not (name and img_url):
                continue
            rows.append({
                "store": store,
                "origin_product_no": p.get("originProductNo"),
                "channel_product_no": cp.get("channelProductNo"),
                "name": name,
                "image_url": img_url,
                "sale_price": cp.get("salePrice"),
                "category": cp.get("wholeCategoryName", ""),
            })
    return rows


def refresh_naver_image_cache() -> int:
    """네이버 두 스토어 상품 이미지 캐시 갱신. 반환: 캐시된 총 상품 수."""
    from api.naver_commerce import load_commerce_clients_from_env

    clients = load_commerce_clients_from_env()
    if not clients:
        return 0

    all_rows: list[dict] = []
    for store, client in clients.items():
        try:
            products = client.get_products()
        except Exception as e:
            print(f"[WARN] {store} 상품 조회 실패: {e}")
            continue
        rows = extract_image_rows(products, store)
        all_rows.extend(rows)

    # 기존 캐시에서 네이버 스토어 행만 교체 (쿠팡 캐시 보존)
    existing = load_image_cache()
    if not existing.empty:
        existing = existing[~existing["store"].isin(list(clients.keys()))]
    df = pd.concat([existing, pd.DataFrame(all_rows)], ignore_index=True) \
        if all_rows else existing

    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(IMAGES_CSV, index=False, encoding="utf-8-sig")
    return len(all_rows)


def refresh_coupang_image_cache(progress_cb=None) -> int:
    """쿠팡 상품 이미지 캐시 갱신."""
    from api.coupang_wing import load_coupang_client_from_env

    client = load_coupang_client_from_env()
    if client is None:
        return 0

    try:
        results = client.get_product_images(progress_cb=progress_cb)
    except Exception as e:
        print(f"[WARN] 쿠팡 상품 조회 실패: {e}")
        return 0

    rows = [{
        "store": "쿠팡",
        "origin_product_no": None,
        "channel_product_no": r.get("seller_product_id"),
        "name": r["name"],
        "image_url": r["image_url"],
        "sale_price": None,
        "category": "",
    } for r in results]

    # 기존 캐시에서 쿠팡 행만 교체
    existing = load_image_cache()
    if not existing.empty:
        existing = existing[existing["store"] != "쿠팡"]
    df = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True) \
        if rows else existing

    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(IMAGES_CSV, index=False, encoding="utf-8-sig")
    return len(rows)


def refresh_cafe24_image_cache() -> int:
    """Cafe24 자사몰 상품 이미지 캐시 갱신 (스토어별)."""
    from api.cafe24 import load_all_cafe24_clients

    clients = load_all_cafe24_clients()
    if not clients:
        return 0

    all_rows: list[dict] = []
    for store_name, client in clients.items():
        try:
            results = client.get_product_images()
        except Exception as e:
            print(f"[WARN] {store_name} Cafe24 상품 조회 실패: {e}")
            continue
        for r in results:
            all_rows.append({
                "store": store_name,
                "origin_product_no": r.get("product_no"),
                "channel_product_no": r.get("product_no"),
                "name": r["name"],
                "image_url": r["image_url"],
                "sale_price": None,
                "category": "",
            })

    # 기존 캐시에서 자사몰_* 행만 교체
    existing = load_image_cache()
    if not existing.empty:
        existing = existing[~existing["store"].astype(str).str.startswith("자사몰_", na=False)]
    df = pd.concat([existing, pd.DataFrame(all_rows)], ignore_index=True) \
        if all_rows else existing

    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(IMAGES_CSV, index=False, encoding="utf-8-sig")
    return len(all_rows)


def refresh_all_image_cache(progress_cb=None) -> dict[str, int]:
    """모든 채널/스토어 이미지 캐시 갱신."""
    counts = {}
    counts["naver"] = refresh_naver_image_cache()
    counts["coupang"] = refresh_coupang_image_cache(progress_cb=progress_cb)
    counts["cafe24"] = refresh_cafe24_image_cache()
    return counts


def load_image_cache() -> pd.DataFrame:
    """저장된 이미지 캐시 로드. 없으면 빈 DataFrame."""
    if not IMAGES_CSV.exists():
        return pd.DataFrame(
            columns=["store", "origin_product_no", "channel_product_no",
                     "name", "image_url", "sale_price", "category"]
        )
    return pd.read_csv(IMAGES_CSV)


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _normalize(s: str) -> str:
    """유사도 비교용 기본 정제: 소문자, 공백/기호 제거."""
    import re
    return re.sub(r"[\s\-_·,/()\[\]]+", "", (s or "").lower())


def find_image(order_product_name: str,
               cache_df: pd.DataFrame,
               min_ratio: float = 0.4) -> str | None:
    """주문 product 이름 ↔ 이미지 캐시에서 최적 매칭 이미지 URL 찾기."""
    if cache_df.empty or not order_product_name:
        return None

    normalized_target = _normalize(order_product_name)
    if not normalized_target:
        return None

    best_ratio = 0.0
    best_url = None

    # 핵심 키워드가 들어있으면 우선 순위 높임 (김똑똑 / 떡뻥 / 롤라루 등)
    KEYWORDS = ["김똑똑", "똑똑떡뻥", "떡뻥", "롤라루", "캐리어", "백팩"]
    target_keywords = {k for k in KEYWORDS if k in order_product_name}

    for _, row in cache_df.iterrows():
        cache_name = str(row.get("name", ""))
        if not cache_name:
            continue

        ratio = _similarity(_normalize(cache_name), normalized_target)

        # 공통 키워드가 있으면 +0.15 보너스
        if target_keywords:
            shared = target_keywords & {k for k in KEYWORDS if k in cache_name}
            if shared:
                ratio += 0.15

        if ratio > best_ratio:
            best_ratio = ratio
            best_url = row.get("image_url")

    if best_ratio >= min_ratio:
        return best_url
    return None


def build_image_lookup(unique_product_names: list[str],
                       cache_df: pd.DataFrame) -> dict[str, str | None]:
    """주문 데이터의 고유 product 이름 → image_url 매핑 (한 번 계산)."""
    result: dict[str, str | None] = {}
    for name in unique_product_names:
        result[name] = find_image(name, cache_df)
    return result


# ==========================================================
# Store fallback 매핑 — 주문 store 값이 이미지 캐시 store 와 다를 때
# 예: orders 의 '쿠팡_똑똑연구소' / '쿠팡_롤라루' → cache 의 '쿠팡'
# ==========================================================
_STORE_CACHE_FALLBACK: dict[str, list[str]] = {
    # 주문 store → 검색할 캐시 store 후보 리스트 (자기 자신 제외)
    "쿠팡_똑똑연구소": ["쿠팡"],
    "쿠팡_롤라루":     ["쿠팡"],
    "쿠팡":           ["쿠팡_똑똑연구소", "쿠팡_롤라루"],  # 역방향도 허용
}


def build_store_scoped_lookup(
    orders_df: pd.DataFrame,
    cache_df: pd.DataFrame,
    min_ratio: float = 0.4,
) -> dict[tuple[str, str], str | None]:
    """(store, product_name) → image_url 매핑.

    store별로 이미지 캐시를 분리 매칭 → 쿠팡 주문이 네이버 상품에 매칭되는
    false positive 방지. 단 쿠팡 계열처럼 store 값이 분기된 경우
    _STORE_CACHE_FALLBACK 을 통해 구버전 store 캐시도 함께 검색.
    """
    if orders_df.empty or cache_df.empty:
        return {}

    lookup: dict[tuple[str, str], str | None] = {}
    for store in orders_df["store"].dropna().unique():
        # 우선 자기 store + fallback store 캐시 합쳐서 검색
        candidate_stores = [store] + _STORE_CACHE_FALLBACK.get(store, [])
        store_cache = cache_df[cache_df["store"].isin(candidate_stores)]

        products = orders_df[orders_df["store"] == store]["product"].dropna().unique()

        if store_cache.empty:
            for p in products:
                lookup[(store, p)] = None
            continue

        for p in products:
            lookup[(store, p)] = find_image(
                str(p), store_cache, min_ratio=min_ratio,
            )
    return lookup
