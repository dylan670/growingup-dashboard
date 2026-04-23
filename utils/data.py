"""데이터 로딩 및 샘플 데이터 생성."""
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

ADS_FILE = DATA_DIR / "ads.csv"
ORDERS_FILE = DATA_DIR / "orders.csv"
REVIEWS_FILE = DATA_DIR / "reviews.csv"
# 쿠팡 벤더 발주 전용 (로켓배송 B2B — 실 소비자 판매 아님, 제품 분석에만 반영)
COUPANG_INBOUND_FILE = DATA_DIR / "coupang_inbound.csv"

CHANNELS = ["네이버", "쿠팡", "자사몰"]

# 그로잉업팀 산하 스토어 (스마트스토어는 2개로 분리)
STORES = ["똑똑연구소", "롤라루", "쿠팡", "자사몰"]

PRODUCTS = [
    {"name": "김똑똑 어린이김", "price": 15000, "store": "똑똑연구소"},
    {"name": "똑똑떡뻥 오리지널", "price": 9900, "store": "똑똑연구소"},
    {"name": "똑똑떡뻥 야채맛", "price": 9900, "store": "똑똑연구소"},
    {"name": "김똑똑+떡뻥 번들", "price": 28000, "store": "똑똑연구소"},
    {"name": "롤라루 A", "price": 18000, "store": "롤라루"},
    {"name": "롤라루 B", "price": 12000, "store": "롤라루"},
]

_CHANNEL_PARAMS = {
    "네이버": {"daily_spend": 12000, "cpc": 350, "ctr": 0.030, "cvr": 0.050},
    "쿠팡":   {"daily_spend": 13000, "cpc": 300, "ctr": 0.040, "cvr": 0.070},
    "자사몰": {"daily_spend": 8000,  "cpc": 480, "ctr": 0.020, "cvr": 0.025},
}

_SAMPLE_REVIEWS = [
    ("네이버", 5, "24개월 아기가 너무 잘 먹어요. 김이 바삭하고 간이 세지 않아서 좋아요."),
    ("네이버", 5, "유기농이라 믿고 주는 간식. 떡뻥이 쌀맛이 진해서 애기가 좋아해요."),
    ("네이버", 4, "맛은 좋은데 가격이 살짝 부담. 그래도 성분 보면 납득."),
    ("쿠팡", 4, "포장이 깔끔하고 배송 빠름. 다만 양이 조금만 더 있으면 좋겠어요."),
    ("쿠팡", 5, "우리 딸 최애 간식. 재구매 3번째! 식감이 부드러워서 이제 막 이유식 뗀 아기도 OK."),
    ("쿠팡", 5, "알레르기 걱정 없이 줄 수 있어서 좋아요. 선물용으로도 구매."),
    ("쿠팡", 5, "돌아기 간식으로 딱. 손에 안 묻어서 외출할 때 편해요."),
    ("쿠팡", 3, "떡뻥이 생각보다 작은 사이즈. 맛은 괜찮아요."),
    ("자사몰", 5, "구독 신청했어요. 매달 새 맛 출시되면 좋겠어요."),
    ("자사몰", 5, "공식몰이라 이벤트 구성 좋음. 김+떡뻥 번들이 가성비 굿."),
]


def generate_sample_data(days: int = 60) -> None:
    """샘플 데이터 3종(광고/주문/리뷰)을 data/ 폴더에 저장.

    스토리 내장:
      - 최근 7일 자사몰 CTR 급락 (소재 피로도 시뮬레이션)
      - 최근 3일 쿠팡 CVR 급등 (예산 증액 기회 시뮬레이션)
    """
    rng = np.random.default_rng(42)
    end_date = datetime(2026, 4, 20)
    start_date = end_date - timedelta(days=days - 1)

    ads_rows: list[dict] = []
    orders_rows: list[dict] = []
    customer_pool: dict[str, list[str]] = {ch: [] for ch in CHANNELS}
    order_id = 1
    customer_id = 1

    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")

        for ch, p in _CHANNEL_PARAMS.items():
            ctr_multi = 0.55 if (ch == "자사몰" and i >= days - 7) else 1.0
            cvr_multi = 1.5 if (ch == "쿠팡" and i >= days - 3) else 1.0

            spend = max(3000, p["daily_spend"] * float(rng.normal(1.0, 0.15)))
            cpc_actual = max(50, p["cpc"] * float(rng.normal(1.0, 0.10)))
            clicks = max(10, int(spend / cpc_actual))

            ctr = max(0.005, p["ctr"] * ctr_multi)
            impressions = int(clicks / ctr)

            cvr = p["cvr"] * cvr_multi
            # Poisson 분포로 전환수 생성 (int 절단 문제 회피)
            conversions = int(rng.poisson(clicks * cvr))

            ad_revenue = 0
            for _ in range(conversions):
                if customer_pool[ch] and rng.random() < 0.30:
                    cust = customer_pool[ch][int(rng.integers(0, len(customer_pool[ch])))]
                else:
                    cust = f"C-{customer_id:05d}"
                    customer_id += 1
                    customer_pool[ch].append(cust)

                # 네이버 채널: 똑똑연구소 70% / 롤라루 30% 비중으로 분기
                if ch == "네이버":
                    brand = "똑똑연구소" if rng.random() < 0.70 else "롤라루"
                    brand_products = [p for p in PRODUCTS if p["store"] == brand]
                    prod = brand_products[int(rng.integers(0, len(brand_products)))]
                    store_val = brand
                else:
                    # 쿠팡/자사몰: 똑똑연구소 제품 위주 (현실 반영)
                    ddok_products = [p for p in PRODUCTS if p["store"] == "똑똑연구소"]
                    prod = ddok_products[int(rng.integers(0, len(ddok_products)))]
                    store_val = ch

                qty = int(rng.choice([1, 2, 3], p=[0.6, 0.3, 0.1]))
                line_rev = prod["price"] * qty
                ad_revenue += line_rev

                orders_rows.append({
                    "date": date_str,
                    "order_id": f"O-{order_id:06d}",
                    "customer_id": cust,
                    "channel": ch,
                    "store": store_val,
                    "product": prod["name"],
                    "quantity": qty,
                    "revenue": line_rev,
                })
                order_id += 1

            ads_rows.append({
                "date": date_str,
                "channel": ch,
                "spend": round(spend),
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": ad_revenue,
            })

    pd.DataFrame(ads_rows).to_csv(ADS_FILE, index=False, encoding="utf-8-sig")
    pd.DataFrame(orders_rows).to_csv(ORDERS_FILE, index=False, encoding="utf-8-sig")

    reviews_rows: list[dict] = []
    for i in range(days):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for _ in range(int(rng.poisson(3))):
            ch, rating, text = _SAMPLE_REVIEWS[int(rng.integers(0, len(_SAMPLE_REVIEWS)))]
            prod = PRODUCTS[int(rng.integers(0, len(PRODUCTS)))]
            reviews_rows.append({
                "date": date_str,
                "channel": ch,
                "product": prod["name"],
                "rating": rating,
                "text": text,
            })
    pd.DataFrame(reviews_rows).to_csv(REVIEWS_FILE, index=False, encoding="utf-8-sig")


def _load_or_generate(file: Path, date_col: str = "date") -> pd.DataFrame:
    if not file.exists():
        generate_sample_data()
    df = pd.read_csv(file)
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
    return df


def load_ads() -> pd.DataFrame:
    return _load_or_generate(ADS_FILE)


def load_orders() -> pd.DataFrame:
    df = _load_or_generate(ORDERS_FILE)
    # 구 스키마 ('store' 컬럼 없음) → 마이그레이션 후 저장
    if "store" not in df.columns:
        df = _add_store_column(df)
        df.to_csv(ORDERS_FILE, index=False, encoding="utf-8-sig")

    # 제품명 정규화 — 모든 페이지에서 일관된 제품명 표시 (긴 SKU 이름 → 모델명)
    # 원본 CSV 는 그대로 두고, 메모리상 로드 시점에만 치환
    if "product" in df.columns:
        try:
            from utils.products import normalize_product_name
            df["product"] = df["product"].apply(normalize_product_name)
        except Exception:
            pass
    return df


def _add_store_column(df: pd.DataFrame) -> pd.DataFrame:
    """기존 orders DF에 store 컬럼 추가 (마이그레이션).

    - 쿠팡 / 자사몰: store = channel
    - 네이버: 똑똑연구소 70% / 롤라루 30% 랜덤 분기 (seed 42로 결정론적)
    """
    df = df.copy()
    df["store"] = df["channel"]

    naver_mask = df["channel"] == "네이버"
    if naver_mask.any():
        rng = np.random.default_rng(42)
        df.loc[naver_mask, "store"] = rng.choice(
            ["똑똑연구소", "롤라루"],
            size=int(naver_mask.sum()),
            p=[0.70, 0.30],
        )
    return df


def load_reviews() -> pd.DataFrame:
    return _load_or_generate(REVIEWS_FILE)


def load_coupang_inbound() -> pd.DataFrame:
    """쿠팡 벤더 발주 데이터 로드 (있으면 반환, 없으면 빈 DF).

    컬럼: date, order_id, customer_id, channel, store, product,
          quantity, revenue (orders.csv 와 동일 스키마)
    store 는 '쿠팡_*_벤더' 형식으로 실 소비자 판매와 분리.
    제품명 정규화 자동 적용 (load_orders 와 동일).
    """
    if not COUPANG_INBOUND_FILE.exists():
        return pd.DataFrame(columns=[
            "date", "order_id", "customer_id", "channel",
            "store", "product", "quantity", "revenue",
        ])
    df = pd.read_csv(COUPANG_INBOUND_FILE)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    # 제품명 정규화 (모든 페이지 일관성)
    if "product" in df.columns:
        try:
            from utils.products import normalize_product_name
            df["product"] = df["product"].apply(normalize_product_name)
        except Exception:
            pass
    return df


def reset_and_generate() -> None:
    for f in [ADS_FILE, ORDERS_FILE, REVIEWS_FILE]:
        if f.exists():
            f.unlink()
    generate_sample_data()


def merge_naver_brand_ads(new_df: pd.DataFrame) -> tuple[int, int]:
    """네이버 검색광고 브랜드별 store 행 병합.

    new_df 는 store='네이버_똑똑연구소' / '네이버_롤라루' 를 모두 포함할 수 있음.
    기존 ads.csv 에서:
      - store 가 '네이버' / '네이버_똑똑연구소' / '네이버_롤라루' 인 행 중
        new_df 에 포함된 날짜 해당 분을 전부 제거 후 교체.
      - 구버전 store='네이버' 는 자동 정리됨 (브랜드별 행으로 치환).
    """
    if new_df.empty:
        return 0, 0

    new_dates = set(new_df["date"].astype(str))
    naver_stores = {"네이버", "네이버_똑똑연구소", "네이버_롤라루"}

    if ADS_FILE.exists():
        existing = pd.read_csv(ADS_FILE)
        existing["date"] = existing["date"].astype(str)
        if "store" not in existing.columns:
            existing["store"] = existing["channel"]

        mask = (
            existing["store"].isin(naver_stores)
            & existing["date"].isin(new_dates)
        )
        removed = int(mask.sum())
        existing = existing[~mask]
    else:
        existing = pd.DataFrame()
        removed = 0

    merged = pd.concat([existing, new_df], ignore_index=True)
    if "store" not in merged.columns:
        merged["store"] = merged["channel"]
    merged = merged.sort_values(["date", "channel", "store"]).reset_index(drop=True)
    merged.to_csv(ADS_FILE, index=False, encoding="utf-8-sig")
    return removed, len(new_df)


def merge_channel_ads(new_df: pd.DataFrame, channel: str) -> tuple[int, int]:
    """특정 채널 × new_df에 포함된 날짜만 교체 (날짜 범위 밖 기존 데이터는 보존).

    이렇게 하면 매일 '최근 3일'만 동기화해도 과거 데이터를 날리지 않음.

    Returns:
        (removed_rows, added_rows)
    """
    new_dates = set(new_df["date"].astype(str))

    if ADS_FILE.exists():
        existing = pd.read_csv(ADS_FILE)
        existing["date"] = existing["date"].astype(str)
        mask = (existing["channel"] == channel) & (existing["date"].isin(new_dates))
        removed = int(mask.sum())
        existing = existing[~mask]
    else:
        existing = pd.DataFrame()
        removed = 0

    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = merged.sort_values(["date", "channel"]).reset_index(drop=True)
    merged.to_csv(ADS_FILE, index=False, encoding="utf-8-sig")
    return removed, len(new_df)


def merge_channel_orders(new_df: pd.DataFrame, channel: str) -> tuple[int, int]:
    """특정 채널 × new_df 날짜 범위만 교체. (store 구분 없이)"""
    new_dates = set(new_df["date"].astype(str))

    if ORDERS_FILE.exists():
        existing = pd.read_csv(ORDERS_FILE)
        existing["date"] = existing["date"].astype(str)
        if "store" not in existing.columns:
            existing = _add_store_column(existing)
        mask = (existing["channel"] == channel) & (existing["date"].isin(new_dates))
        removed = int(mask.sum())
        existing = existing[~mask]
    else:
        existing = pd.DataFrame()
        removed = 0

    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = merged.sort_values(["date", "channel"]).reset_index(drop=True)
    merged.to_csv(ORDERS_FILE, index=False, encoding="utf-8-sig")
    return removed, len(new_df)


def merge_store_orders(new_df: pd.DataFrame, store: str) -> tuple[int, int]:
    """특정 스토어 × new_df의 전체 날짜 범위(min~max)를 교체.

    스마트스토어 2개(똑똑연구소, 롤라루) 각각 독립 업로드용.
    날짜 범위 안에서 샘플/구 데이터를 완전히 제거하고 실 데이터로 대체.
    """
    if len(new_df) == 0:
        return 0, 0

    min_date = str(new_df["date"].astype(str).min())
    max_date = str(new_df["date"].astype(str).max())

    if ORDERS_FILE.exists():
        existing = pd.read_csv(ORDERS_FILE)
        existing["date"] = existing["date"].astype(str)
        if "store" not in existing.columns:
            existing = _add_store_column(existing)
        # 범위 기반 제거: 업로드된 기간 내 해당 스토어 행 전부 삭제
        mask = (
            (existing["store"] == store)
            & (existing["date"] >= min_date)
            & (existing["date"] <= max_date)
        )
        removed = int(mask.sum())
        existing = existing[~mask]
    else:
        existing = pd.DataFrame()
        removed = 0

    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = merged.sort_values(["date", "store"]).reset_index(drop=True)
    merged.to_csv(ORDERS_FILE, index=False, encoding="utf-8-sig")
    return removed, len(new_df)
