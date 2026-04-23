"""쿠팡 Supplier Hub 상품별 판매 리포트 CSV/Excel → orders.csv 병합 파서.

쿠팡 로켓배송(벤더 풀필먼트) 주문은 판매자 Wing API 로 조회 불가 →
Supplier Hub (supplier.coupang.com) 에서 매출 리포트 CSV 수동 다운로드.

다운로드 경로 (쿠팡 Supplier Hub):
    https://supplier.coupang.com → 애널리틱스 → 판매 분석
    → 일별 × 상품별 리포트 (CSV/Excel)

    ⚠ wing.coupang.com (Wing) 이 아니라 supplier.coupang.com (Supplier Hub)
    ⚠ 물류 → 발주리스트(PO)는 쿠팡 → 벤더 발주이므로 매출 아님

기대 컬럼 (다양한 형태 자동 매칭):
    날짜 / 일자 / 기간
    상품명 / 상품 / 제품명 / productName
    판매수량 / 수량 / quantity
    매출 / 판매금액 / 실결제금액 / revenue
    (선택) 판매형태 / 배송방식 (로켓배송 / 로켓그로스 / 업체배송)

대시보드 스키마 (orders):
    date, order_id, customer_id, channel='쿠팡',
    store=f'쿠팡_{브랜드}', product, quantity, revenue
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd

from utils.products import classify_product


# ---- 컬럼 매핑 (후보 리스트) ----
COLUMN_CANDIDATES = {
    "date": [
        "날짜", "일자", "기간", "date", "집계일", "주문일",
        "결제일", "판매일",
    ],
    "product": [
        "상품명", "상품", "제품명", "옵션명", "product", "productName",
        "등록상품명", "노출상품명",
    ],
    "quantity": [
        "판매수량", "수량", "판매 수량", "판매량", "quantity",
        "주문수량", "결제수량",
    ],
    "revenue": [
        "매출", "판매금액", "실결제금액", "정산금액", "순매출",
        "매출액", "revenue", "총매출", "총 매출",
        "결제금액", "결제 금액",
    ],
    "order_id": [
        "주문번호", "주문 번호", "order_id", "orderId",
    ],
    # 배송방식 — 판매 형태 구분 (로켓배송/로켓그로스/업체배송)
    "shipment_type": [
        "배송방식", "판매방식", "배송 유형", "shipping_type",
        "fulfillment", "상품 유형",
    ],
}


def _resolve_col(df_cols: list[str], candidates: list[str]) -> str | None:
    """대소문자·공백 무시 매칭."""
    normalized = {c.strip().replace(" ", "").lower(): c for c in df_cols}
    for cand in candidates:
        key = cand.strip().replace(" ", "").lower()
        if key in normalized:
            return normalized[key]
    return None


def _normalize_date(val) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(s.split(".")[0], fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return None


def _clean_int(val) -> int:
    try:
        if pd.isna(val):
            return 0
        s = str(val).replace(",", "").replace("원", "").replace("₩", "").strip()
        return int(float(s))
    except Exception:
        return 0


def read_coupang_sales_file(path: Path) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)

    last_err: Exception | None = None
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"CSV 인코딩 탐지 실패: {last_err}")


def _classify_coupang_product_to_brand(product_name: str) -> str:
    """쿠팡 판매 상품명 → 운영 브랜드.

    일반 제품 분류 로직(classify_product) 재사용 — 제품명 기반.
    반환: '똑똑연구소' / '롤라루' / '공통'
    """
    _, umbrella = classify_product(product_name or "")
    if umbrella in ("똑똑연구소", "롤라루"):
        return umbrella
    return "공통"


def parse_to_orders(df: pd.DataFrame) -> pd.DataFrame:
    """쿠팡 판매 CSV → orders.csv 스키마.

    - 제품명 기반 자동 브랜드 분류
    - 일별 × 제품 집계 (여러 주문이 같은 제품 같은 날이면 합산)

    반환 컬럼: date, order_id, customer_id, channel, store,
              product, quantity, revenue
    """
    cols = list(df.columns.astype(str))
    col_date = _resolve_col(cols, COLUMN_CANDIDATES["date"])
    col_prod = _resolve_col(cols, COLUMN_CANDIDATES["product"])
    col_qty = _resolve_col(cols, COLUMN_CANDIDATES["quantity"])
    col_rev = _resolve_col(cols, COLUMN_CANDIDATES["revenue"])

    missing = [k for k, v in {
        "date": col_date, "product": col_prod, "revenue": col_rev,
    }.items() if v is None]
    if missing:
        raise ValueError(
            f"필수 컬럼 매핑 실패: {missing}\n"
            f"실제 CSV 헤더 (앞 20개): {cols[:20]}"
        )

    work = df.copy()
    work["_date"] = work[col_date].map(_normalize_date)
    work = work.dropna(subset=["_date"])
    if work.empty:
        return pd.DataFrame(columns=[
            "date", "order_id", "customer_id", "channel", "store",
            "product", "quantity", "revenue",
        ])

    work["_product"] = work[col_prod].astype(str).str.strip()
    work["_quantity"] = work[col_qty].map(_clean_int) if col_qty else 1
    work["_revenue"] = work[col_rev].map(_clean_int)
    work["_brand"] = work["_product"].map(_classify_coupang_product_to_brand)

    # store 매핑
    def _store_for_brand(b: str) -> str:
        return {
            "똑똑연구소": "쿠팡_똑똑연구소",
            "롤라루":     "쿠팡_롤라루",
        }.get(b, "쿠팡")

    work["_store"] = work["_brand"].map(_store_for_brand)

    # 일별 × 제품 집계
    grouped = (
        work.groupby(["_date", "_store", "_product"])
        .agg(
            quantity=("_quantity", "sum"),
            revenue=("_revenue", "sum"),
            rows=("_date", "count"),
        )
        .reset_index()
    )

    # 결정론적 order_id/customer_id 생성 (익명 해시)
    def _make_order_id(date_s: str, store: str, product: str) -> str:
        raw = f"CPS-{date_s}-{store}-{product}"
        return "CPS-" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:10].upper()

    def _make_customer_id(date_s: str, product: str) -> str:
        # 익명 customer — 일별 제품 단위로 통합 (재구매 분석 불가)
        raw = f"CPS-ANON-{date_s[:7]}-{product}"
        return "CPS-" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:8].upper()

    out = pd.DataFrame({
        "date": grouped["_date"],
        "order_id": grouped.apply(
            lambda r: _make_order_id(r["_date"], r["_store"], r["_product"]),
            axis=1,
        ),
        "customer_id": grouped.apply(
            lambda r: _make_customer_id(r["_date"], r["_product"]),
            axis=1,
        ),
        "channel": "쿠팡",
        "store": grouped["_store"],
        "product": grouped["_product"],
        "quantity": grouped["quantity"].astype(int),
        "revenue": grouped["revenue"].astype(int),
    })
    return out[[
        "date", "order_id", "customer_id", "channel", "store",
        "product", "quantity", "revenue",
    ]]
