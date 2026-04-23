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

from utils.products import classify_product, is_blocked_product


# ---- 컬럼 매핑 (후보 리스트) ----
COLUMN_CANDIDATES = {
    "date": [
        # 일반 판매 리포트
        "날짜", "일자", "기간", "date", "집계일", "주문일",
        "결제일", "판매일",
        # 쿠팡 Supplier Hub 발주리스트
        "일시", "발주일", "발주일시", "입고일", "입고 예정일",
    ],
    "product": [
        # 일반 판매
        "상품명", "상품", "제품명", "옵션명", "product", "productName",
        "등록상품명", "노출상품명",
        # 발주리스트 (첫 SKU 명 형태)
        "첫 SKU명", "첫SKU명", "SKU명", "SKU 명",
    ],
    "quantity": [
        # 일반
        "판매수량", "수량", "판매 수량", "판매량", "quantity",
        "주문수량", "결제수량",
        # 발주
        "발주수량", "발주 수량", "발주", "입고수량", "입고 수량",
    ],
    "revenue": [
        # 일반 판매
        "매출", "판매금액", "실결제금액", "정산금액", "순매출",
        "매출액", "revenue", "총매출", "총 매출",
        "결제금액", "결제 금액",
        # 발주
        "금액", "발주금액", "발주 금액", "실공급가", "공급가",
        "공급가액", "총 발주액",
    ],
    "order_id": [
        "주문번호", "주문 번호", "order_id", "orderId",
        "발주번호", "발주 번호",
    ],
    "shipment_type": [
        "배송방식", "판매방식", "배송 유형", "shipping_type",
        "fulfillment", "상품 유형", "운송",
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


# ==========================================================
# 쿠팡 Supplier Hub 개별 발주서(PO) 전용 파서
# 파일 형식: 다중 섹션 (거래처/발주/상품/메시지/회송)
# ==========================================================
def _is_po_format(path: Path) -> bool:
    """파일이 개별 발주서 양식인지 판별 — 첫 행에 '발주서 No.' 패턴."""
    try:
        df = pd.read_excel(path, header=None, nrows=5)
        if df.empty:
            return False
        first_val = str(df.iloc[0, 0]) if pd.notna(df.iloc[0, 0]) else ""
        return "발주서" in first_val and "No" in first_val
    except Exception:
        return False


def parse_po_file(path: Path) -> pd.DataFrame:
    """개별 발주서 Excel → (date, product, quantity, revenue) DF.

    발주서 구조:
      row 0: 발주서 No.XXXX
      row 2: 1. 거래처정보
      row 7: 2. 발주정보
        row 11-12: 입고예정일시 / 물류센터 / ...
      row 17: 3. 상품정보
        row 19: 헤더 (No, 상품코드, 상품명, ..., 발주수량, ..., 발주금액, ...)
        row 21+: 상품 행
        row 23: 합계 (무시)
    """
    import re

    raw = pd.read_excel(path, header=None)
    if raw.empty:
        return pd.DataFrame()

    # ---- 발주번호 ----
    po_no = ""
    first_val = str(raw.iloc[0, 0]) if pd.notna(raw.iloc[0, 0]) else ""
    m = re.search(r"No\.?\s*(\d+)", first_val)
    if m:
        po_no = m.group(1)

    # ---- 입고예정일시 / 발주일 찾기 ----
    po_date = None
    # 모든 셀 순회하면서 YYYY/MM/DD 또는 YYYY-MM-DD 패턴 찾기
    # (입고예정일시 컬럼 먼저 찾고 바로 다음/같은 행의 날짜 값 사용)
    for i in range(len(raw)):
        row = raw.iloc[i]
        header_row_has_inbound = any(
            "입고예정일시" in str(v) for v in row if pd.notna(v)
        )
        if header_row_has_inbound:
            # 다음 행에 날짜 값
            for search_row in [i + 1, i + 2]:
                if search_row >= len(raw):
                    break
                for val in raw.iloc[search_row]:
                    if pd.notna(val):
                        s = str(val)
                        m = re.search(
                            r"(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})", s,
                        )
                        if m:
                            po_date = (
                                f"{m.group(1)}-"
                                f"{int(m.group(2)):02d}-"
                                f"{int(m.group(3)):02d}"
                            )
                            break
                if po_date:
                    break
            if po_date:
                break

    if po_date is None:
        # fallback — 파일 내 아무 날짜 패턴 찾기
        for i in range(len(raw)):
            for val in raw.iloc[i]:
                if pd.notna(val):
                    s = str(val)
                    m = re.search(
                        r"(\d{4})[./\-](\d{1,2})[./\-](\d{1,2})", s,
                    )
                    if m:
                        po_date = (
                            f"{m.group(1)}-"
                            f"{int(m.group(2)):02d}-"
                            f"{int(m.group(3)):02d}"
                        )
                        break
            if po_date:
                break

    # ---- '3. 상품정보' 섹션 위치 찾기 ----
    product_section_start = None
    for i in range(len(raw)):
        row_str = " ".join(str(x) for x in raw.iloc[i] if pd.notna(x))
        if "3. 상품정보" in row_str or "상품정보" == row_str.strip():
            product_section_start = i
            break
    if product_section_start is None:
        return pd.DataFrame()

    # ---- 헤더 행 찾기 (상품 테이블 헤더) ----
    header_idx = None
    for i in range(product_section_start + 1,
                   min(product_section_start + 8, len(raw))):
        row_vals = [str(x) for x in raw.iloc[i] if pd.notna(x)]
        joined = " ".join(row_vals)
        if "상품" in joined and ("발주수량" in joined or "수량" in joined):
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame()

    header = raw.iloc[header_idx].tolist()

    # 컬럼 위치 찾기
    col_product = None
    col_qty = None
    col_amount = None
    for idx, h in enumerate(header):
        hs = str(h) if pd.notna(h) else ""
        if col_product is None and "상품명" in hs:
            col_product = idx
        if col_qty is None and ("발주수량" == hs or "발주 수량" == hs):
            col_qty = idx
        if col_amount is None and ("발주금액" == hs or "발주 금액" == hs):
            col_amount = idx
    # fallback — 부분 매칭
    if col_qty is None:
        for idx, h in enumerate(header):
            hs = str(h) if pd.notna(h) else ""
            if "발주" in hs and "수량" in hs:
                col_qty = idx
                break
    if col_amount is None:
        for idx, h in enumerate(header):
            hs = str(h) if pd.notna(h) else ""
            if "발주" in hs and "금액" in hs:
                col_amount = idx
                break

    if col_product is None or col_amount is None:
        return pd.DataFrame()

    # ---- 데이터 행 추출 (헤더 이후, 합계/다음 섹션 전) ----
    rows = []
    for i in range(header_idx + 1, len(raw)):
        row = raw.iloc[i]
        first = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        first = first.strip()

        # 다음 섹션 시작 또는 합계 — 종료
        if first.startswith("4.") or first.startswith("5.") or first == "합계":
            break

        # 상품 행: 첫 컬럼이 숫자 No.
        if not first.isdigit():
            continue

        product = (
            str(row.iloc[col_product]).strip()
            if pd.notna(row.iloc[col_product]) else ""
        )
        if not product:
            continue

        qty = _clean_int(row.iloc[col_qty]) if col_qty is not None else 0
        amount = _clean_int(row.iloc[col_amount])

        rows.append({
            "date": po_date,
            "product": product,
            "quantity": qty,
            "revenue": amount,
            "po_number": po_no,
        })

    return pd.DataFrame(rows)


def parse_po_files_to_orders(file_paths: list[Path]) -> pd.DataFrame:
    """여러 발주서 파일을 순회하며 orders 스키마로 변환.

    - 오즈키즈 등 차단 키워드 필터
    - 제품명 기반 브랜드 자동 분류 → 쿠팡_*_벤더 store 매핑
    """
    all_po_rows: list[pd.DataFrame] = []
    failed: list[tuple[str, str]] = []

    for path in file_paths:
        try:
            po_df = parse_po_file(path)
            if not po_df.empty:
                all_po_rows.append(po_df)
        except Exception as e:
            failed.append((path.name, f"{type(e).__name__}: {e}"))

    if not all_po_rows:
        df = pd.DataFrame()
        df.attrs["failed"] = failed
        return df

    combined = pd.concat(all_po_rows, ignore_index=True)

    # 차단 제품 (오즈키즈 등) 필터
    before = len(combined)
    combined = combined[~combined["product"].map(is_blocked_product)].copy()
    blocked = before - len(combined)

    if combined.empty:
        df = pd.DataFrame()
        df.attrs["blocked"] = blocked
        df.attrs["failed"] = failed
        return df

    # 브랜드 분류 + store 매핑
    from utils.products import classify_product
    def _brand(name: str) -> str:
        _, umb = classify_product(name)
        return umb if umb in ("똑똑연구소", "롤라루") else "공통"
    combined["_brand"] = combined["product"].map(_brand)
    combined["_store"] = combined["_brand"].map({
        "똑똑연구소": "쿠팡_똑똑연구소_벤더",
        "롤라루":     "쿠팡_롤라루_벤더",
    }).fillna("쿠팡_벤더_기타")

    # (date × store × product) 단위 집계 — 같은 제품이 여러 PO 에 있으면 합산
    agg = (
        combined.groupby(["date", "_store", "product"], as_index=False)
        .agg(
            quantity=("quantity", "sum"),
            revenue=("revenue", "sum"),
            po_count=("po_number", "nunique"),
        )
    )

    # orders 스키마 변환 (고유 ID 생성)
    def _make_order_id(date_s: str, store: str, product: str) -> str:
        raw = f"CPVI-{date_s}-{store}-{product}"
        return "CPVI-" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:10].upper()

    def _make_customer_id(date_s: str, product: str) -> str:
        raw = f"CPVI-ANON-{date_s[:7]}-{product}"
        return "CPVI-" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:8].upper()

    out = pd.DataFrame({
        "date": agg["date"],
        "order_id": agg.apply(
            lambda r: _make_order_id(r["date"], r["_store"], r["product"]),
            axis=1,
        ),
        "customer_id": agg.apply(
            lambda r: _make_customer_id(r["date"], r["product"]),
            axis=1,
        ),
        "channel": "쿠팡",
        "store": agg["_store"],
        "product": agg["product"],
        "quantity": agg["quantity"].astype(int),
        "revenue": agg["revenue"].astype(int),
    })
    out = out[[
        "date", "order_id", "customer_id", "channel", "store",
        "product", "quantity", "revenue",
    ]].sort_values(["date", "store", "product"]).reset_index(drop=True)

    out.attrs["blocked"] = blocked
    out.attrs["failed"] = failed
    out.attrs["po_count"] = combined["po_number"].nunique()
    return out


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

    # 차단 키워드 필터 (오즈키즈 등 타 브랜드 제품 제외)
    before_n = len(work)
    work = work[~work["_product"].map(is_blocked_product)]
    blocked_n = before_n - len(work)
    if blocked_n:
        # 진단 정보 DataFrame attribute 로 첨부 (CLI에서 로그에 표시)
        work.attrs["blocked_rows"] = blocked_n
    if work.empty:
        return pd.DataFrame(columns=[
            "date", "order_id", "customer_id", "channel", "store",
            "product", "quantity", "revenue",
        ])
    work["_quantity"] = work[col_qty].map(_clean_int) if col_qty else 1
    work["_revenue"] = work[col_rev].map(_clean_int)
    work["_brand"] = work["_product"].map(_classify_coupang_product_to_brand)

    # store 매핑 — 벤더 발주 데이터는 "_벤더" suffix 로 소비자 판매와 분리
    # (orders.csv 의 쿠팡_롤라루/쿠팡_똑똑연구소 는 실제 판매, 이건 B2B 입고)
    def _store_for_brand(b: str) -> str:
        return {
            "똑똑연구소": "쿠팡_똑똑연구소_벤더",
            "롤라루":     "쿠팡_롤라루_벤더",
        }.get(b, "쿠팡_벤더_기타")

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
