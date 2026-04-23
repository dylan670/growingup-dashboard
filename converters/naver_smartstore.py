"""네이버 스마트스토어 주문 내역 CSV → orders.csv 스키마로 변환.

사용 경로:
    스마트스토어 판매자센터 (sell.smartstore.naver.com)
    → 주문/배송 → 주문 내역 조회
    → 조회 기간 설정 → '엑셀 다운로드' 또는 '대량 엑셀 다운로드'
    → .xlsx → CSV로 저장 (또는 직접 CSV 업로드도 가능)

개인정보 처리:
    구매자ID / 이름 / 연락처는 모두 해시(MD5 8자리)로 익명화되어 저장됩니다.
    로컬 디스크에만 보관되며 외부 서버로 전송되지 않습니다.
"""
from __future__ import annotations

import hashlib
import io
from typing import Union

import pandas as pd


COLUMN_ALIASES: dict[str, list[str]] = {
    "date": [
        "결제일", "결제일시", "결제일자",
        "주문일", "주문일시", "주문일자",
        "발주확인일", "Order Date",
    ],
    "order_id": [
        "상품주문번호", "주문번호", "주문 번호",
        "Order ID", "order_id",
    ],
    "customer_id": [
        "구매자ID", "구매자 ID", "구매자 아이디", "구매자아이디",
        "회원번호", "회원 번호", "고객 ID",
    ],
    "customer_name": [
        "구매자명", "구매자", "주문자명", "주문자",
    ],
    "customer_phone": [
        "구매자연락처", "구매자 연락처", "연락처", "전화번호",
    ],
    "product": [
        "상품명", "Product", "Item", "상품 명",
    ],
    "quantity": [
        "수량", "주문수량", "상품수량", "Quantity",
    ],
    "revenue": [
        "상품별 총 주문금액", "상품별총주문금액",
        "총 주문금액", "총주문금액",
        "결제금액", "최종결제금액", "실결제금액",
        "판매금액", "상품가격", "주문금액",
    ],
}


def _read_with_encoding(source: Union[str, bytes, io.IOBase]) -> pd.DataFrame:
    if isinstance(source, bytes):
        raw = source
    elif hasattr(source, "read"):
        raw = source.read()
        if hasattr(source, "seek"):
            source.seek(0)
    else:
        with open(source, "rb") as f:
            raw = f.read()

    last_err: Exception | None = None
    for enc in ["utf-8-sig", "utf-8", "cp949", "euc-kr"]:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc)
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            last_err = e
            continue
    raise ValueError(
        f"CSV 인코딩을 해석할 수 없습니다. 원인: {last_err}"
    )


def _find_column(columns: list[str], aliases: list[str]) -> str | None:
    normalized = {c.strip().lower(): c for c in columns}
    for alias in aliases:
        key = alias.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def _hash_id(raw: object) -> str:
    """개인정보 보호를 위한 해시 ID 생성 (접두어 'NS-' + MD5 8자리)."""
    return "NS-" + hashlib.md5(str(raw).encode("utf-8")).hexdigest()[:8].upper()


def _clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("원", "", regex=False)
        .str.strip(),
        errors="coerce",
    ).fillna(0)


def convert(source: Union[str, bytes, io.IOBase],
            store: str = "똑똑연구소") -> pd.DataFrame:
    """스마트스토어 주문 CSV를 대시보드 orders.csv 포맷으로 변환.

    Args:
        source: CSV 경로 또는 bytes 또는 file-like object
        store: 스토어 이름 ("똑똑연구소" 또는 "롤라루") — store 컬럼 값으로 사용

    반환 DataFrame 컬럼:
        date, order_id, customer_id, channel, store, product, quantity, revenue
    """
    df = _read_with_encoding(source)

    date_col = _find_column(list(df.columns), COLUMN_ALIASES["date"])
    order_col = _find_column(list(df.columns), COLUMN_ALIASES["order_id"])
    revenue_col = _find_column(list(df.columns), COLUMN_ALIASES["revenue"])

    # 필수: date_col, order_col만 / revenue_col은 있으면 사용, 없으면 0
    missing = []
    if not date_col:
        missing.append("결제일/주문일시")
    if not order_col:
        missing.append("상품주문번호/주문번호")

    if missing:
        raise ValueError(
            f"스마트스토어 주문 CSV에서 필수 컬럼을 찾을 수 없습니다: {missing}\n"
            f"CSV 실제 컬럼 (일부): {list(df.columns)[:15]}\n"
            f"판매자센터 → 주문/배송 → 주문 내역 → '엑셀 다운로드'로 받은 파일인지 확인하세요."
        )

    revenue_missing = revenue_col is None

    # customer_id 결정 (우선순위: 구매자ID → 이름+연락처 → 주문번호)
    cust_col = _find_column(list(df.columns), COLUMN_ALIASES["customer_id"])
    name_col = _find_column(list(df.columns), COLUMN_ALIASES["customer_name"])
    phone_col = _find_column(list(df.columns), COLUMN_ALIASES["customer_phone"])

    if cust_col:
        customer_ids = df[cust_col].astype(str).apply(_hash_id)
        cust_source = f"'{cust_col}' 컬럼"
    elif name_col and phone_col:
        customer_ids = (df[name_col].astype(str) + "_" + df[phone_col].astype(str)).apply(_hash_id)
        cust_source = f"'{name_col}' + '{phone_col}' 조합"
    elif name_col:
        customer_ids = df[name_col].astype(str).apply(_hash_id)
        cust_source = f"'{name_col}' 컬럼 (중복 가능성 있음 — 재구매 분석 정확도 저하)"
    else:
        customer_ids = df[order_col].astype(str).apply(_hash_id)
        cust_source = "주문번호 대체 (재구매 분석 불가 — '구매자ID' 컬럼 포함해 재다운로드 권장)"

    product_col = _find_column(list(df.columns), COLUMN_ALIASES["product"])
    quantity_col = _find_column(list(df.columns), COLUMN_ALIASES["quantity"])

    result = pd.DataFrame({
        "date": pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d"),
        "order_id": df[order_col].astype(str),
        "customer_id": customer_ids,
        "channel": "네이버",
        "store": store,
        "product": df[product_col].astype(str) if product_col else "미지정",
        "quantity": (
            _clean_numeric(df[quantity_col]).astype(int)
            if quantity_col else 1
        ),
        "revenue": (
            _clean_numeric(df[revenue_col]).astype(int)
            if revenue_col else 0
        ),
    })

    # 날짜 파싱 실패 행 제거
    result = result.dropna(subset=["date"])

    # 메타정보를 attrs에 담아 UI에서 사용자에게 보여줄 수 있게
    result.attrs["customer_id_source"] = cust_source
    result.attrs["original_rows"] = len(df)
    result.attrs["valid_rows"] = len(result)
    result.attrs["revenue_missing"] = revenue_missing
    if revenue_missing:
        result.attrs["revenue_note"] = (
            "원본 CSV에 결제금액 컬럼이 없어 revenue=0으로 저장. "
            "정산관리 → 정산 내역 또는 주문 상세 다운로드에서 금액 포함 파일 별도 필요."
        )

    return result
