"""이지어드민 재고 CSV/Excel 업로드 파서.

이지어드민(EasyAdmin) 에서 '재고 현황' 또는 '상품/SKU 관리' 메뉴 → 엑셀
다운로드 받은 파일을 파싱해서 통합 재고 정보로 변환.

수동 업로드 패턴 (쿠팡 광고/판매 CSV 와 동일):
    1. 이지어드민에서 재고 데이터 export (CSV/Excel)
    2. data/easyadmin_inventory_upload/ 폴더에 파일 저장
       (또는 대시보드 '📤 CSV 업로드' 페이지에서 업로드)
    3. parse_inventory_file() 호출 → data/inventory.parquet 저장

본업 계정 보호 원칙: 사람-주도 워크플로우 (자동 스크래핑 X)
"""
from __future__ import annotations

import io
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).parent.parent
INVENTORY_FILE = ROOT / "data" / "inventory.parquet"
UPLOAD_DIR = ROOT / "data" / "easyadmin_inventory_upload"


# ==========================================================
# 컬럼 매핑 — 이지어드민 컬럼명이 버전/설정마다 달라서 fuzzy 매칭
# ==========================================================
COLUMN_ALIASES: dict[str, list[str]] = {
    "sku":           ["SKU", "SKU코드", "상품코드", "옵션코드", "바코드",
                      "관리코드", "재고관리코드", "ProductCode"],
    "product":       ["상품명", "제품명", "상품이름", "ProductName", "Name"],
    "option":        ["옵션", "옵션명", "옵션값", "Option"],
    "stock":         ["현재고", "재고", "재고수량", "가용재고", "정상재고",
                      "출고가능재고", "Stock", "Qty", "Quantity"],
    "safety_stock":  ["안전재고", "최소재고", "SafetyStock"],
    "incoming":      ["입고예정", "입고예정수량", "발주수량", "Incoming",
                      "이동중", "이동중수량"],
    "sold_30d":      ["30일판매", "월판매량", "30일판매량", "최근30일",
                      "월매출수량", "Sold30d"],
    "sold_7d":       ["7일판매", "주간판매", "7일판매량", "Sold7d"],
    "category":      ["카테고리", "분류", "Category"],
    "brand":         ["브랜드", "Brand"],
    "price":         ["판매가", "가격", "단가", "Price"],
    "warehouse":     ["창고", "창고명", "Warehouse"],
    "last_in_date":  ["마지막입고일", "최근입고일", "LastInDate"],
    "last_out_date": ["마지막출고일", "최근출고일", "LastOutDate"],
}


def _detect_column(df: pd.DataFrame, target: str) -> str | None:
    """alias 목록과 매칭되는 첫 컬럼 반환."""
    aliases = COLUMN_ALIASES.get(target, [])
    cols_norm = {c: str(c).strip().replace(" ", "") for c in df.columns}
    for alias in aliases:
        alias_n = alias.replace(" ", "")
        for orig, norm in cols_norm.items():
            if norm == alias_n or alias_n in norm:
                return orig
    return None


def _to_int(val) -> int:
    """문자/숫자 → int (콤마, 공백 제거)."""
    if pd.isna(val):
        return 0
    s = str(val).replace(",", "").strip()
    if not s or s in ("-", "—"):
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _brand_from_product(p: str) -> str:
    """제품명 → 브랜드 추정."""
    if not isinstance(p, str):
        return "기타"
    pn = p.replace(" ", "")
    if any(k in pn for k in ["똑똑", "김똑똑", "떡뻥"]):
        return "똑똑연구소"
    if any(k in pn for k in ["롤라루", "캐리어", "여행", "기내용", "백팩"]):
        return "롤라루"
    if any(k in pn for k in ["루티니", "러닝", "운동조끼", "장갑"]):
        return "루티니스트"
    return "기타"


def _read_any(path: Path) -> pd.DataFrame:
    """CSV / Excel / TSV 자동 감지 + 인코딩 fallback."""
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls", ".xlsm"):
        return pd.read_excel(path, dtype=object)
    # csv / tsv
    sep = "\t" if suffix == ".tsv" else ","
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, sep=sep, encoding=enc, dtype=object)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(
        path, sep=sep, encoding="utf-8", encoding_errors="replace", dtype=object,
    )


def _read_uploaded(data: bytes, filename: str) -> pd.DataFrame:
    """업로드 파일 bytes → DataFrame."""
    suffix = Path(filename).suffix.lower()
    if suffix in (".xlsx", ".xls", ".xlsm"):
        return pd.read_excel(io.BytesIO(data), dtype=object)
    sep = "\t" if suffix == ".tsv" else ","
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(
                io.BytesIO(data), sep=sep, encoding=enc, dtype=object,
            )
        except UnicodeDecodeError:
            continue
    return pd.read_csv(
        io.BytesIO(data), sep=sep, encoding="utf-8",
        encoding_errors="replace", dtype=object,
    )


# ==========================================================
# 메인 파서
# ==========================================================
def parse_inventory_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    """이지어드민 CSV/Excel DataFrame → 정규화된 inventory DataFrame.

    반환 컬럼:
        sku, product, option, stock, safety_stock, incoming,
        sold_30d, sold_7d, category, brand, price, warehouse,
        last_in_date, last_out_date, days_left
    """
    if raw.empty:
        return pd.DataFrame()

    raw.columns = [str(c).strip() for c in raw.columns]
    detected: dict[str, str | None] = {
        k: _detect_column(raw, k) for k in COLUMN_ALIASES.keys()
    }

    out = pd.DataFrame()
    out["sku"] = (
        raw[detected["sku"]].astype(str).str.strip()
        if detected["sku"] else ""
    )
    out["product"] = (
        raw[detected["product"]].astype(str).str.strip()
        if detected["product"] else ""
    )
    out["option"] = (
        raw[detected["option"]].astype(str).str.strip()
        if detected["option"] else ""
    )
    out["stock"] = (
        raw[detected["stock"]].apply(_to_int)
        if detected["stock"] else 0
    )
    out["safety_stock"] = (
        raw[detected["safety_stock"]].apply(_to_int)
        if detected["safety_stock"] else 0
    )
    out["incoming"] = (
        raw[detected["incoming"]].apply(_to_int)
        if detected["incoming"] else 0
    )
    out["sold_30d"] = (
        raw[detected["sold_30d"]].apply(_to_int)
        if detected["sold_30d"] else 0
    )
    out["sold_7d"] = (
        raw[detected["sold_7d"]].apply(_to_int)
        if detected["sold_7d"] else 0
    )
    out["category"] = (
        raw[detected["category"]].astype(str).str.strip()
        if detected["category"] else ""
    )
    out["price"] = (
        raw[detected["price"]].apply(_to_int)
        if detected["price"] else 0
    )
    out["warehouse"] = (
        raw[detected["warehouse"]].astype(str).str.strip()
        if detected["warehouse"] else ""
    )
    out["last_in_date"] = (
        raw[detected["last_in_date"]].astype(str).str.strip()
        if detected["last_in_date"] else ""
    )
    out["last_out_date"] = (
        raw[detected["last_out_date"]].astype(str).str.strip()
        if detected["last_out_date"] else ""
    )

    # 브랜드 — column 이 있으면 그대로, 없으면 제품명에서 추론
    if detected["brand"]:
        out["brand"] = raw[detected["brand"]].astype(str).str.strip()
    else:
        out["brand"] = out["product"].apply(_brand_from_product)

    # 소진 임박일 (days_left) — sold_30d 기반
    out["days_left"] = out.apply(
        lambda r: (
            int(r["stock"] / (r["sold_30d"] / 30))
            if r["sold_30d"] > 0 and r["stock"] >= 0
            else 9999   # 판매 0 → 매우 큰 값 (압박 카테고리)
        ),
        axis=1,
    )

    # 빈 sku 또는 빈 product 제외
    out = out[(out["sku"] != "") | (out["product"] != "")].copy()
    return out.reset_index(drop=True)


def save_inventory(df: pd.DataFrame) -> None:
    """parquet 저장."""
    if df.empty:
        return
    INVENTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(INVENTORY_FILE, index=False)


def load_inventory() -> pd.DataFrame:
    """저장된 재고 데이터 로드."""
    if not INVENTORY_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(INVENTORY_FILE)
    except Exception:
        return pd.DataFrame()


def save_uploaded_file(uploaded, target_dir: Path | None = None) -> Path:
    """업로드 파일을 디스크에 저장 (백업/감사용)."""
    target_dir = target_dir or UPLOAD_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    name = uploaded.name
    target = target_dir / name
    if target.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = target_dir / f"{Path(name).stem}_{ts}{Path(name).suffix}"
    target.write_bytes(uploaded.getvalue())
    return target


def process_uploaded(uploaded) -> tuple[pd.DataFrame, dict]:
    """업로드 파일 한 번에 처리 — 저장 + 파싱 + 저장.

    반환: (파싱된 DataFrame, info dict)
    """
    saved_path = save_uploaded_file(uploaded)
    raw = _read_uploaded(uploaded.getvalue(), uploaded.name)
    parsed = parse_inventory_dataframe(raw)
    save_inventory(parsed)

    info = {
        "saved_path": str(saved_path),
        "raw_rows": len(raw),
        "parsed_rows": len(parsed),
        "raw_columns": list(raw.columns),
        "matched_columns": {
            k: _detect_column(raw, k) for k in COLUMN_ALIASES.keys()
        },
    }
    return parsed, info


def get_inventory_alerts() -> dict:
    """저장된 재고에서 4가지 알림 자동 추출.

    반환:
        {
            'low_stock': DataFrame,    # 소진 임박 (days_left <= 14)
            'top_stock': DataFrame,    # 현재 재고 TOP 10
            'incoming':  DataFrame,    # 입고 예정 (incoming > 0)
            'pressure':  DataFrame,    # 재고 압박 (sold_30d 적은데 stock 많음)
        }
    """
    df = load_inventory()
    empty = pd.DataFrame()
    if df.empty:
        return {
            "low_stock": empty, "top_stock": empty,
            "incoming": empty, "pressure": empty,
        }

    # 1) 소진 임박 — days_left 14일 이하 (안전재고 위 → 0 이상)
    low = df[(df["stock"] > 0) & (df["days_left"] <= 14)].copy()
    low = low.sort_values("days_left").head(10)

    # 2) 현재 재고 TOP 10
    top = df.sort_values("stock", ascending=False).head(10)

    # 3) 입고 예정
    inc = df[df["incoming"] > 0].sort_values("incoming", ascending=False).head(10)

    # 4) 재고 압박 — 30일 판매 < 평균 30% 이고 재고 > 평균 1.5배
    if len(df) > 5 and df["stock"].mean() > 0:
        avg_sold = df["sold_30d"].mean()
        avg_stock = df["stock"].mean()
        pressure = df[
            (df["sold_30d"] < avg_sold * 0.3)
            & (df["stock"] > avg_stock * 1.5)
        ].copy()
        # 회전율 = sold_30d / stock (낮을수록 압박)
        pressure["turnover"] = pressure["sold_30d"] / pressure["stock"].clip(lower=1)
        pressure = pressure.sort_values("turnover").head(10)
    else:
        pressure = empty

    return {
        "low_stock": low,
        "top_stock": top,
        "incoming": inc,
        "pressure": pressure,
    }
