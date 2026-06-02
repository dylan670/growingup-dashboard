"""제품 원가 마스터 CSV 파서.

원가율 / 마진율 계산을 위한 SKU 단위 원가 입력 시스템.

CSV 형식 (필수 컬럼 자동 매칭):
    product (또는 상품명, sku, SKU코드)
    unit_cost (또는 원가, cost, 단가원가)

저장:
    data/product_costs.parquet
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).parent.parent
COSTS_FILE = ROOT / "data" / "product_costs.parquet"
UPLOAD_DIR = ROOT / "data" / "product_costs_upload"


COLUMN_ALIASES = {
    "product":   ["product", "상품명", "제품명", "상품이름", "Name"],
    "sku":       ["sku", "SKU", "SKU코드", "상품코드", "옵션코드"],
    "unit_cost": ["unit_cost", "원가", "cost", "단가원가", "공급가",
                  "원가단가", "매입가", "UnitCost"],
}


def _detect(df: pd.DataFrame, target: str) -> str | None:
    aliases = COLUMN_ALIASES.get(target, [])
    cols_norm = {c: str(c).strip() for c in df.columns}
    for alias in aliases:
        for orig, norm in cols_norm.items():
            if norm == alias or alias.replace(" ", "") in norm.replace(" ", ""):
                return orig
    return None


def _to_int(v) -> int:
    if pd.isna(v):
        return 0
    s = str(v).replace(",", "").strip()
    if not s or s in ("-", "—"):
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _read_uploaded(data: bytes, filename: str) -> pd.DataFrame:
    suffix = Path(filename).suffix.lower()
    if suffix in (".xlsx", ".xls", ".xlsm"):
        # 이지어드민 HTML xls 호환
        head = data[:300].lower()
        if b"<html" in head or b"<table" in head:
            tables = pd.read_html(io.BytesIO(data), encoding="utf-8")
            if tables:
                t = tables[0]
                if list(t.columns) == list(range(len(t.columns))) and len(t) > 1:
                    header = t.iloc[0].astype(str).tolist()
                    t = t.iloc[1:].copy()
                    t.columns = header
                return t
        return pd.read_excel(io.BytesIO(data), dtype=object)
    sep = "\t" if suffix == ".tsv" else ","
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(io.BytesIO(data), sep=sep, encoding=enc, dtype=object)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(
        io.BytesIO(data), sep=sep, encoding="utf-8",
        encoding_errors="replace", dtype=object,
    )


def parse_costs(raw: pd.DataFrame) -> pd.DataFrame:
    """원본 → 정규화 (product, sku, unit_cost)."""
    if raw.empty:
        return pd.DataFrame(columns=["product", "sku", "unit_cost"])

    raw.columns = [str(c).strip() for c in raw.columns]
    prod_col = _detect(raw, "product")
    sku_col = _detect(raw, "sku")
    cost_col = _detect(raw, "unit_cost")

    out = pd.DataFrame()
    out["product"] = (
        raw[prod_col].astype(str).str.strip()
        if prod_col else ""
    )
    out["sku"] = (
        raw[sku_col].astype(str).str.strip()
        if sku_col else ""
    )
    out["unit_cost"] = (
        raw[cost_col].apply(_to_int)
        if cost_col else 0
    )

    out = out[(out["product"] != "") | (out["sku"] != "")]
    out = out[out["unit_cost"] > 0]
    return out.reset_index(drop=True)


def save_costs(df: pd.DataFrame) -> None:
    if df.empty:
        return
    COSTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(COSTS_FILE, index=False)


def load_costs() -> pd.DataFrame:
    if not COSTS_FILE.exists():
        return pd.DataFrame(columns=["product", "sku", "unit_cost"])
    try:
        return pd.read_parquet(COSTS_FILE)
    except Exception:
        return pd.DataFrame(columns=["product", "sku", "unit_cost"])


def save_uploaded_file(uploaded, target_dir: Path | None = None) -> Path:
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
    save_uploaded_file(uploaded)
    raw = _read_uploaded(uploaded.getvalue(), uploaded.name)
    parsed = parse_costs(raw)
    save_costs(parsed)
    return parsed, {
        "raw_rows": len(raw),
        "parsed_rows": len(parsed),
        "raw_columns": list(raw.columns),
        "matched_columns": {
            k: _detect(raw, k) for k in COLUMN_ALIASES.keys()
        },
    }


def get_cost_for_product(product_name: str, costs_df: pd.DataFrame) -> int:
    """제품명 → 단가 원가. 완전 일치 → 부분 일치 → 0."""
    if costs_df.empty or not isinstance(product_name, str):
        return 0
    # 완전 일치
    exact = costs_df[costs_df["product"] == product_name]
    if not exact.empty:
        return int(exact.iloc[0]["unit_cost"])
    # 부분 일치 — costs_df 의 product 가 product_name 에 포함
    for _, r in costs_df.iterrows():
        p = str(r["product"])
        if p and p in product_name:
            return int(r["unit_cost"])
    return 0


def compute_cost_ratio(orders_df: pd.DataFrame, costs_df: pd.DataFrame) -> dict:
    """orders + costs → 원가/매출/마진 통합 계산.

    반환:
        {
            'total_revenue': N, 'total_cost': N, 'cost_ratio': %,
            'matched_orders': N, 'unmatched_orders': N,
        }
    """
    if orders_df.empty:
        return {"total_revenue": 0, "total_cost": 0, "cost_ratio": 0,
                "matched_orders": 0, "unmatched_orders": 0}

    if costs_df.empty:
        return {
            "total_revenue": int(orders_df["revenue"].sum()),
            "total_cost": 0,
            "cost_ratio": 0,
            "matched_orders": 0,
            "unmatched_orders": len(orders_df),
        }

    # product → unit_cost lookup (dict)
    cost_lookup = dict(zip(costs_df["product"], costs_df["unit_cost"]))

    matched = 0
    unmatched = 0
    total_cost = 0
    for _, row in orders_df.iterrows():
        product = str(row.get("product", ""))
        qty = float(row.get("quantity", 0) or 0)
        unit_cost = cost_lookup.get(product, 0)
        if unit_cost == 0:
            # fuzzy match
            for p, c in cost_lookup.items():
                if p and p in product:
                    unit_cost = c
                    break
        if unit_cost > 0:
            total_cost += unit_cost * qty
            matched += 1
        else:
            unmatched += 1

    total_rev = int(orders_df["revenue"].sum())
    cost_ratio = total_cost / total_rev * 100 if total_rev > 0 else 0
    return {
        "total_revenue": total_rev,
        "total_cost": int(total_cost),
        "cost_ratio": round(cost_ratio, 1),
        "matched_orders": matched,
        "unmatched_orders": unmatched,
    }
