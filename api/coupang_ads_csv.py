"""쿠팡 광고센터 리포트 CSV/Excel → 대시보드 ads.csv 병합 파서.

쿠팡은 광고주용 Open API 미제공 → 광고센터(advertising.coupang.com)에서
수동 다운로드한 CSV/Excel 를 파싱.

다운로드 경로 (쿠팡 광고센터):
    리포트 → 광고 리포트 → 기간 선택 → 다운로드 (CSV 또는 Excel)

기대 컬럼 (다양한 형태 자동 매칭):
    날짜 / 기간 / 일자
    캠페인명 / 캠페인 / 광고상품명
    노출수 / 노출 / impressions
    클릭수 / 클릭 / clicks
    광고비 / 비용 / spend
    주문수 / 전환수 / 판매수 / conversions
    매출 / 주문금액 / 전환매출 / revenue

대시보드 스키마:
    date, channel='쿠팡', store=f'쿠팡_{브랜드}', spend, impressions,
    clicks, conversions, revenue
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from utils.products import classify_coupang_ad_to_brand


# ---- 컬럼 매핑 (후보 리스트) ----
COLUMN_CANDIDATES = {
    "date": [
        "날짜", "기간", "일자", "date", "집계일", "게재일", "노출일",
    ],
    "campaign": [
        "캠페인명", "캠페인", "광고상품명", "광고상품", "campaign",
        "campaign_name", "상품명",
    ],
    "impressions": [
        "노출수", "노출", "노출 수", "impressions", "impression", "IMP",
    ],
    "clicks": [
        "클릭수", "클릭", "클릭 수", "clicks", "click", "CLK",
    ],
    "spend": [
        "광고비", "비용", "광고 비용", "광고비(원)", "spend", "cost",
        "금액", "집행비용",
    ],
    "conversions": [
        "주문수", "주문 수", "전환수", "판매수", "전환", "conversions",
        "주문 건수", "판매 건수",
    ],
    "revenue": [
        "매출", "매출액", "주문금액", "전환매출", "판매금액", "revenue",
        "매출 금액", "주문 금액",
    ],
}


def _resolve_col(df_cols: list[str], candidates: list[str]) -> str | None:
    """컬럼명 후보 중 실제 존재하는 것 반환. 대소문자·공백 무시."""
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


def read_coupang_ads_file(path: Path) -> pd.DataFrame:
    """CSV/Excel 자동 판별 로드. 인코딩 탐지."""
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)

    # CSV: 여러 인코딩 시도
    last_err: Exception | None = None
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"CSV 인코딩 탐지 실패: {last_err}")


def parse_to_ads(df: pd.DataFrame) -> pd.DataFrame:
    """쿠팡 광고 리포트 DataFrame → ads.csv 스키마.

    캠페인명 기반 브랜드 자동 분류:
        김똑똑/떡뻥/로켓그로스 → 쿠팡_똑똑연구소
        AI 광고/롤라루 → 쿠팡_롤라루
        매칭 실패 → 쿠팡 (미분류)

    반환 컬럼: date, channel, store, spend, impressions, clicks,
              conversions, revenue
    """
    cols = list(df.columns.astype(str))

    col_date = _resolve_col(cols, COLUMN_CANDIDATES["date"])
    col_camp = _resolve_col(cols, COLUMN_CANDIDATES["campaign"])
    col_imp = _resolve_col(cols, COLUMN_CANDIDATES["impressions"])
    col_clk = _resolve_col(cols, COLUMN_CANDIDATES["clicks"])
    col_spend = _resolve_col(cols, COLUMN_CANDIDATES["spend"])
    col_conv = _resolve_col(cols, COLUMN_CANDIDATES["conversions"])
    col_rev = _resolve_col(cols, COLUMN_CANDIDATES["revenue"])

    # 최소 필수: date, spend (+ 선택적으로 campaign)
    missing = [
        k for k, v in {
            "date": col_date, "spend": col_spend,
        }.items() if v is None
    ]
    if missing:
        raise ValueError(
            f"필수 컬럼 매핑 실패: {missing}\n"
            f"실제 CSV 헤더: {cols[:15]}"
        )

    work = df.copy()
    work["_date"] = work[col_date].map(_normalize_date)
    work = work.dropna(subset=["_date"])
    if work.empty:
        return pd.DataFrame(columns=[
            "date", "channel", "store", "spend", "impressions",
            "clicks", "conversions", "revenue",
        ])

    # 브랜드 분류 — campaign 컬럼 있으면 이름 기반, 없으면 "공통"
    if col_camp:
        work["_brand"] = work[col_camp].astype(str).map(classify_coupang_ad_to_brand)
    else:
        work["_brand"] = "공통"

    # store 값
    def _store(brand: str) -> str:
        if brand == "똑똑연구소":
            return "쿠팡_똑똑연구소"
        if brand == "롤라루":
            return "쿠팡_롤라루"
        return "쿠팡"

    work["_store"] = work["_brand"].map(_store)

    # 일자 × store 단위로 aggregation (캠페인 여러개가 같은 날 같은 브랜드에
    # 있으면 합산)
    grouped = (
        work.groupby(["_date", "_store"])
        .agg(
            spend=(col_spend, lambda s: s.map(_clean_int).sum()),
            impressions=(
                col_imp, lambda s: s.map(_clean_int).sum()
            ) if col_imp else ("_date", "count"),
            clicks=(
                col_clk, lambda s: s.map(_clean_int).sum()
            ) if col_clk else ("_date", "count"),
            conversions=(
                col_conv, lambda s: s.map(_clean_int).sum()
            ) if col_conv else ("_date", "count"),
            revenue=(
                col_rev, lambda s: s.map(_clean_int).sum()
            ) if col_rev else ("_date", "count"),
        )
        .reset_index()
    )

    # imp/clk/conv/rev 미제공 컬럼은 0 처리
    for c in ["impressions", "clicks", "conversions", "revenue"]:
        if c not in grouped.columns or grouped[c].dtype == "int64" and not col_imp:
            # 위 groupby 에서 count 으로 fallback 됐을 수 있음 — 0 으로 덮어쓰기
            pass

    # fallback count → 0 으로 (col_imp 등이 None 이었을 때만)
    if not col_imp:
        grouped["impressions"] = 0
    if not col_clk:
        grouped["clicks"] = 0
    if not col_conv:
        grouped["conversions"] = 0
    if not col_rev:
        grouped["revenue"] = 0

    out = pd.DataFrame({
        "date": grouped["_date"],
        "channel": "쿠팡",
        "store": grouped["_store"],
        "spend": grouped["spend"].astype(int),
        "impressions": grouped["impressions"].astype(int),
        "clicks": grouped["clicks"].astype(int),
        "conversions": grouped["conversions"].astype(int),
        "revenue": grouped["revenue"].astype(int),
    })
    return out[[
        "date", "channel", "store", "spend", "impressions",
        "clicks", "conversions", "revenue",
    ]]


def parse_to_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    """캠페인 단위 집계 (기간 전체 합계) — drill-down 페이지 용.

    반환 컬럼: campaign_name, brand, spend, impressions, clicks,
              ctr_pct, cpc, conversions, revenue, roas_pct
    """
    cols = list(df.columns.astype(str))
    col_camp = _resolve_col(cols, COLUMN_CANDIDATES["campaign"])
    col_imp = _resolve_col(cols, COLUMN_CANDIDATES["impressions"])
    col_clk = _resolve_col(cols, COLUMN_CANDIDATES["clicks"])
    col_spend = _resolve_col(cols, COLUMN_CANDIDATES["spend"])
    col_conv = _resolve_col(cols, COLUMN_CANDIDATES["conversions"])
    col_rev = _resolve_col(cols, COLUMN_CANDIDATES["revenue"])

    if not (col_camp and col_spend):
        return pd.DataFrame()

    work = df.copy()
    work["_spend"] = work[col_spend].map(_clean_int)
    work["_imp"] = work[col_imp].map(_clean_int) if col_imp else 0
    work["_clk"] = work[col_clk].map(_clean_int) if col_clk else 0
    work["_conv"] = work[col_conv].map(_clean_int) if col_conv else 0
    work["_rev"] = work[col_rev].map(_clean_int) if col_rev else 0

    agg = (
        work.groupby(col_camp)
        .agg(
            spend=("_spend", "sum"),
            impressions=("_imp", "sum"),
            clicks=("_clk", "sum"),
            conversions=("_conv", "sum"),
            revenue=("_rev", "sum"),
        )
        .reset_index()
        .rename(columns={col_camp: "campaign_name"})
    )

    agg["brand"] = agg["campaign_name"].astype(str).map(classify_coupang_ad_to_brand)
    agg["ctr_pct"] = (
        agg["clicks"] / agg["impressions"].replace(0, pd.NA) * 100
    ).round(2).fillna(0)
    agg["cpc"] = (
        agg["spend"] / agg["clicks"].replace(0, pd.NA)
    ).round(0).fillna(0).astype(int)
    agg["roas_pct"] = (
        agg["revenue"] / agg["spend"].replace(0, pd.NA) * 100
    ).round(0).fillna(0).astype(int)

    return agg.sort_values("spend", ascending=False).reset_index(drop=True)


def parse_to_campaigns_daily(df: pd.DataFrame) -> pd.DataFrame:
    """캠페인 × 일자 단위 집계 — loader 가 기간 슬라이스 후 합산.

    파생 지표(ctr/cpc/roas)는 저장하지 않음 (합산 시 부정확).

    반환 컬럼: date, campaign_name, brand, spend, impressions,
              clicks, conversions, revenue
    """
    cols = list(df.columns.astype(str))
    col_date = _resolve_col(cols, COLUMN_CANDIDATES["date"])
    col_camp = _resolve_col(cols, COLUMN_CANDIDATES["campaign"])
    col_imp = _resolve_col(cols, COLUMN_CANDIDATES["impressions"])
    col_clk = _resolve_col(cols, COLUMN_CANDIDATES["clicks"])
    col_spend = _resolve_col(cols, COLUMN_CANDIDATES["spend"])
    col_conv = _resolve_col(cols, COLUMN_CANDIDATES["conversions"])
    col_rev = _resolve_col(cols, COLUMN_CANDIDATES["revenue"])

    if not (col_camp and col_spend and col_date):
        return pd.DataFrame()

    work = df.copy()
    work["_date"] = work[col_date].map(_normalize_date)
    work = work.dropna(subset=["_date"])
    if work.empty:
        return pd.DataFrame()

    work["_spend"] = work[col_spend].map(_clean_int)
    work["_imp"] = work[col_imp].map(_clean_int) if col_imp else 0
    work["_clk"] = work[col_clk].map(_clean_int) if col_clk else 0
    work["_conv"] = work[col_conv].map(_clean_int) if col_conv else 0
    work["_rev"] = work[col_rev].map(_clean_int) if col_rev else 0

    agg = (
        work.groupby(["_date", col_camp])
        .agg(
            spend=("_spend", "sum"),
            impressions=("_imp", "sum"),
            clicks=("_clk", "sum"),
            conversions=("_conv", "sum"),
            revenue=("_rev", "sum"),
        )
        .reset_index()
        .rename(columns={"_date": "date", col_camp: "campaign_name"})
    )
    agg["brand"] = agg["campaign_name"].astype(str).map(classify_coupang_ad_to_brand)

    return agg[[
        "date", "campaign_name", "brand",
        "spend", "impressions", "clicks", "conversions", "revenue",
    ]].sort_values(["date", "spend"], ascending=[True, False]).reset_index(drop=True)
