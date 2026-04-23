"""네이버 검색광고 성과 리포트 CSV → ads.csv 스키마로 변환.

사용 경로:
    네이버 검색광고 시스템 (searchad.naver.com)
    → 보고서 → 다차원 보고서 / 성과 리포트 → '일자별' 선택
    → 엑셀/CSV 다운로드

다음 컬럼이 있으면 자동 매핑됩니다:
    일자 / 노출수 / 클릭수 / 총비용 / 전환수 / 전환매출액
"""
from __future__ import annotations

import io
from typing import Union

import pandas as pd


# 네이버 검색광고 리포트의 가능한 컬럼명 → 표준 컬럼명
COLUMN_ALIASES: dict[str, list[str]] = {
    "date": ["일자", "날짜", "일별", "date", "Date"],
    "impressions": ["노출수", "노출", "임프레션", "총노출수", "Impressions", "Impr"],
    "clicks": ["클릭수", "클릭", "총클릭수", "Clicks", "Click"],
    "spend": ["총비용", "비용", "광고비", "총 비용", "Cost", "Spend"],
    "conversions": [
        "전환수", "전환", "총전환수", "총 전환수",
        "Conversions", "Conv",
    ],
    "revenue": [
        "전환매출액", "전환 매출액", "전환매출", "매출", "매출액",
        "총전환매출액", "총 전환매출액",
        "Revenue", "Total Conv Value", "Conv Value",
    ],
}


def _read_with_encoding(source: Union[str, bytes, io.IOBase]) -> pd.DataFrame:
    """UTF-8-BOM → UTF-8 → CP949 → EUC-KR 순으로 인코딩 자동 감지."""
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
        f"CSV 인코딩을 해석할 수 없습니다 (시도: utf-8, cp949, euc-kr). 원인: {last_err}"
    )


def _find_column(columns: list[str], aliases: list[str]) -> str | None:
    """대소문자·공백 무시하고 컬럼 이름 매칭."""
    normalized = {c.strip().lower(): c for c in columns}
    for alias in aliases:
        key = alias.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def _rename_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    """원본 컬럼명을 표준 컬럼명으로 변경."""
    rename_map = {}
    for std, aliases in COLUMN_ALIASES.items():
        col = _find_column(list(df.columns), aliases)
        if col is not None and col != std:
            rename_map[col] = std
    return df.rename(columns=rename_map)


def _clean_numeric(series: pd.Series) -> pd.Series:
    """'1,234' / ' 1234원 ' 같은 문자열을 숫자로."""
    return pd.to_numeric(
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("원", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip(),
        errors="coerce",
    ).fillna(0)


def convert(source: Union[str, bytes, io.IOBase]) -> pd.DataFrame:
    """네이버 검색광고 CSV를 대시보드 ads.csv 포맷으로 변환.

    반환 DataFrame 컬럼:
        date, channel, spend, impressions, clicks, conversions, revenue
    """
    df = _read_with_encoding(source)
    df = _rename_to_standard(df)

    required = ["date", "spend", "clicks"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"네이버 검색광고 CSV에서 필수 컬럼을 찾을 수 없습니다: {missing}\n"
            f"CSV 실제 컬럼: {list(df.columns)}\n"
            f"네이버 검색광고 → 보고서 → '다차원 보고서' 또는 '성과 리포트' → "
            f"'일자별' 기준으로 내보내기 하세요."
        )

    # 없을 수 있는 컬럼 기본값
    for col in ["impressions", "conversions", "revenue"]:
        if col not in df.columns:
            df[col] = 0

    # 숫자 정리
    for col in ["impressions", "clicks", "spend", "conversions", "revenue"]:
        df[col] = _clean_numeric(df[col])

    # 날짜 정리
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # 키워드·그룹·캠페인별로 쪼개져 있어도 날짜별 합산
    agg = df.groupby("date", as_index=False).agg(
        spend=("spend", "sum"),
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        conversions=("conversions", "sum"),
        revenue=("revenue", "sum"),
    )

    agg["channel"] = "네이버"
    for col in ["spend", "impressions", "clicks", "conversions", "revenue"]:
        agg[col] = agg[col].round(0).astype(int)

    return agg[["date", "channel", "spend", "impressions", "clicks", "conversions", "revenue"]]
