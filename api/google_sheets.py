"""Google Sheets — 일간 매출 목표·달성 데이터 파서.

소스: 2026 그로잉업팀 일간통계 시트
      3개 브랜드(롤라루·똑똑연구소·루티니스트)가 가로로 섹션 나열된 구조.
      공개 CSV export URL 이므로 인증 불필요.

반환 포맷 (long format):
    date | brand | channel | target | actual
"""
from __future__ import annotations

import io
import os
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env", override=False)

# Streamlit Cloud 대응 — st.secrets → os.environ 즉시 승격
try:
    from utils.env_bootstrap import bootstrap_env
    bootstrap_env()
except Exception:
    pass


# ==========================================================
# 브랜드별 채널 컬럼 매핑
#   - 첫 번째 채널 중복 시 pandas 가 자동으로 '.1', '.2' suffix 부여
#   - 무신사 컬럼은 시트에 오타 있음 ("일당성")
# ==========================================================
BRAND_CHANNEL_COLUMNS: dict[str, list[tuple[str, str, str]]] = {
    # (채널 표시명, 목표 컬럼명, 달성 컬럼명)
    # 목표 컬럼명이 빈 문자열 "" 이면 해당 채널은 target=0 (달성만 기록)
    "롤라루": [
        ("자사몰",             "자사몰_일목표",    "자사몰_일달성"),
        ("네이버 스마트스토어", "스스_일목표",     "스스_일달성"),
        # 롤라루 쿠팡 = 쿠팡이 직매입하는 '로켓배송' (Wing API 불가, 시트 전용)
        ("쿠팡 로켓배송",      "쿠팡_일목표",     "쿠팡_일달성"),
        # 쿠팡 판매자 판매 (Wing) — 시트 단일 컬럼 (달성만)
        ("쿠팡 판매자 판매",   "",                "쿠팡 판매자 판매"),
        ("무신사",             "무신사 일목표",   "무신사 일당성"),  # sheet typo: 일당성
        ("오프라인",           "오프라인_일목표", "오프라인_일달성"),
        ("이지웰",             "이지웰_일목표",   "이지웰_일달성"),
        ("오늘의집",           "오늘의집_일목표", "오늘의집_일달성"),
    ],
    "똑똑연구소": [
        ("자사몰",             "자사몰_일목표.1",  "자사몰_일달성.1"),
        ("네이버 스마트스토어", "스스_일목표.1",   "스스_일달성.1"),
        # 똑똑 쿠팡 = Wing API 의 쿠팡 로켓그로스와 동일 (매출 일치)
        ("쿠팡 로켓그로스",    "쿠팡_일목표.1",   "쿠팡_일달성.1"),
    ],
    "루티니스트": [
        ("자사몰",             "자사몰_일목표.2",  "자사몰_일달성.2"),
        ("네이버 스마트스토어", "스스_일목표.2",   "스스_일달성.2"),
    ],
}


def _env_sheet_id() -> str:
    return os.getenv("GOOGLE_SHEET_ID", "").strip()


def _env_gid() -> str:
    return os.getenv("GOOGLE_SHEET_GID", "0").strip()


def fetch_sheet_csv(sheet_id: str | None = None,
                    gid: str | None = None,
                    timeout: int = 20) -> str:
    """공개 시트 CSV export 다운로드. 원문 텍스트 반환."""
    sid = sheet_id or _env_sheet_id()
    g = gid or _env_gid()
    if not sid:
        raise ValueError(".env 의 GOOGLE_SHEET_ID 설정 필요")

    url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={g}"
    r = requests.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text


def _safe_num(val) -> int:
    """NaN·빈칸·문자열 → 0, 숫자는 int 로."""
    if pd.isna(val):
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def parse_daily_sales(csv_text: str) -> pd.DataFrame:
    """시트 CSV → long format DataFrame.

    반환: date (datetime.date) · brand · channel · target · actual
          (date×brand×channel 조합당 1행)
    """
    # header=1 이면 2번째 줄이 컬럼명, 3번째 줄부터 데이터
    # 2번째 행 (index 0 = 합계 총합 행) 은 skiprows 로 제외
    df = pd.read_csv(
        io.StringIO(csv_text), header=1, thousands=",",
        skiprows=[2],  # "합계" row
    )

    # 월 컬럼은 매월 첫 날에만 값, 이후는 빈값 → forward fill
    if "월" in df.columns:
        df["월"] = df["월"].ffill()

    # date 컬럼 생성 (YYYYMM + 일 → date)
    def _make_date(row):
        m = str(row.get("월", "")).strip()
        d = row.get("일")
        if not m or m == "nan" or pd.isna(d):
            return None
        try:
            year = int(m[:4])
            month = int(m[4:6])
            day = int(d)
            return date(year, month, day)
        except (ValueError, TypeError):
            return None

    df["_date"] = df.apply(_make_date, axis=1)
    df = df[df["_date"].notna()].copy()

    # long format 변환
    records: list[dict] = []
    for _, row in df.iterrows():
        d = row["_date"]
        for brand, channels in BRAND_CHANNEL_COLUMNS.items():
            for ch_name, t_col, a_col in channels:
                target = _safe_num(row.get(t_col)) if t_col else 0
                actual = _safe_num(row.get(a_col)) if a_col else 0
                # 둘 다 0 이면 스킵 (해당 채널 운영 안 하는 날)
                if target == 0 and actual == 0:
                    continue
                records.append({
                    "date": d,
                    "brand": brand,
                    "channel": ch_name,
                    "target": target,
                    "actual": actual,
                })

    result = pd.DataFrame(records)
    if not result.empty:
        result["date"] = pd.to_datetime(result["date"])
    return result


# ==========================================================
# 편의 함수 — 대시보드에서 호출
# ==========================================================

def load_sheet_daily_sales(prefer_precomputed: bool = True) -> pd.DataFrame:
    """전체 시트 매출 데이터 로드 (date·brand·channel·target·actual).

    prefer_precomputed=True 이면 data/precomputed/sheet_daily_sales.parquet
    를 먼저 시도 (네트워크 없이 즉시 반환). 파일 없으면 live 다운로드.
    """
    if prefer_precomputed:
        try:
            from utils.precomputed import load_precomputed_parquet
            df = load_precomputed_parquet("sheet_daily_sales.parquet")
            if not df.empty:
                # 날짜 컬럼 type 복원
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                return df
        except Exception:
            pass

    # Fallback: live fetch
    csv_text = fetch_sheet_csv()
    return parse_daily_sales(csv_text)


def get_brand_channels(brand: str) -> list[str]:
    """브랜드의 채널 목록 (표시명)."""
    return [ch for ch, _, _ in BRAND_CHANNEL_COLUMNS.get(brand, [])]
