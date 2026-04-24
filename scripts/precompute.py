"""프리컴퓨트 스크립트 — 매일 10시 sync 직후 실행.

1. 구글 시트 다운로드 → Parquet 저장 (네트워크 의존 제거)
2. Meta/Naver 캠페인 API 조회 → Parquet (느린 API 호출 캐시)
3. 브랜드 × 기간 KPI 집계 → JSON (즉시 로드용)
4. 인사이트 룰 실행 결과 → JSON

대시보드는 이 파일만 읽어서 즉시 렌더링.
"""
from __future__ import annotations

import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from utils.precomputed import (  # noqa: E402
    save_precomputed_parquet,
    save_precomputed_json,
    mark_last_updated,
)


LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [precompute] {msg}"
    try:
        print(line)
    except UnicodeEncodeError:
        try:
            sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))
        except Exception:
            pass
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def precompute_sheet() -> int:
    """구글 시트 다운로드 → Parquet 저장. 반환: 저장한 행 수."""
    from api.google_sheets import load_sheet_daily_sales

    log("구글 시트 다운로드 시작")
    df = load_sheet_daily_sales()
    if df.empty:
        log("시트 비어있음 (skip)")
        return 0

    save_precomputed_parquet(df, "sheet_daily_sales.parquet")
    log(f"시트 저장 완료: {len(df)}행")
    return len(df)


def precompute_campaigns(days: int = 90) -> dict:
    """Meta/네이버 캠페인 × 일자 단위 → Parquet (loader 기간별 슬라이스).

    저장 파일:
      meta_campaigns_{brand}_daily.parquet — 컬럼: date, campaign_id,
        campaign_name, spend, impressions, clicks, conversions, revenue, brand
      naver_campaigns_daily.parquet — 컬럼: date, campaign_id, campaign_name,
        brand, spend, impressions, clicks, conversions, revenue
    """
    from api.meta_ads import load_meta_client
    from api.naver_searchad import load_client_from_env

    until = date.today() - timedelta(days=1)
    since = until - timedelta(days=days - 1)

    results = {}

    # Meta — 브랜드별, 일자 단위
    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        try:
            client = load_meta_client(brand)
            if client is None:
                log(f"Meta {brand}: 자격증명 없음 (skip)")
                continue
            df = client.fetch_campaigns_daily_df(since, until)
            if not df.empty:
                df["brand"] = brand
            save_precomputed_parquet(df, f"meta_campaigns_{brand}_daily.parquet")
            log(f"Meta {brand}: {len(df)} 행 저장 "
                f"(광고비 {int(df['spend'].sum()) if not df.empty else 0:,}원 "
                f"/ {df['campaign_id'].nunique() if not df.empty else 0} 캠페인 × "
                f"{df['date'].nunique() if not df.empty else 0} 일)")
            results[f"meta_{brand}_daily"] = len(df)
        except Exception as e:
            log(f"Meta {brand} 실패: {type(e).__name__}: {e}")

    # 네이버 검색광고 — 캠페인 × 일자 (느림: 최대 10분)
    try:
        client = load_client_from_env()
        if client is None:
            log("네이버 검색광고 자격증명 없음 (skip)")
        else:
            df = client.fetch_campaigns_daily_df(since, until)
            save_precomputed_parquet(df, "naver_campaigns_daily.parquet")
            log(f"네이버 검색광고: {len(df)} 행 저장 "
                f"(광고비 {int(df['spend'].sum()) if not df.empty else 0:,}원 "
                f"/ {df['campaign_id'].nunique() if not df.empty else 0} 캠페인 × "
                f"{df['date'].nunique() if not df.empty else 0} 일)")
            results["naver_daily"] = len(df)
    except Exception as e:
        log(f"네이버 캠페인 실패: {type(e).__name__}: {e}")

    return results


def precompute_home_snapshot() -> dict:
    """홈 KPI 스냅샷 (여러 기간 미리 계산) → JSON.

    대시보드는 이 JSON 바로 읽어서 KPI 그리면 됨.
    """
    from api.google_sheets import load_sheet_daily_sales
    from utils.data import load_orders, load_ads

    sheet = load_sheet_daily_sales()
    orders = load_orders()
    ads = load_ads()

    today = (
        orders["date"].max().date()
        if not orders.empty else date.today() - timedelta(days=1)
    )

    # 여러 기간 프리컴퓨트
    periods = {
        "이번달": (pd.Timestamp(today.replace(day=1)), pd.Timestamp(today)),
        "지난7일": (pd.Timestamp(today) - pd.Timedelta(days=6), pd.Timestamp(today)),
        "지난30일": (pd.Timestamp(today) - pd.Timedelta(days=29), pd.Timestamp(today)),
        "지난90일": (pd.Timestamp(today) - pd.Timedelta(days=89), pd.Timestamp(today)),
    }

    snapshot = {
        "computed_at": datetime.now().isoformat(),
        "data_today": str(today),
        "periods": {},
    }

    for label, (start, end) in periods.items():
        # 전체
        period_sheet = sheet[(sheet["date"] >= start) & (sheet["date"] <= end)]
        total_target = int(period_sheet["target"].sum())
        total_actual = int(period_sheet["actual"].sum())
        pct = (total_actual / total_target * 100) if total_target else 0

        # 일별 집계
        daily = (
            period_sheet.groupby(period_sheet["date"].dt.date)
            .agg(actual=("actual", "sum"), target=("target", "sum"))
        )
        best_day_date = None
        best_day_rev = 0
        avg_daily = 0
        days_achieved = 0
        if not daily.empty:
            best = daily["actual"].idxmax()
            best_day_date = str(best)
            best_day_rev = int(daily.loc[best, "actual"])
            avg_daily = int(daily["actual"].mean())
            days_achieved = int((daily["actual"] >= daily["target"]).sum())

        # 브랜드별
        brand_summary = {}
        for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
            b_sheet = period_sheet[period_sheet["brand"] == brand]
            b_target = int(b_sheet["target"].sum())
            b_actual = int(b_sheet["actual"].sum())
            brand_summary[brand] = {
                "target": b_target,
                "actual": b_actual,
                "pct": round(b_actual / b_target * 100, 1) if b_target else 0,
                "days_recorded": int(b_sheet["date"].nunique()),
            }

        snapshot["periods"][label] = {
            "start": str(start.date()),
            "end": str(end.date()),
            "days": (end - start).days + 1,
            "total_target": total_target,
            "total_actual": total_actual,
            "pct": round(pct, 1),
            "avg_daily": avg_daily,
            "best_day_date": best_day_date,
            "best_day_revenue": best_day_rev,
            "days_achieved": days_achieved,
            "days_total": len(daily),
            "brands": brand_summary,
        }

    # 월별 추이 (올해 전체)
    year_data = sheet[sheet["date"].dt.year == today.year].copy()
    year_data["month"] = year_data["date"].dt.month
    monthly = (
        year_data.groupby("month")
        .agg(target=("target", "sum"), actual=("actual", "sum"))
        .reset_index()
    )
    snapshot["monthly"] = monthly.to_dict(orient="records")

    save_precomputed_json(snapshot, "home_snapshot.json")
    log(f"홈 스냅샷 저장 ({len(periods)}개 기간 + 월별 {len(monthly)}개월)")
    return snapshot


def precompute_insights() -> list:
    """룰 기반 인사이트 계산 → JSON."""
    from api.google_sheets import load_sheet_daily_sales
    from utils.data import load_orders, load_ads
    from utils.insights import generate_insights

    sheet = load_sheet_daily_sales()
    orders = load_orders()
    ads = load_ads()

    today = (
        orders["date"].max().date()
        if not orders.empty else date.today() - timedelta(days=1)
    )
    start = pd.Timestamp(today.replace(day=1))
    end = pd.Timestamp(today)

    insights = generate_insights(sheet, ads, orders, start, end, max_count=6)
    save_precomputed_json({
        "computed_at": datetime.now().isoformat(),
        "period": f"{start.date()} ~ {end.date()}",
        "insights": insights,
    }, "insights_snapshot.json")
    log(f"인사이트 저장: {len(insights)}건")
    return insights


def main() -> int:
    log("========== 프리컴퓨트 시작 ==========")
    try:
        precompute_sheet()
    except Exception as e:
        log(f"시트 프리컴퓨트 실패: {type(e).__name__}: {e}")

    try:
        precompute_home_snapshot()
    except Exception as e:
        log(f"홈 스냅샷 실패: {type(e).__name__}: {e}")

    try:
        precompute_insights()
    except Exception as e:
        log(f"인사이트 실패: {type(e).__name__}: {e}")

    try:
        precompute_campaigns(days=90)
    except Exception as e:
        log(f"캠페인 프리컴퓨트 실패: {type(e).__name__}: {e}")

    mark_last_updated()
    log("========== 프리컴퓨트 완료 ==========")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        tb = traceback.format_exc()
        try:
            sys.stderr.write(tb)
        except Exception:
            pass
        try:
            LOG_FILE.parent.mkdir(exist_ok=True)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{ts}] [precompute] UNCAUGHT:\n{tb}\n")
        except Exception:
            pass
        sys.exit(99)
