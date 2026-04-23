"""쿠팡 광고 CSV/Excel → ads.csv 자동 병합 CLI.

사용법:
    1. 쿠팡 광고센터 → 리포트 → CSV/Excel 다운로드
    2. data/coupang_ads_upload/ 폴더에 드롭
    3. 실행: .venv\\Scripts\\python.exe scripts\\sync_coupang_ads_csv.py
"""
from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.coupang_ads_csv import (  # noqa: E402
    read_coupang_ads_file, parse_to_ads, parse_to_campaigns,
    parse_to_campaigns_daily,
)
from utils.data import ADS_FILE  # noqa: E402
from utils.precomputed import (  # noqa: E402
    save_precomputed_parquet, load_precomputed_parquet, PRECOMP_DIR,
)


UPLOAD_DIR = ROOT / "data" / "coupang_ads_upload"
LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [coupang_ads] {msg}"
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="쿠팡 광고 CSV/Excel → ads.csv 병합")
    p.add_argument("--file", type=str, default=None,
                   help="특정 파일 경로 (미지정 시 업로드 폴더 최신)")
    return p.parse_args()


def merge_into_ads_csv(new_df) -> tuple[int, int]:
    import pandas as pd

    if new_df.empty:
        return 0, 0

    new_dates = set(new_df["date"].astype(str))
    coupang_stores = {"쿠팡", "쿠팡_똑똑연구소", "쿠팡_롤라루"}

    if ADS_FILE.exists():
        existing = pd.read_csv(ADS_FILE)
        existing["date"] = existing["date"].astype(str)
        if "store" not in existing.columns:
            existing["store"] = existing["channel"]
        mask = (
            existing["store"].isin(coupang_stores)
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


def main() -> int:
    args = parse_args()

    if args.file:
        target = Path(args.file)
        if not target.exists():
            log(f"파일 없음: {target}")
            return 1
    else:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        candidates = [
            p for p in UPLOAD_DIR.glob("*")
            if p.suffix.lower() in (".csv", ".xlsx", ".xls")
            and not p.name.startswith(".")
            and p.name.lower() != "readme.txt"
        ]
        if not candidates:
            log(f"업로드 폴더에 파일 없음 (쿠팡 광고 CSV 드롭 대기): {UPLOAD_DIR}")
            return 0
        target = max(candidates, key=lambda p: p.stat().st_mtime)
        log(f"업로드 폴더 최신 파일 사용: {target.name}")

    try:
        raw = read_coupang_ads_file(target)
        log(f"원본 {len(raw)}행, 헤더 {list(raw.columns)[:8]}")
    except Exception as e:
        log(f"파일 읽기 실패: {type(e).__name__}: {e}")
        return 2

    try:
        ads_df = parse_to_ads(raw)
    except ValueError as e:
        log(f"컬럼 매핑 실패: {e}")
        return 3

    if ads_df.empty:
        log("파싱 결과 0건")
        return 0

    total_spend = int(ads_df["spend"].sum())
    total_rev = int(ads_df["revenue"].sum())
    roas = total_rev / total_spend * 100 if total_spend else 0
    log(
        f"파싱 완료: {len(ads_df)}행 / 광고비 {total_spend:,}원 "
        f"/ 매출 {total_rev:,}원 / ROAS {roas:.0f}%"
    )

    for store in sorted(ads_df["store"].unique()):
        sdf = ads_df[ads_df["store"] == store]
        spend = int(sdf["spend"].sum())
        log(f"  [{store}] {len(sdf)}행 / 광고비 {spend:,}원")

    try:
        removed, added = merge_into_ads_csv(ads_df)
        log(f"병합 완료: 기존 {removed}행 제거 -> 신규 {added}행 추가")
    except Exception as e:
        log(f"병합 실패: {type(e).__name__}: {e}")
        return 4

    # ----- 캠페인 × 일자 parquet: 누적 병합 (중복 날짜만 새 데이터로 교체) -----
    try:
        import pandas as pd

        new_daily = parse_to_campaigns_daily(raw)
        if new_daily.empty:
            log("일자 단위 파싱 결과 0건 (skip)")
        else:
            new_daily["date"] = new_daily["date"].astype(str)
            new_dates = set(new_daily["date"])

            existing_path = PRECOMP_DIR / "coupang_campaigns_daily.parquet"
            if existing_path.exists():
                existing = pd.read_parquet(existing_path)
                existing["date"] = existing["date"].astype(str)
                # 새 파일 날짜 범위에 해당하는 기존 행 제거 → 신규로 덮어쓰기
                # (그 외 날짜의 기존 데이터는 보존)
                overlap_mask = existing["date"].isin(new_dates)
                removed = int(overlap_mask.sum())
                kept = existing[~overlap_mask]
            else:
                kept = pd.DataFrame()
                removed = 0

            merged = pd.concat([kept, new_daily], ignore_index=True)
            merged = merged.sort_values(["date", "campaign_name"]).reset_index(drop=True)
            save_precomputed_parquet(merged, "coupang_campaigns_daily.parquet")

            log(
                f"일자 parquet 누적 병합: 보존 {len(kept)}행 + 신규 {len(new_daily)}행 "
                f"(중복 {removed}행 교체) → 총 {len(merged)}행 "
                f"({merged['campaign_name'].nunique()} 캠페인 × "
                f"{merged['date'].nunique()} 일, "
                f"{merged['date'].min()} ~ {merged['date'].max()})"
            )

            # ----- legacy 전체 합계 parquet: 누적된 daily 에서 재계산 -----
            agg = (
                merged.groupby("campaign_name")
                .agg(
                    spend=("spend", "sum"),
                    impressions=("impressions", "sum"),
                    clicks=("clicks", "sum"),
                    conversions=("conversions", "sum"),
                    revenue=("revenue", "sum"),
                )
                .reset_index()
            )
            from utils.products import classify_coupang_ad_to_brand
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
            agg = agg.sort_values("spend", ascending=False).reset_index(drop=True)
            save_precomputed_parquet(agg, "coupang_campaigns.parquet")
            log(f"legacy 집계 parquet 재계산: {len(agg)} 캠페인 "
                f"(총 광고비 {int(agg['spend'].sum()):,}원, "
                f"매출 {int(agg['revenue'].sum()):,}원)")
    except Exception as e:
        import traceback
        log(f"캠페인 병합 실패: {type(e).__name__}: {e}")
        log(traceback.format_exc())

    log("동기화 성공.")
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
                f.write(f"[{ts}] [coupang_ads] UNCAUGHT:\n{tb}\n")
        except Exception:
            pass
        sys.exit(99)
