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
from utils.precomputed import save_precomputed_parquet  # noqa: E402


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

    try:
        camp_df = parse_to_campaigns(raw)
        if not camp_df.empty:
            save_precomputed_parquet(camp_df, "coupang_campaigns.parquet")
            log(f"캠페인 집계 저장: {len(camp_df)} 캠페인")
    except Exception as e:
        log(f"캠페인 집계 실패: {type(e).__name__}: {e}")

    try:
        daily_df = parse_to_campaigns_daily(raw)
        if not daily_df.empty:
            save_precomputed_parquet(daily_df, "coupang_campaigns_daily.parquet")
            log(
                f"캠페인 × 일자 저장: {len(daily_df)} 행 "
                f"({daily_df['campaign_name'].nunique()} 캠페인 × "
                f"{daily_df['date'].nunique()} 일)"
            )
    except Exception as e:
        log(f"캠페인 × 일자 집계 실패: {type(e).__name__}: {e}")

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
