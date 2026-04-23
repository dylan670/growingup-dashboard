"""Meta Marketing API 광고 인사이트 자동 동기화 CLI.

환경변수 (.env):
    META_ACCESS_TOKEN_DDOK / META_AD_ACCOUNT_ID_DDOK   (똑똑연구소 자사몰 광고)
    META_ACCESS_TOKEN_ROLLA / META_AD_ACCOUNT_ID_ROLLA (롤라루 자사몰 광고)

또는 토큰이 하나일 때:
    META_ACCESS_TOKEN (공유) + META_AD_ACCOUNT_ID_DDOK / ROLLA
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.meta_ads import load_all_meta_clients  # noqa: E402
from utils.data import merge_store_orders  # noqa: E402 (실은 광고용이지만 store 기반 merge 재사용)


LOG_FILE = ROOT / "data" / "sync_log.txt"
ADS_FILE = ROOT / "data" / "ads.csv"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [meta] {msg}"
    # stdout 인코딩 실패(CP949)해도 스크립트가 죽지 않도록
    try:
        print(line)
    except UnicodeEncodeError:
        try:
            sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))
            sys.stdout.buffer.flush()
        except Exception:
            pass
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def merge_meta_ads_into_csv(new_df, store: str) -> tuple[int, int]:
    """ads.csv에 store 기반 merge. ads.csv에 store 컬럼 없으면 마이그레이션."""
    import pandas as pd

    new_dates = set(new_df["date"].astype(str))
    if ADS_FILE.exists():
        existing = pd.read_csv(ADS_FILE)
        existing["date"] = existing["date"].astype(str)
        # store 컬럼 없으면 기본값 채움 (네이버/쿠팡 구분용)
        if "store" not in existing.columns:
            existing["store"] = existing["channel"].map({
                "네이버": "네이버",
                "쿠팡": "쿠팡",
                "자사몰": "자사몰",
            }).fillna(existing["channel"])
        mask = (existing["store"] == store) & (existing["date"].isin(new_dates))
        removed = int(mask.sum())
        existing = existing[~mask]
    else:
        existing = pd.DataFrame()
        removed = 0

    merged = pd.concat([existing, new_df], ignore_index=True)
    # 모든 행에 store 컬럼이 있도록
    if "store" not in merged.columns:
        merged["store"] = merged["channel"]
    merged = merged.sort_values(["date", "channel", "store"]).reset_index(drop=True)
    merged.to_csv(ADS_FILE, index=False, encoding="utf-8-sig")
    return removed, len(new_df)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Meta 광고 자동 동기화")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--since", type=str, default=None)
    p.add_argument("--until", type=str, default=None)
    p.add_argument("--only", type=str, default=None)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    clients = load_all_meta_clients()
    if not clients:
        log("ERROR: Meta 자격증명 없음. .env에 META_ACCESS_TOKEN_* 등 설정 필요.")
        return 1

    if args.only and args.only in clients:
        clients = {args.only: clients[args.only]}

    until = date.fromisoformat(args.until) if args.until else date.today() - timedelta(days=1)
    since = date.fromisoformat(args.since) if args.since else until - timedelta(days=args.days - 1)

    log(f"동기화 시작: {since} ~ {until}, 스토어: {list(clients.keys())}")

    exit_code = 0
    for store, client in clients.items():
        ok, msg = client.test_connection()
        if not ok:
            log(f"[{store}] 인증 실패 - {msg}")
            exit_code = 2
            continue
        log(f"[{store}] 인증 OK - {msg[:80]}")

        try:
            df = client.fetch_ads_df(since, until, store)
        except Exception as e:
            log(f"[{store}] 조회 실패 - {type(e).__name__}: {e}")
            exit_code = 2
            continue

        if df.empty:
            log(f"[{store}] 광고 데이터 없음.")
            continue

        total_spend = int(df["spend"].sum())
        total_rev = int(df["revenue"].sum())
        roas = total_rev / total_spend * 100 if total_spend else 0
        log(f"[{store}] {len(df)}일 / 광고비 {total_spend:,}원 / 매출 {total_rev:,}원 / ROAS {roas:.0f}%")

        try:
            removed, added = merge_meta_ads_into_csv(df, store)
            log(f"[{store}] 병합: 기존 {removed}행 제거 -> 신규 {added}행 추가")
        except Exception as e:
            log(f"[{store}] 병합 실패 - {type(e).__name__}: {e}")
            exit_code = 3

    log(f"전체 종료 (exit={exit_code})")
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        tb = traceback.format_exc()
        # stderr 로 먼저 출력 (log() 가 실패해도 보이도록)
        try:
            sys.stderr.write(tb)
            sys.stderr.flush()
        except Exception:
            pass
        # 파일에도 기록 시도 (인코딩 안전하게)
        try:
            LOG_FILE.parent.mkdir(exist_ok=True)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{ts}] [meta] UNCAUGHT:\n{tb}\n")
        except Exception:
            pass
        sys.exit(99)
