"""네이버 커머스 API 자동 주문 동기화 CLI 스크립트.

Windows 작업 스케줄러에서 매일 자동 실행하도록 설계.

사용법:
    python scripts/sync_naver_commerce.py --days 3
    python scripts/sync_naver_commerce.py --since 2026-01-22 --until 2026-04-21

환경변수 (.env):
    NAVER_COMMERCE_CLIENT_ID_DDOK / _SECRET_DDOK
    NAVER_COMMERCE_CLIENT_ID_ROLLA / _SECRET_ROLLA

스토어 1개만 등록해도 해당 스토어만 동기화됨.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.naver_commerce import load_commerce_clients_from_env  # noqa: E402
from utils.data import merge_store_orders  # noqa: E402


LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [commerce] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="네이버 커머스 API 자동 주문 동기화")
    p.add_argument("--days", type=int, default=3,
                   help="전일 기준 며칠치 동기화 (기본 3일)")
    p.add_argument("--since", type=str, default=None, help="시작일 YYYY-MM-DD")
    p.add_argument("--until", type=str, default=None, help="종료일 YYYY-MM-DD")
    p.add_argument("--only", type=str, default=None,
                   help="특정 스토어만 동기화 (예: --only 똑똑연구소)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    clients = load_commerce_clients_from_env()
    if not clients:
        log("ERROR: 커머스 API 자격증명 없음. .env에 NAVER_COMMERCE_CLIENT_ID_* / _SECRET_* 설정 필요.")
        return 1

    if args.only:
        if args.only not in clients:
            log(f"ERROR: '{args.only}' 스토어 자격증명 없음. 등록된: {list(clients.keys())}")
            return 1
        clients = {args.only: clients[args.only]}

    until = date.fromisoformat(args.until) if args.until else date.today() - timedelta(days=1)
    since = date.fromisoformat(args.since) if args.since else until - timedelta(days=args.days - 1)

    log(f"동기화 시작: {since} ~ {until}, 스토어: {list(clients.keys())}")

    overall_exit = 0
    for store, client in clients.items():
        log(f"[{store}] 시작")
        ok, msg = client.test_connection()
        if not ok:
            log(f"[{store}] 인증 실패 — {msg}")
            overall_exit = 2
            continue

        try:
            df = client.fetch_orders_df(since, until, store)
        except Exception as e:
            log(f"[{store}] 조회 실패 — {type(e).__name__}: {e}")
            overall_exit = 2
            continue

        if df.empty:
            log(f"[{store}] 조회 결과 없음.")
            continue

        total_revenue = int(df["revenue"].sum())
        unique_customers = df["customer_id"].nunique()
        log(f"[{store}] 조회 완료: {len(df)}건 / 매출 {total_revenue:,}원 / 고객 {unique_customers}명")

        try:
            removed, added = merge_store_orders(df, store)
            log(f"[{store}] 병합 완료: 기존 {removed}행 제거 → 신규 {added}행 추가")
        except Exception as e:
            log(f"[{store}] 병합 실패 — {type(e).__name__}: {e}")
            overall_exit = 3

    log(f"전체 동기화 종료 (exit={overall_exit})")
    return overall_exit


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        try:
            log(f"UNCAUGHT:\n{traceback.format_exc()}")
        except Exception:
            pass
        sys.exit(99)
