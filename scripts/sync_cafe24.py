"""Cafe24 자사몰 주문 자동 동기화 CLI.

환경변수 (.env):
    CAFE24_MALL_ID_DDOK, CAFE24_CLIENT_ID_DDOK, CAFE24_CLIENT_SECRET_DDOK
    CAFE24_MALL_ID_ROLLA, CAFE24_CLIENT_ID_ROLLA, CAFE24_CLIENT_SECRET_ROLLA

토큰 (data/cafe24_tokens.json):
    각 mall별 access_token / refresh_token / expires_at
    대시보드의 Cafe24 OAuth 플로우로 초기 발급 후 자동 갱신됨
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Streamlit Cloud / GitHub Actions 대응:
# CAFE24_TOKENS_JSON 환경변수가 있으면 파일로 덤프 (OAuth 토큰 복원)
import os  # noqa: E402
_tokens_env = os.getenv("CAFE24_TOKENS_JSON", "").strip()
if _tokens_env:
    _tokens_file = ROOT / "data" / "cafe24_tokens.json"
    _tokens_file.parent.mkdir(exist_ok=True)
    if not _tokens_file.exists():
        _tokens_file.write_text(_tokens_env, encoding="utf-8")
        print(f"[cafe24] CAFE24_TOKENS_JSON → {_tokens_file} 덤프 완료")

from api.cafe24 import load_all_cafe24_clients  # noqa: E402
from utils.data import merge_store_orders  # noqa: E402


LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [cafe24] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cafe24 자사몰 주문 동기화")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--since", type=str, default=None)
    p.add_argument("--until", type=str, default=None)
    p.add_argument("--only", type=str, default=None,
                   help="특정 스토어만 (자사몰_똑똑연구소 / 자사몰_롤라루)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    clients = load_all_cafe24_clients()
    if not clients:
        log("ERROR: Cafe24 자격증명 없음. .env에 CAFE24_MALL_ID_* 등 설정 필요.")
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
            log(f"[{store}] 인증 실패 — {msg}")
            exit_code = 2
            continue
        log(f"[{store}] 인증 OK")

        try:
            df = client.fetch_orders_df(since, until, store)
        except Exception as e:
            log(f"[{store}] 조회 실패 — {type(e).__name__}: {e}")
            exit_code = 2
            continue

        if df.empty:
            log(f"[{store}] 주문 없음")
            continue

        total_rev = int(df["revenue"].sum())
        log(f"[{store}] {len(df)}건 / 매출 {total_rev:,}원 / 고객 {df['customer_id'].nunique()}명")
        removed, added = merge_store_orders(df, store)
        log(f"[{store}] 병합: 기존 {removed}행 제거 → 신규 {added}행 추가")

    log(f"전체 종료 (exit={exit_code})")
    return exit_code


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
