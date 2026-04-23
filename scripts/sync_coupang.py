"""쿠팡 Wing API 주문 자동 동기화 CLI.

환경변수 (.env):
    COUPANG_ACCESS_KEY
    COUPANG_SECRET_KEY
    COUPANG_VENDOR_ID
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.coupang_wing import load_coupang_client_from_env  # noqa: E402
from utils.data import merge_store_orders  # noqa: E402
from utils.products import classify_product  # noqa: E402


LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [coupang] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="쿠팡 Wing API 주문 동기화")
    p.add_argument("--days", type=int, default=3,
                   help="전일 기준 며칠치 동기화 (기본 3일)")
    p.add_argument("--since", type=str, default=None)
    p.add_argument("--until", type=str, default=None)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    client = load_coupang_client_from_env()
    if client is None:
        log("ERROR: COUPANG_ACCESS_KEY / SECRET_KEY / VENDOR_ID 환경변수 누락")
        return 1

    until = date.fromisoformat(args.until) if args.until else date.today() - timedelta(days=1)
    since = date.fromisoformat(args.since) if args.since else until - timedelta(days=args.days - 1)

    log(f"동기화 시작: {since} ~ {until}")

    ok, msg = client.test_connection()
    if not ok:
        log(f"인증 실패 - {msg}")
        return 2
    log(f"인증 OK - {msg}")

    try:
        df = client.fetch_orders_df(since, until)
    except Exception as e:
        log(f"조회 실패 - {type(e).__name__}: {e}")
        return 2

    if df.empty:
        log("조회 결과 없음.")
        return 0

    # 상품명 기반 브랜드 자동 분류 → store 값 분기
    #   똑똑연구소 상품 → store='쿠팡_똑똑연구소'
    #   롤라루 상품     → store='쿠팡_롤라루'
    #   미분류          → store='쿠팡' (fallback)
    def _store_label(product_name: str) -> str:
        _, umbrella = classify_product(product_name)
        if umbrella == "똑똑연구소":
            return "쿠팡_똑똑연구소"
        if umbrella == "롤라루":
            return "쿠팡_롤라루"
        return "쿠팡"

    df["store"] = df["product"].map(_store_label)

    total_revenue = int(df["revenue"].sum())
    unique_customers = df["customer_id"].nunique()
    log(f"조회 완료: {len(df)}건 / 매출 {total_revenue:,}원 / 고객 {unique_customers}명")

    # 브랜드별 분기 결과 로그
    for s in sorted(df["store"].unique()):
        sdf = df[df["store"] == s]
        srev = int(sdf["revenue"].sum())
        log(f"  [{s}] {len(sdf):,}건 / 매출 {srev:,}원")

    # 스토어별 병합 (쿠팡_똑똑연구소 / 쿠팡_롤라루 / 쿠팡 각각)
    try:
        total_removed = 0
        total_added = 0
        for store_label in sorted(df["store"].unique()):
            subset = df[df["store"] == store_label]
            removed, added = merge_store_orders(subset, store_label)
            total_removed += removed
            total_added += added
            log(f"  병합 [{store_label}]: 기존 {removed}행 제거 -> 신규 {added}행 추가")
        # 구버전 store='쿠팡' 남은 데이터가 있으면 정리 (브랜드 있는 신규 데이터 기간 내만)
        _cleanup_legacy_coupang_store(df)
        log(f"병합 완료 (총): 제거 {total_removed}행, 추가 {total_added}행")
    except Exception as e:
        log(f"병합 실패 - {type(e).__name__}: {e}")
        return 3

    log("동기화 성공.")
    return 0


def _cleanup_legacy_coupang_store(new_df):
    """구버전 store='쿠팡' 데이터를 new_df 기간 내에서 정리.

    브랜드가 분류된 새 데이터가 들어온 날짜에 대해서는 구버전 '쿠팡' 레거시
    레코드를 삭제해 중복 집계를 방지한다.
    """
    import pandas as pd
    ORDERS_FILE = ROOT / "data" / "orders.csv"
    if not ORDERS_FILE.exists() or new_df.empty:
        return

    existing = pd.read_csv(ORDERS_FILE)
    existing["date"] = existing["date"].astype(str)
    new_dates = set(new_df["date"].astype(str))

    mask = (existing["store"] == "쿠팡") & (existing["date"].isin(new_dates))
    if mask.any():
        removed_n = int(mask.sum())
        existing = existing[~mask]
        existing.to_csv(ORDERS_FILE, index=False, encoding="utf-8-sig")
        log(f"  레거시 정리: store='쿠팡' 행 {removed_n}건 제거")


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
