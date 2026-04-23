"""쿠팡 Wing 상품별 판매 CSV/Excel → orders.csv 자동 병합 CLI.

쿠팡 로켓배송(벤더 풀필먼트) 주문은 Open API 미제공 → Wing 매출 리포트 CSV
수동 다운로드 → 이 스크립트로 파싱 → orders.csv 누적 병합.

사용법:
    1. 쿠팡 Wing → 판매 관리 → 상품별 판매 리포트 / 통계 → CSV 다운로드
    2. data/coupang_sales_upload/ 폴더에 드롭
    3. 실행: .venv\\Scripts\\python.exe scripts\\sync_coupang_sales_csv.py
"""
from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.coupang_sales_csv import (  # noqa: E402
    read_coupang_sales_file, parse_to_orders,
)
from utils.data import ORDERS_FILE  # noqa: E402


UPLOAD_DIR = ROOT / "data" / "coupang_sales_upload"
LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [coupang_sales] {msg}"
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
    p = argparse.ArgumentParser(description="쿠팡 판매 CSV → orders.csv 누적 병합")
    p.add_argument("--file", type=str, default=None,
                   help="특정 파일 경로 (미지정 시 업로드 폴더 최신)")
    return p.parse_args()


def merge_into_orders_csv(new_df) -> tuple[int, int, int]:
    """누적 병합: 새 CSV 날짜 범위만 교체, 그 외 기존 데이터 보존.

    Returns: (보존 행, 중복 제거 행, 신규 추가 행)
    """
    import pandas as pd

    if new_df.empty:
        return 0, 0, 0

    new_df = new_df.copy()
    new_df["date"] = new_df["date"].astype(str)
    new_dates = set(new_df["date"])
    coupang_stores = {"쿠팡", "쿠팡_똑똑연구소", "쿠팡_롤라루"}

    if ORDERS_FILE.exists():
        existing = pd.read_csv(ORDERS_FILE)
        existing["date"] = existing["date"].astype(str)
        if "store" not in existing.columns:
            existing["store"] = existing["channel"]
        # 쿠팡 스토어 × 새 파일 날짜 행만 제거, 나머지 보존
        mask = (
            existing["store"].isin(coupang_stores)
            & existing["date"].isin(new_dates)
        )
        removed = int(mask.sum())
        kept = existing[~mask]
        total_kept = len(kept)
    else:
        kept = pd.DataFrame()
        total_kept = 0
        removed = 0

    merged = pd.concat([kept, new_df], ignore_index=True)
    merged = merged.sort_values(["date", "store", "product"]).reset_index(drop=True)
    merged.to_csv(ORDERS_FILE, index=False, encoding="utf-8-sig")
    return total_kept, removed, len(new_df)


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
            log(f"업로드 폴더에 파일 없음 (쿠팡 판매 CSV 드롭 대기): {UPLOAD_DIR}")
            return 0
        target = max(candidates, key=lambda p: p.stat().st_mtime)
        log(f"업로드 폴더 최신 파일 사용: {target.name}")

    try:
        raw = read_coupang_sales_file(target)
        log(f"원본 {len(raw)}행, 헤더 {list(raw.columns)[:10]}")
    except Exception as e:
        log(f"파일 읽기 실패: {type(e).__name__}: {e}")
        return 2

    try:
        orders_df = parse_to_orders(raw)
    except ValueError as e:
        log(f"컬럼 매핑 실패: {e}")
        return 3

    if orders_df.empty:
        log("파싱 결과 0건")
        return 0

    total_qty = int(orders_df["quantity"].sum())
    total_rev = int(orders_df["revenue"].sum())
    log(
        f"파싱 완료: {len(orders_df)}행 / 수량 {total_qty:,}개 "
        f"/ 매출 {total_rev:,}원"
    )

    for store in sorted(orders_df["store"].unique()):
        sdf = orders_df[orders_df["store"] == store]
        rev = int(sdf["revenue"].sum())
        qty = int(sdf["quantity"].sum())
        products_n = sdf["product"].nunique()
        log(f"  [{store}] {len(sdf)}행 / {products_n} 상품 / "
            f"수량 {qty:,}개 / 매출 {rev:,}원")

    try:
        kept, removed, added = merge_into_orders_csv(orders_df)
        log(
            f"orders.csv 누적 병합: 기존 쿠팡 데이터 {removed}행 교체 · "
            f"신규 {added}행 추가 · 보존 {kept}행"
        )
    except Exception as e:
        log(f"병합 실패: {type(e).__name__}: {e}")
        return 4

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
                f.write(f"[{ts}] [coupang_sales] UNCAUGHT:\n{tb}\n")
        except Exception:
            pass
        sys.exit(99)
