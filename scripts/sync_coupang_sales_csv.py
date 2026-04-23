"""쿠팡 Supplier Hub 상품별 판매 CSV/Excel → orders.csv 자동 병합 CLI.

쿠팡 로켓배송(벤더 풀필먼트) 주문은 판매자 Open API 미제공 →
Supplier Hub (supplier.coupang.com) 매출 리포트 CSV 를 수동 다운로드 →
이 스크립트로 파싱 → orders.csv 누적 병합.

사용법:
    1. https://supplier.coupang.com/ → 애널리틱스 → 판매 분석
       → 일별 × 상품별 리포트 CSV 다운로드
    2. data/coupang_sales_upload/ 폴더에 드롭
    3. 실행: .venv\\Scripts\\python.exe scripts\\sync_coupang_sales_csv.py

주의: wing.coupang.com (판매자 Wing, 업체배송·로켓그로스용) 아니라
      supplier.coupang.com (벤더 Supplier Hub, 로켓배송용).
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
    _is_po_format, parse_po_files_to_orders,
)


UPLOAD_DIR = ROOT / "data" / "coupang_sales_upload"
LOG_FILE = ROOT / "data" / "sync_log.txt"

# 벤더 발주 데이터 전용 파일 — orders.csv (실 소비자 판매) 와 분리
# 제품 분석 페이지에서만 병합해서 표시 (매출 분석/CRM 미포함)
INBOUND_FILE = ROOT / "data" / "coupang_inbound.csv"


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


def merge_into_inbound_csv(new_df) -> tuple[int, int, int]:
    """coupang_inbound.csv 누적 병합 — orders.csv 와 완전히 분리.

    새 CSV 날짜 범위만 교체, 그 외 날짜 기존 데이터는 보존.
    Returns: (보존 행, 중복 제거 행, 신규 추가 행)
    """
    import pandas as pd

    if new_df.empty:
        return 0, 0, 0

    new_df = new_df.copy()
    new_df["date"] = new_df["date"].astype(str)
    new_dates = set(new_df["date"])

    if INBOUND_FILE.exists():
        existing = pd.read_csv(INBOUND_FILE)
        existing["date"] = existing["date"].astype(str)
        # 새 파일 날짜 행만 교체, 그 외 모두 보존
        mask = existing["date"].isin(new_dates)
        removed = int(mask.sum())
        kept = existing[~mask]
        total_kept = len(kept)
    else:
        kept = pd.DataFrame()
        total_kept = 0
        removed = 0

    merged = pd.concat([kept, new_df], ignore_index=True)
    merged = merged.sort_values(["date", "store", "product"]).reset_index(drop=True)
    INBOUND_FILE.parent.mkdir(exist_ok=True)
    merged.to_csv(INBOUND_FILE, index=False, encoding="utf-8-sig")
    return total_kept, removed, len(new_df)


def main() -> int:
    import pandas as pd

    args = parse_args()

    # 처리 대상 파일 결정 (단일 또는 폴더 내 전체)
    if args.file:
        target_files = [Path(args.file)]
        if not target_files[0].exists():
            log(f"파일 없음: {target_files[0]}")
            return 1
    else:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        target_files = sorted([
            p for p in UPLOAD_DIR.glob("*")
            if p.suffix.lower() in (".csv", ".xlsx", ".xls")
            and not p.name.startswith(".")
            and p.name.lower() != "readme.txt"
        ])
        if not target_files:
            log(f"업로드 폴더에 파일 없음: {UPLOAD_DIR}")
            return 0
        log(f"업로드 폴더 전체 파일 {len(target_files)}개 순차 처리 시작")

    # 파일 형식 판별 — PO (발주서) 양식 vs 일반 CSV/Excel 리포트
    po_files = [f for f in target_files if _is_po_format(f)]
    generic_files = [f for f in target_files if not _is_po_format(f)]
    log(
        f"파일 형식 분류: PO(발주서) {len(po_files)}개 / "
        f"일반 리포트 {len(generic_files)}개"
    )

    all_orders: list = []
    total_raw = 0
    failed_files: list[tuple[str, str]] = []

    # ---------- 1. PO(발주서) 일괄 파싱 ----------
    if po_files:
        log(f"발주서 {len(po_files)}개 파싱 시작...")
        po_orders = parse_po_files_to_orders(po_files)
        po_failed = po_orders.attrs.get("failed", []) if hasattr(po_orders, "attrs") else []
        po_blocked = po_orders.attrs.get("blocked", 0) if hasattr(po_orders, "attrs") else 0
        po_count = po_orders.attrs.get("po_count", 0) if hasattr(po_orders, "attrs") else 0
        if po_failed:
            failed_files.extend(po_failed)
        if po_blocked:
            log(f"  차단(오즈키즈 등): {po_blocked}행 제외")
        if not po_orders.empty:
            log(
                f"  발주서 {po_count}건 파싱 성공 → {len(po_orders)}행 "
                f"(매출 {int(po_orders['revenue'].sum()):,}원)"
            )
            all_orders.append(po_orders)
        else:
            log("  발주서 파싱 결과 0건")

    # ---------- 2. 일반 CSV/Excel 리포트 파싱 ----------
    for tf in generic_files:
        try:
            raw = read_coupang_sales_file(tf)
            total_raw += len(raw)
        except Exception as e:
            log(f"[{tf.name}] 파일 읽기 실패: {type(e).__name__}: {e}")
            failed_files.append((tf.name, f"읽기 실패: {e}"))
            continue

        try:
            orders_df = parse_to_orders(raw)
        except ValueError as e:
            log(f"[{tf.name}] 컬럼 매핑 실패: {e}")
            failed_files.append((tf.name, f"매핑 실패: {e}"))
            continue

        if orders_df.empty:
            log(f"[{tf.name}] 원본 {len(raw)}행 → 파싱 결과 0건")
            continue

        log(
            f"[{tf.name}] 원본 {len(raw)}행 → 파싱 {len(orders_df)}행 "
            f"(매출 {int(orders_df['revenue'].sum()):,}원)"
        )
        all_orders.append(orders_df)

    if not all_orders:
        log("모든 파일 파싱 결과 0건. 병합 스킵.")
        if failed_files:
            log(f"실패 파일 {len(failed_files)}개:")
            for name, err in failed_files[:10]:
                log(f"  - {name}: {err}")
        return 3

    merged_df = pd.concat(all_orders, ignore_index=True)

    # 동일 (date, store, product) 중복 제거 — 수량/매출 합산
    before_dedup = len(merged_df)
    merged_df = (
        merged_df.groupby(["date", "store", "product"], as_index=False)
        .agg(
            order_id=("order_id", "first"),
            customer_id=("customer_id", "first"),
            channel=("channel", "first"),
            quantity=("quantity", "sum"),
            revenue=("revenue", "sum"),
        )
    )
    merged_df = merged_df[[
        "date", "order_id", "customer_id", "channel", "store",
        "product", "quantity", "revenue",
    ]]
    dedup_merged = before_dedup - len(merged_df)
    if dedup_merged:
        log(f"중복 (date × store × product) {dedup_merged}행 합산 → {len(merged_df)}행")

    total_qty = int(merged_df["quantity"].sum())
    total_rev = int(merged_df["revenue"].sum())
    log(
        f"=== 전체 파싱 요약 ===\n"
        f"  파일: {len(target_files)}개 (실패 {len(failed_files)}개)\n"
        f"  원본: {total_raw:,}행 → 유효 {len(merged_df):,}행\n"
        f"  수량: {total_qty:,}개 / 매출: {total_rev:,}원"
    )

    for store in sorted(merged_df["store"].unique()):
        sdf = merged_df[merged_df["store"] == store]
        rev = int(sdf["revenue"].sum())
        qty = int(sdf["quantity"].sum())
        products_n = sdf["product"].nunique()
        log(f"  [{store}] {len(sdf)}행 / {products_n} 상품 / "
            f"수량 {qty:,}개 / 매출 {rev:,}원")

    try:
        kept, removed, added = merge_into_inbound_csv(merged_df)
        log(
            f"coupang_inbound.csv 누적 병합: 기존 {removed}행 교체 · "
            f"신규 {added}행 추가 · 과거 데이터 보존 {kept}행"
        )
        log(f"저장 위치: {INBOUND_FILE}")
    except Exception as e:
        log(f"병합 실패: {type(e).__name__}: {e}")
        return 4

    if failed_files:
        log(f"⚠ 실패 파일 {len(failed_files)}개 (수동 확인 필요):")
        for name, err in failed_files[:10]:
            log(f"  - {name}: {err}")

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
