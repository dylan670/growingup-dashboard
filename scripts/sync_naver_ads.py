"""네이버 검색광고 API 자동 동기화 CLI 스크립트.

Windows 작업 스케줄러 등에서 매일 자동 실행하도록 설계된 스크립트입니다.

사용법:
    python scripts/sync_naver_ads.py --days 3
    python scripts/sync_naver_ads.py --since 2026-04-01 --until 2026-04-20

환경변수 (.env):
    NAVER_SEARCHAD_API_KEY
    NAVER_SEARCHAD_SECRET_KEY
    NAVER_SEARCHAD_CUSTOMER_ID

종료 코드:
    0 = 성공
    1 = API 키 누락
    2 = API 오류
    3 = 병합 실패
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# 프로젝트 루트를 import 경로에 추가
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.naver_searchad import load_client_from_env  # noqa: E402
from utils.data import merge_naver_brand_ads  # noqa: E402


LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="네이버 검색광고 데이터 자동 동기화")
    p.add_argument("--days", type=int, default=3,
                   help="전일 기준 며칠치를 동기화할지 (기본 3일, 일시 장애 대비 여유)")
    p.add_argument("--since", type=str, default=None, help="시작일 YYYY-MM-DD (--days 무시)")
    p.add_argument("--until", type=str, default=None, help="종료일 YYYY-MM-DD (기본: 어제)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # 클라이언트 로드
    client = load_client_from_env()
    if client is None:
        log("ERROR: API 자격증명 누락. .env 파일에 NAVER_SEARCHAD_API_KEY / SECRET_KEY / CUSTOMER_ID 설정 필요.")
        return 1

    # 기간 결정
    until = date.fromisoformat(args.until) if args.until else date.today() - timedelta(days=1)
    if args.since:
        since = date.fromisoformat(args.since)
    else:
        since = until - timedelta(days=args.days - 1)

    log(f"동기화 시작: {since} ~ {until} ({(until - since).days + 1}일치)")

    # 연결 테스트 (옵션)
    ok, msg = client.test_connection()
    if not ok:
        log(f"ERROR: 연결 실패 - {msg}")
        return 2
    log(f"연결 OK - {msg}")

    # 데이터 조회 (브랜드별 분리)
    try:
        df = client.get_daily_stats_by_brand_df(since, until)
    except Exception as e:
        log(f"ERROR: API 조회 실패 - {e}")
        return 2

    if df.empty:
        log("WARN: 조회 결과 없음. 기간이나 캠페인 상태 확인 필요.")
        return 0

    # 브랜드별 요약 로그
    for brand_store in sorted(df["store"].unique()):
        bdf = df[df["store"] == brand_store]
        spend = int(bdf["spend"].sum())
        rev = int(bdf["revenue"].sum())
        conv = int(bdf["conversions"].sum())
        roas = rev / spend * 100 if spend else 0
        log(
            f"  [{brand_store}] 광고비 {spend:,}원 / 매출 {rev:,}원 / "
            f"전환 {conv:,}건 / ROAS {roas:.0f}%"
        )

    total_spend = int(df["spend"].sum())
    total_rev = int(df["revenue"].sum())
    log(f"조회 완료: {len(df)}행 (일 x 브랜드) / 합계 광고비 {total_spend:,}원 / 매출 {total_rev:,}원")

    # 병합 (네이버 구 데이터 포함 덮어쓰기)
    try:
        removed, added = merge_naver_brand_ads(df)
        log(f"병합 완료: 기존 네이버 {removed}행 제거 -> 신규 {added}행 추가")
    except Exception as e:
        log(f"ERROR: 병합 실패 - {e}")
        return 3

    log("동기화 성공.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        try:
            log(f"UNCAUGHT EXCEPTION:\n{traceback.format_exc()}")
        except Exception:
            pass
        sys.exit(99)
