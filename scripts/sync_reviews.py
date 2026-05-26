"""네이버 + 자사몰 리뷰 자동 동기화 CLI.

각 채널의 공식 API 로 리뷰를 수집해서 data/reviews.csv 에 병합.

지원 채널:
    - 네이버 스마트스토어 (NaverCommerceClient.fetch_reviews_df)
        엔드포인트: POST /external/v1/product-reviews/search
        ※ 커머스 API 콘솔에서 '상품 리뷰' 권한 활성화 필요할 수 있음
    - 자사몰 카페24 (Cafe24Client.fetch_reviews_df)
        엔드포인트: GET /api/v2/admin/reviews (또는 게시판 articles 폴백)
        ※ OAuth scope 'mall.read_community' 필요

사용법:
    python scripts/sync_reviews.py                    # 최근 7일
    python scripts/sync_reviews.py --days 30
    python scripts/sync_reviews.py --since 2026-01-01 --until 2026-05-25
    python scripts/sync_reviews.py --only-naver       # 네이버만
    python scripts/sync_reviews.py --only-cafe24      # 자사몰만

환경변수: 기존 sync_naver_commerce.py / sync_cafe24.py 와 동일.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Cafe24 토큰 환경변수 → 파일 복원 (Streamlit Cloud / GitHub Actions 대응)
import os  # noqa: E402
_tokens_env = os.getenv("CAFE24_TOKENS_JSON", "").strip()
if _tokens_env:
    _tf = ROOT / "data" / "cafe24_tokens.json"
    _tf.parent.mkdir(exist_ok=True)
    if not _tf.exists():
        _tf.write_text(_tokens_env, encoding="utf-8")

from api.naver_commerce import load_commerce_clients_from_env  # noqa: E402
from api.cafe24 import load_all_cafe24_clients  # noqa: E402
from utils.data import merge_reviews  # noqa: E402


LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [reviews] {msg}"
    # Windows cp949 콘솔에서 이모지/유니코드 깨짐 방지
    try:
        print(line)
    except UnicodeEncodeError:
        import sys as _sys
        enc = (_sys.stdout.encoding or "utf-8").lower()
        print(line.encode(enc, errors="replace").decode(enc, errors="replace"))
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="네이버 + 자사몰 리뷰 동기화")
    p.add_argument("--days", type=int, default=7,
                   help="전일 기준 며칠치 동기화 (기본 7일)")
    p.add_argument("--since", type=str, default=None)
    p.add_argument("--until", type=str, default=None)
    p.add_argument("--only-naver", action="store_true",
                   help="네이버만 동기화")
    p.add_argument("--only-cafe24", action="store_true",
                   help="자사몰만 동기화")
    return p.parse_args()


def _store_to_brand(store: str) -> str:
    s = str(store).replace(" ", "")
    if "똑똑" in s:
        return "똑똑연구소"
    if "롤라루" in s:
        return "롤라루"
    if "루티니" in s:
        return "루티니스트"
    return "기타"


def sync_naver(since: date, until: date) -> tuple[int, int, int]:
    """네이버 스마트스토어 리뷰 sync. 반환: (총 수집, 신규 추가, 실패 스토어 수)."""
    clients = load_commerce_clients_from_env()
    if not clients:
        log("네이버: 자격증명 없음 — 스킵")
        return 0, 0, 0

    total, total_added, fail = 0, 0, 0
    for store, client in clients.items():
        ok, msg = client.test_connection()
        if not ok:
            log(f"네이버 [{store}] 인증 실패: {msg}")
            fail += 1
            continue
        try:
            df = client.fetch_reviews_df(since, until, store)
        except Exception as e:
            log(f"네이버 [{store}] 조회 실패: {type(e).__name__}: {e}")
            fail += 1
            continue

        if df.empty:
            log(f"네이버 [{store}] 0건")
            continue

        total += len(df)
        brand = _store_to_brand(store)
        removed, added = merge_reviews(df, channel="네이버", brand=brand)
        total_added += added
        log(f"네이버 [{store}/{brand}] {len(df)}건 수집 · 기존 {removed}건 교체")
    return total, total_added, fail


def sync_cafe24(since: date, until: date) -> tuple[int, int, int]:
    """자사몰 카페24 리뷰 sync."""
    clients = load_all_cafe24_clients()
    if not clients:
        log("자사몰: 자격증명 없음 — 스킵")
        return 0, 0, 0

    total, total_added, fail = 0, 0, 0
    for store, client in clients.items():
        ok, msg = client.test_connection()
        if not ok:
            log(f"자사몰 [{store}] 인증 실패: {msg}")
            fail += 1
            continue
        try:
            df = client.fetch_reviews_df(since, until, store)
        except Exception as e:
            log(f"자사몰 [{store}] 조회 실패: {type(e).__name__}: {e}")
            fail += 1
            continue

        if df.empty:
            log(f"자사몰 [{store}] 0건")
            continue

        total += len(df)
        brand = _store_to_brand(store)
        removed, added = merge_reviews(df, channel="자사몰", brand=brand)
        total_added += added
        log(f"자사몰 [{store}/{brand}] {len(df)}건 수집 · 기존 {removed}건 교체")
    return total, total_added, fail


def main() -> int:
    args = parse_args()
    until = date.fromisoformat(args.until) if args.until else date.today() - timedelta(days=1)
    since = date.fromisoformat(args.since) if args.since else until - timedelta(days=args.days - 1)
    log(f"리뷰 동기화 시작: {since} ~ {until}")

    do_naver = not args.only_cafe24
    do_cafe24 = not args.only_naver

    grand_total = 0
    grand_added = 0
    total_fail = 0

    if do_naver:
        t, a, f = sync_naver(since, until)
        grand_total += t
        grand_added += a
        total_fail += f

    if do_cafe24:
        t, a, f = sync_cafe24(since, until)
        grand_total += t
        grand_added += a
        total_fail += f

    log(f"완료 — 총 수집 {grand_total}건 · 신규 추가 {grand_added}건 · 실패 {total_fail}건")

    if grand_total == 0:
        log("[!] 0건 수집 — 권한/스코프/날짜 범위 확인 필요. "
            "데모 데이터는 data/reviews.csv 에 보존됨.")
        return 4 if total_fail > 0 else 0

    # 실데이터 수집 성공 → meta marker 를 'live' 로 전환
    import json as _json
    meta = {
        "source": "live",
        "last_sync_at": datetime.now().isoformat(),
        "last_sync_total": grand_total,
        "last_sync_added": grand_added,
        "last_sync_failed": total_fail,
    }
    (ROOT / "data" / "reviews_meta.json").write_text(
        _json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    log(f"[OK] reviews_meta.json → source='live'")

    return 0 if total_fail == 0 else 2


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
