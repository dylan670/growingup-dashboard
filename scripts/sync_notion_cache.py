"""Notion DB → 로컬 디스크 캐시.

페이지 로드 시 노션 API 호출 없이 즉시 표시되도록 미리 계산.

저장 위치: data/notion_cache/
    - databases.json — 접근 가능 DB 목록 + 분류
    - rows_<db_id>.json — 각 DB 의 행 데이터 (raw properties 포함)

실행:
    python scripts/sync_notion_cache.py            # 전체 sync
    python scripts/sync_notion_cache.py --quiet    # 로그 최소화

자동 sync: sync_all.bat 가 호출.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.notion_meetings import (  # noqa: E402
    list_accessible_databases, query_database,
    cache_save, _get_creds,
)


def _classify(title: str) -> tuple[str, str] | None:
    """DB 제목 → (label, icon). 페이지 코드와 동일 로직."""
    t = title.lower().replace(" ", "")
    if "회의록" in title or "meeting" in t:
        return ("회의록", "📝")
    if ("할 일" in title or "할일" in title
            or "task" in t or "todo" in t or "to-do" in t):
        return ("할 일", "✅")
    if "지식" in title or "knowledge" in t:
        return ("지식", "📚")
    if "캘린더" in title or "calendar" in t or "일정" in title:
        return ("캘린더", "🗓")
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument(
        "--max-rows", type=int, default=300,
        help="DB 당 최대 행 수 (기본 300)",
    )
    args = ap.parse_args()

    def log(*a, **k):
        if not args.quiet:
            print(*a, **k)

    token, _ = _get_creds()
    if not token:
        print("❌ NOTION_TOKEN 없음 — .env 확인")
        return 1

    t0 = time.time()
    log(f"[{time.strftime('%H:%M:%S')}] Notion DB sync 시작...")

    # 1) DB 목록
    dbs = list_accessible_databases()
    if not dbs:
        print("❌ 접근 가능 DB 없음")
        return 1
    log(f"  DB 목록: {len(dbs)}개 발견")

    # 2) 분류 — 그로잉업팀 관련만 (중복 시 '그로잉업' 키워드 우선)
    classified: list[dict] = []
    seen: dict[str, str] = {}   # label → db_id
    for db in dbs:
        result = _classify(db["title"])
        if result is None:
            continue
        label, icon = result
        is_growingup = "그로잉업" in db["title"]
        if label in seen and not is_growingup:
            continue
        classified = [c for c in classified if c["label"] != label]
        classified.append({**db, "label": label, "icon": icon})
        seen[label] = db["id"]

    # 우선순위 정렬
    order = {"회의록": 0, "할 일": 1, "캘린더": 2, "지식": 3}
    classified.sort(key=lambda x: order.get(x["label"], 99))

    # 저장 — properties_schema 같은 큰 dict 도 함께 (어차피 메타)
    cache_save("databases", classified)
    log(f"  databases.json 저장 — {len(classified)}개 DB")

    # 3) 각 DB rows
    total_rows = 0
    failed: list[tuple[str, str]] = []
    for c in classified:
        db_id = c["id"]
        title = c["title"]
        try:
            rows = query_database(
                db_id, max_count=args.max_rows,
                raise_on_error=True, include_raw=True,
            )
            cache_save(f"rows_{db_id.replace('-', '')}", rows)
            log(f"  ✓ {c['icon']} {title}: {len(rows)}건")
            total_rows += len(rows)
        except Exception as e:
            msg = str(e)[:120]
            failed.append((title, msg))
            log(f"  ✗ {c['icon']} {title}: {msg}")

    elapsed = time.time() - t0
    log(f"\n완료 — {len(classified)}개 DB · 총 {total_rows}건 · {elapsed:.1f}초")
    if failed:
        log(f"\n실패 {len(failed)}건:")
        for title, msg in failed:
            log(f"  - {title}: {msg}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
