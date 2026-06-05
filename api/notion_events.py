"""무신사 캘린더 (채널별 행사/프로모션) Notion 연동.

회의록(notion_meetings)과 동일한 Integration · REST 패턴 재사용.
DB: 🕶️ 무신사 캘린더 (그로잉업팀 > 롤라루 캘린더)
스키마: 이름(title) · 날짜(date) · 브랜드(select) · 판매처(select)

⚠️ 대시보드 Notion Integration 이 이 DB(또는 상위 페이지)에
   연결돼 있어야 동작 (미연결 시 404 → 빈 리스트 반환).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from api.notion_meetings import (
    NOTION_API_BASE,
    _get_creds,
    _headers,
    _post_with_retry,
    _extract_title,
    _extract_select,
    _extract_date_full,
    cache_save,
    cache_load,
)

# 무신사 캘린더 inline DB id (행사 데이터)
EVENTS_DB_ID = "342c081d6cc980e09a06ed3daabcc7e3"
_CACHE_KEY = "rows_events_musinsa"


def load_events(max_count: int = 300, use_cache: bool = True) -> list[dict]:
    """행사 목록 로드.

    반환: [{name, date_start, date_end, brand, channel}], 날짜 오름차순.
    Integration 미연결/실패 시 캐시 → 빈 리스트 순으로 fallback.
    """
    token, _ = _get_creds()
    if not token:
        cached, _ = cache_load(_CACHE_KEY) if use_cache else (None, None)
        return cached or []

    url = f"{NOTION_API_BASE}/databases/{EVENTS_DB_ID}/query"
    rows: list[dict] = []
    cursor: str | None = None

    try:
        while len(rows) < max_count:
            payload: dict[str, Any] = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor
            resp = _post_with_retry(url, _headers(token), payload, timeout=20)
            if resp is None or not resp.ok:
                break
            data = resp.json()
            for r in data.get("results", []):
                p = r.get("properties", {})
                date_obj = _extract_date_full(p.get("날짜", {}))
                rows.append({
                    "name": _extract_title(p.get("이름", {})),
                    "date_start": (date_obj or {}).get("start", ""),
                    "date_end": (date_obj or {}).get("end", "") or "",
                    "brand": _extract_select(p.get("브랜드", {})),
                    "channel": _extract_select(p.get("판매처", {})),
                })
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
    except Exception:
        # 실패 시 캐시 fallback
        cached, _ = cache_load(_CACHE_KEY) if use_cache else (None, None)
        return cached or []

    # 날짜 오름차순 (start 없는 건 뒤로)
    rows.sort(key=lambda x: x.get("date_start") or "9999")

    if rows:
        try:
            cache_save(_CACHE_KEY, rows)
        except Exception:
            pass
    elif use_cache:
        cached, _ = cache_load(_CACHE_KEY)
        if cached:
            return cached
    return rows
