"""Notion API 클라이언트 — 회의록 DB 연동.

매주 새로 추가되는 그로잉업팀 회의록을 사이드바에서 자동 조회.

설정:
    1. https://www.notion.so/my-integrations 에서 integration 생성
    2. Internal Integration Secret 복사 → .env NOTION_TOKEN
    3. 회의록 DB 페이지의 '...' → Connections → 방금 생성한 integration 추가
    4. DB URL 의 ?v= 앞 32자리 hex → .env NOTION_MEETINGS_DB_ID

사용:
    from api.notion_meetings import load_meetings
    rows = load_meetings()   # 최근순 list[dict]
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# 로컬 디스크 캐시 — 빠른 페이지 로드 + 노션 장애 시 대비
_ROOT = Path(__file__).parent.parent
CACHE_DIR = _ROOT / "data" / "notion_cache"


def _cache_path(name: str) -> Path:
    """캐시 파일 경로. name 은 'databases', 'rows_<db_id>' 등."""
    safe = name.replace("/", "_").replace("\\", "_")
    return CACHE_DIR / f"{safe}.json"


def cache_save(name: str, data: Any) -> None:
    """JSON 직렬화 가능한 데이터를 디스크에 저장."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at": datetime.utcnow().isoformat() + "Z",
        "data": data,
    }
    _cache_path(name).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def cache_load(name: str) -> tuple[Any, str | None]:
    """(데이터, saved_at) — 캐시 없으면 (None, None)."""
    p = _cache_path(name)
    if not p.exists():
        return None, None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj.get("data"), obj.get("saved_at")
    except Exception:
        return None, None


def cache_age_seconds(name: str) -> float | None:
    """캐시 저장 후 경과 초. 캐시 없으면 None."""
    _, saved_at = cache_load(name)
    if not saved_at:
        return None
    try:
        dt = datetime.fromisoformat(saved_at.replace("Z", "+00:00"))
        return (datetime.utcnow().replace(tzinfo=dt.tzinfo) - dt).total_seconds()
    except Exception:
        return None


def cache_all_keys() -> list[str]:
    """저장된 모든 캐시 파일 이름 (확장자 제외)."""
    if not CACHE_DIR.exists():
        return []
    return sorted(p.stem for p in CACHE_DIR.glob("*.json"))


def _post_with_retry(
    url: str, headers: dict, json_payload: dict, timeout: int = 20,
    max_retries: int = 2,
):
    """5xx / 429 / timeout 에 대해 지수 backoff 재시도."""
    last_resp = None
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(
                url, headers=headers, json=json_payload, timeout=timeout,
            )
            last_resp = resp
            if resp.status_code < 500 and resp.status_code != 429:
                return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
        if attempt < max_retries:
            time.sleep(1.0 * (2 ** attempt))
    if last_resp is not None:
        return last_resp
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("재시도 실패")


def _get_with_retry(
    url: str, headers: dict, params: dict | None = None,
    timeout: int = 20, max_retries: int = 2,
):
    """5xx / 429 / timeout 에 대해 지수 backoff 재시도 (GET)."""
    last_resp = None
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(
                url, headers=headers, params=params, timeout=timeout,
            )
            last_resp = resp
            if resp.status_code < 500 and resp.status_code != 429:
                return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
        if attempt < max_retries:
            time.sleep(1.0 * (2 ** attempt))
    if last_resp is not None:
        return last_resp
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("재시도 실패")


def _get_creds() -> tuple[str | None, str | None]:
    """환경변수에서 Notion token + DB ID 로드."""
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
    try:
        # Streamlit Cloud secrets 도 지원
        from utils.env_bootstrap import bootstrap_env
        bootstrap_env()
    except Exception:
        pass

    token = os.getenv("NOTION_TOKEN", "").strip()
    db_id = os.getenv("NOTION_MEETINGS_DB_ID", "").strip()
    return (token or None), (db_id or None)


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _extract_title(prop: dict) -> str:
    """Notion title property → plain text."""
    parts = prop.get("title", []) or []
    return "".join(p.get("plain_text", "") for p in parts).strip()


def _extract_rich_text(prop: dict) -> str:
    parts = prop.get("rich_text", []) or []
    return "".join(p.get("plain_text", "") for p in parts).strip()


def _extract_people(prop: dict) -> list[str]:
    return [
        p.get("name", "") for p in (prop.get("people", []) or []) if p.get("name")
    ]


def _extract_select(prop: dict) -> str:
    s = prop.get("select")
    return s.get("name", "") if s else ""


def _extract_multi_select(prop: dict) -> list[str]:
    return [s.get("name", "") for s in (prop.get("multi_select", []) or [])]


def _extract_date(prop: dict) -> str:
    d = prop.get("date")
    return d.get("start", "") if d else ""


def _extract_date_full(prop: dict) -> dict:
    """캘린더용 — start/end 모두 반환."""
    d = prop.get("date")
    if not d:
        return {}
    return {
        "start": d.get("start", "") or "",
        "end": d.get("end", "") or "",
    }


def _extract_created_time(prop: dict) -> str:
    return prop.get("created_time", "")


def _parse_property(prop: dict) -> Any:
    """Notion property → Python value (type별 분기)."""
    t = prop.get("type")
    if t == "title":
        return _extract_title(prop)
    if t == "rich_text":
        return _extract_rich_text(prop)
    if t == "people":
        return _extract_people(prop)
    if t == "select":
        return _extract_select(prop)
    if t == "multi_select":
        return _extract_multi_select(prop)
    if t == "date":
        return _extract_date(prop)
    if t == "created_time":
        return _extract_created_time(prop)
    if t == "number":
        return prop.get("number")
    if t == "checkbox":
        return prop.get("checkbox", False)
    if t == "url":
        return prop.get("url", "")
    return ""


def load_meetings(max_count: int = 50) -> list[dict]:
    """회의록 DB 의 모든 항목 → 최신순 list.

    반환 dict 구조:
        {
            'id': 'page-uuid',
            'title': '5월 3주차 그로잉업팀 회의록',
            'created_at': '2026-05-21T08:46:00.000Z',
            'url': 'https://www.notion.so/...',
            'properties': {
                '팀': '그로잉업',
                '참석자': ['클레어', '제인', '딜런'],
                ...
            }
        }
    """
    token, db_id = _get_creds()
    if not token or not db_id:
        return []

    url = f"{NOTION_API_BASE}/databases/{db_id}/query"
    payload = {
        "page_size": min(max_count, 100),
        "sorts": [{
            "timestamp": "created_time",
            "direction": "descending",
        }],
    }
    try:
        resp = _post_with_retry(url, _headers(token), payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as e:
        body = e.response.text[:200] if e.response is not None else ""
        status = e.response.status_code if e.response is not None else "?"
        if status in (502, 503, 504):
            raise RuntimeError(
                f"Notion 서비스 일시 장애 (HTTP {status}) — 잠시 후 재시도해주세요."
            )
        raise RuntimeError(f"Notion API 오류 HTTP {status}: {body}")
    except Exception as e:
        raise RuntimeError(f"Notion 연결 실패: {type(e).__name__}: {e}")

    rows: list[dict] = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        # 제목 컬럼 자동 탐지 (type=title 인 첫 컬럼)
        title = ""
        parsed: dict[str, Any] = {}
        for key, val in props.items():
            if val.get("type") == "title":
                title = _extract_title(val)
            else:
                parsed[key] = _parse_property(val)
        rows.append({
            "id": page.get("id", ""),
            "title": title,
            "created_at": page.get("created_time", ""),
            "last_edited_at": page.get("last_edited_time", ""),
            "url": page.get("url", ""),
            "properties": parsed,
        })
    return rows


def load_page_content(page_id: str, max_blocks: int = 200) -> list[dict]:
    """페이지의 blocks (본문) 가져오기 — markdown-like dict 리스트.

    반환:
        [{'type': 'paragraph', 'text': '...'}, ...]
    """
    token, _ = _get_creds()
    if not token or not page_id:
        return []

    url = f"{NOTION_API_BASE}/blocks/{page_id}/children"
    blocks: list[dict] = []
    cursor: str | None = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        try:
            resp = _get_with_retry(url, _headers(token), params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            raise RuntimeError(f"Notion blocks 조회 실패: {type(e).__name__}: {e}")

        for b in data.get("results", []):
            btype = b.get("type", "")
            content = b.get(btype, {})
            text_parts = content.get("rich_text", []) or []
            text = "".join(p.get("plain_text", "") for p in text_parts)
            blocks.append({
                "type": btype,
                "text": text,
                "checked": content.get("checked", False) if btype == "to_do" else None,
            })
            if len(blocks) >= max_blocks:
                return blocks

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return blocks


def list_accessible_databases() -> list[dict]:
    """Integration 이 접근 가능한 모든 DB 검색.

    반환: [{'id', 'title', 'url'}, ...]
    """
    token, _ = _get_creds()
    if not token:
        return []

    url = f"{NOTION_API_BASE}/search"
    payload = {
        "filter": {"value": "database", "property": "object"},
        "page_size": 100,
    }
    try:
        resp = _post_with_retry(url, _headers(token), payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    rows: list[dict] = []
    for db in data.get("results", []):
        title_parts = db.get("title", []) or []
        title = "".join(p.get("plain_text", "") for p in title_parts) or "(제목 없음)"
        rows.append({
            "id": db.get("id", ""),
            "title": title,
            "url": db.get("url", ""),
            "properties_schema": db.get("properties", {}),
        })
    return rows


def query_database(
    db_id: str,
    max_count: int = 100,
    raise_on_error: bool = False,
    include_raw: bool = False,
) -> list[dict]:
    """특정 DB 의 모든 행 조회. 일반 DB(회의록/할일/지식/캘린더 등) 지원.

    반환: [{'id', 'title', 'url', 'created_at', 'properties': {...}}, ...]
    raise_on_error=True 면 HTTP 오류 시 RuntimeError 발생.
    include_raw=True 면 _raw_properties 에 노션 원본 property dict 보존.
    """
    token, _ = _get_creds()
    if not token:
        if raise_on_error:
            raise RuntimeError("NOTION_TOKEN 없음")
        return []

    # db_id 정규화 (URL 포함되면 hex 만 추출)
    import re as _re
    m = _re.search(r"([0-9a-f]{32})", db_id.replace("-", ""))
    if m:
        db_id_clean = m.group(1)
    else:
        db_id_clean = db_id.replace("-", "").strip()

    url = f"{NOTION_API_BASE}/databases/{db_id_clean}/query"
    rows: list[dict] = []
    cursor: str | None = None

    while True:
        payload: dict = {"page_size": min(max_count - len(rows), 100)}
        if cursor:
            payload["start_cursor"] = cursor
        try:
            resp = _post_with_retry(url, _headers(token), payload, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as e:
            body = e.response.text[:300] if e.response is not None else ""
            status = e.response.status_code if e.response is not None else "?"
            if raise_on_error:
                if status in (502, 503, 504):
                    raise RuntimeError(
                        f"Notion 서비스 일시 장애 (HTTP {status}) — 잠시 후 재시도"
                    )
                raise RuntimeError(f"HTTP {status}: {body}")
            break
        except (requests.Timeout, requests.ConnectionError) as e:
            if raise_on_error:
                raise RuntimeError(
                    f"Notion 서비스 일시 장애 (timeout/connection) — 잠시 후 재시도"
                )
            break
        except Exception as e:
            if raise_on_error:
                raise RuntimeError(f"{type(e).__name__}: {e}")
            break

        for page in data.get("results", []):
            props = page.get("properties", {})
            title = ""
            parsed: dict = {}
            for key, val in props.items():
                if val.get("type") == "title":
                    title = _extract_title(val)
                else:
                    parsed[key] = _parse_property(val)
            row = {
                "id": page.get("id", ""),
                "title": title,
                "created_at": page.get("created_time", ""),
                "last_edited_at": page.get("last_edited_time", ""),
                "url": page.get("url", ""),
                "properties": parsed,
            }
            if include_raw:
                row["_raw_properties"] = props
            rows.append(row)
            if len(rows) >= max_count:
                return rows

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return rows


def test_connection() -> tuple[bool, str]:
    """Notion API 연결 테스트."""
    token, db_id = _get_creds()
    if not token:
        return False, "NOTION_TOKEN 환경변수 없음"
    if not db_id:
        return False, "NOTION_MEETINGS_DB_ID 환경변수 없음"

    try:
        url = f"{NOTION_API_BASE}/databases/{db_id}"
        resp = requests.get(url, headers=_headers(token), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        title_parts = data.get("title", []) or []
        db_title = "".join(p.get("plain_text", "") for p in title_parts) or "(제목 없음)"
        return True, f"연결 성공 — DB '{db_title}'"
    except requests.HTTPError as e:
        body = e.response.text[:200] if e.response is not None else ""
        status = e.response.status_code if e.response is not None else "?"
        return False, f"HTTP {status}: {body}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def save_notion_credentials(token: str, db_id: str) -> None:
    """token + db_id 를 .env 에 저장 (페이지 설정용)."""
    env_path = Path(__file__).parent.parent / ".env"
    existing: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()
    existing["NOTION_TOKEN"] = token.strip()
    existing["NOTION_MEETINGS_DB_ID"] = db_id.strip()
    lines = [f"{k}={v}" for k, v in existing.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
