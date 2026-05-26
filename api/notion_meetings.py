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

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


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
        resp = requests.post(url, headers=_headers(token), json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as e:
        body = e.response.text[:200] if e.response else ""
        raise RuntimeError(f"Notion API 오류 HTTP {e.response.status_code}: {body}")
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
            resp = requests.get(url, headers=_headers(token), params=params, timeout=20)
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
        body = e.response.text[:200] if e.response else ""
        return False, f"HTTP {e.response.status_code}: {body}"
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
