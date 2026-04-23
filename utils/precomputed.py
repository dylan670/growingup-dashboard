"""프리컴퓨트 캐시 로더 — 대시보드 빠른 로드용.

매일 10시 sync 직후 `scripts/precompute.py` 가 계산한 결과를
Parquet/JSON 으로 저장 → 대시보드는 이걸 바로 읽기만 함.

로드 우선순위:
    1. data/precomputed/*.parquet (있으면 즉시 반환)
    2. live 계산 (fallback)
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd


ROOT = Path(__file__).parent.parent
PRECOMP_DIR = ROOT / "data" / "precomputed"


def _file_mtime(path: Path) -> datetime | None:
    """파일 수정 시각 → datetime. 없으면 None."""
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime)


def load_precomputed_parquet(
    filename: str,
    fallback: Callable[[], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Parquet 우선 로드 → 없으면 fallback 호출."""
    path = PRECOMP_DIR / filename
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            pass
    if fallback is not None:
        return fallback()
    return pd.DataFrame()


def load_precomputed_json(
    filename: str,
    fallback: Callable[[], dict] | None = None,
) -> dict:
    """JSON 우선 로드 → 없으면 fallback."""
    path = PRECOMP_DIR / filename
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    if fallback is not None:
        return fallback()
    return {}


def get_last_updated() -> datetime | None:
    """프리컴퓨트 마지막 업데이트 시각."""
    path = PRECOMP_DIR / "last_updated.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return datetime.fromisoformat(data["updated_at"])
    except Exception:
        return _file_mtime(path)


def save_precomputed_parquet(df: pd.DataFrame, filename: str) -> None:
    """Parquet 저장 (precompute 스크립트 전용)."""
    PRECOMP_DIR.mkdir(parents=True, exist_ok=True)
    path = PRECOMP_DIR / filename
    df.to_parquet(path, index=False)


def save_precomputed_json(data: Any, filename: str) -> None:
    """JSON 저장 (precompute 스크립트 전용)."""
    PRECOMP_DIR.mkdir(parents=True, exist_ok=True)
    path = PRECOMP_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def mark_last_updated() -> None:
    """precompute 완료 마커."""
    save_precomputed_json(
        {"updated_at": datetime.now().isoformat()},
        "last_updated.json",
    )
