"""Notion 회의록 CSV/ZIP 업로드 파서.

Notion DB 우상단 '...' → '내보내기' → 형식 'Markdown 및 CSV' 또는 'CSV'
선택 시 다운로드 받는 파일을 파싱.

지원 형식:
    A) CSV 단독 — DB properties (제목/팀/참석자/날짜 등) 만
    B) ZIP (Markdown & CSV) — CSV + 각 회의록의 .md 본문 포함

저장 위치:
    data/meetings.parquet   — 통합 회의록 리스트 (속성 + 본문 markdown)
"""
from __future__ import annotations

import io
import re
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).parent.parent
MEETINGS_FILE = ROOT / "data" / "meetings.parquet"
UPLOAD_DIR = ROOT / "data" / "meetings_upload"


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼명 정리 — 공백/특수문자 제거."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _parse_date(val) -> str:
    """다양한 형식 → ISO date string."""
    if pd.isna(val) or not val:
        return ""
    s = str(val).strip()
    for fmt in (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %I:%M %p",
        "%Y년 %m월 %d일 %H:%M",
        "%Y년 %m월 %d일 오전 %I:%M",
        "%Y년 %m월 %d일 오후 %I:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
    ):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    # pandas 마지막 시도
    try:
        return pd.to_datetime(s).isoformat()
    except Exception:
        return s


def parse_csv_file(path: Path) -> pd.DataFrame:
    """Notion DB CSV → DataFrame (속성만)."""
    df = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        df = pd.read_csv(path, encoding="utf-8", encoding_errors="replace")

    df = _normalize_columns(df)

    # 제목 컬럼 자동 탐지 (가장 흔한 이름들)
    title_candidates = ["이름", "제목", "Title", "Name"]
    title_col = next((c for c in title_candidates if c in df.columns), None)
    if title_col is None and len(df.columns) > 0:
        title_col = df.columns[0]   # 첫 컬럼

    # 날짜 컬럼 자동 탐지
    date_candidates = ["생성일자", "생성 일자", "날짜", "Date", "Created", "Created time"]
    date_col = next((c for c in date_candidates if c in df.columns), None)

    df["__title"] = df[title_col].astype(str).fillna("").str.strip()
    if date_col:
        df["__date"] = df[date_col].apply(_parse_date)
    else:
        df["__date"] = ""
    df["__content_md"] = ""   # CSV 만으론 본문 없음
    return df


def parse_markdown_files(md_paths: list[Path]) -> dict[str, str]:
    """각 .md 파일을 읽어 {파일명(확장자 제거): markdown 본문}.

    Notion export 시 파일명: '제목 페이지UUID.md' (UUID 32자리)
    파싱 결과 키 = 제목만 (UUID 제거)
    """
    result: dict[str, str] = {}
    uuid_re = re.compile(r"\s+[0-9a-f]{32}$", re.IGNORECASE)
    for p in md_paths:
        try:
            text = p.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = p.read_text(encoding="utf-8", errors="replace")
        stem = p.stem
        clean = uuid_re.sub("", stem).strip()
        result[clean] = text
    return result


def parse_zip_export(zip_path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """Notion 'Markdown 및 CSV' 내보내기 ZIP 파싱.

    반환: (CSV DataFrame, {제목: markdown 본문})
    """
    df_csv: pd.DataFrame | None = None
    md_map: dict[str, str] = {}

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            lower = name.lower()
            if lower.endswith(".csv") and df_csv is None:
                with zf.open(name) as f:
                    raw = f.read()
                # 인코딩 fallback
                for enc in ("utf-8-sig", "utf-8", "cp949"):
                    try:
                        df_csv = pd.read_csv(io.BytesIO(raw), encoding=enc)
                        break
                    except UnicodeDecodeError:
                        continue
                if df_csv is not None:
                    df_csv = _normalize_columns(df_csv)
            elif lower.endswith(".md"):
                with zf.open(name) as f:
                    text = f.read().decode("utf-8", errors="replace")
                stem = Path(name).stem
                clean = re.sub(r"\s+[0-9a-f]{32}$", "", stem, flags=re.IGNORECASE).strip()
                md_map[clean] = text

    if df_csv is None:
        df_csv = pd.DataFrame()
    return df_csv, md_map


def merge_csv_and_md(
    df_csv: pd.DataFrame, md_map: dict[str, str],
) -> pd.DataFrame:
    """CSV 속성 + markdown 본문 매핑."""
    df = _normalize_columns(df_csv).copy()

    # 제목 컬럼 자동 탐지
    title_candidates = ["이름", "제목", "Title", "Name"]
    title_col = next((c for c in title_candidates if c in df.columns), None)
    if title_col is None and len(df.columns) > 0:
        title_col = df.columns[0]

    date_candidates = ["생성일자", "생성 일자", "날짜", "Date", "Created", "Created time"]
    date_col = next((c for c in date_candidates if c in df.columns), None)

    df["__title"] = df[title_col].astype(str).fillna("").str.strip()
    if date_col:
        df["__date"] = df[date_col].apply(_parse_date)
    else:
        df["__date"] = ""
    df["__content_md"] = df["__title"].map(lambda t: md_map.get(t, ""))
    return df


def save_meetings(df: pd.DataFrame) -> None:
    """기존 데이터와 누적 병합 후 저장."""
    df = df.copy()
    if "__title" not in df.columns:
        df["__title"] = ""
    if "__date" not in df.columns:
        df["__date"] = ""
    if "__content_md" not in df.columns:
        df["__content_md"] = ""

    if MEETINGS_FILE.exists():
        try:
            existing = pd.read_parquet(MEETINGS_FILE)
            # 같은 제목은 새 데이터로 교체
            existing = existing[~existing["__title"].isin(df["__title"])]
            df = pd.concat([existing, df], ignore_index=True)
        except Exception:
            pass

    # 날짜순 정렬 (최신순)
    df = df.sort_values("__date", ascending=False, na_position="last").reset_index(drop=True)
    MEETINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(MEETINGS_FILE, index=False)


def load_meetings() -> pd.DataFrame:
    """저장된 회의록 로드."""
    if not MEETINGS_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(MEETINGS_FILE)
    except Exception:
        return pd.DataFrame()


def clear_meetings() -> None:
    """저장된 회의록 모두 삭제."""
    if MEETINGS_FILE.exists():
        MEETINGS_FILE.unlink()


def save_uploaded_file(uploaded, target_dir: Path | None = None) -> Path:
    """업로드된 파일을 디스크에 저장."""
    target_dir = target_dir or UPLOAD_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    name = uploaded.name
    target = target_dir / name
    if target.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = target_dir / f"{Path(name).stem}_{ts}{Path(name).suffix}"
    target.write_bytes(uploaded.getvalue())
    return target
