"""상품 이미지 캐시 갱신 CLI.

각 채널 상품 API 로 이름 ↔ 대표이미지 URL 매핑을 수집해
data/product_images.csv 에 저장. 대시보드(랭킹·제품 분석)가
주문 제품명으로 fuzzy 매칭해서 썸네일 표시.

수집 채널:
    - Cafe24 자사몰 (3개 매장) — get_product_images
    - 네이버 스마트스토어 — representativeImage
    - 쿠팡 윙 — 상품 이미지

매일 sync_all.bat 에서 자동 호출. 일부 채널 실패해도 나머지는 진행.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Cafe24 토큰 환경변수 → 파일 복원 (Streamlit Cloud / GitHub Actions 대응)
_tokens_env = os.getenv("CAFE24_TOKENS_JSON", "").strip()
if _tokens_env:
    _tf = ROOT / "data" / "cafe24_tokens.json"
    _tf.parent.mkdir(exist_ok=True)
    if not _tf.exists():
        _tf.write_text(_tokens_env, encoding="utf-8")

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except Exception:
    pass

LOG_FILE = ROOT / "data" / "sync_log.txt"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [images] {msg}"
    try:
        print(line)
    except UnicodeEncodeError:
        enc = (sys.stdout.encoding or "utf-8").lower()
        print(line.encode(enc, errors="replace").decode(enc, errors="replace"))
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> int:
    from utils.product_images import (
        refresh_cafe24_image_cache,
        refresh_naver_image_cache,
        refresh_coupang_image_cache,
    )

    log("상품 이미지 캐시 갱신 시작")
    total = 0

    # 1. Cafe24 자사몰 (3개 매장) — 가장 정확한 자사 상품 이미지
    try:
        n = refresh_cafe24_image_cache()
        total += n
        log(f"Cafe24 자사몰: {n}개")
    except Exception as e:
        log(f"Cafe24 실패: {type(e).__name__}: {e}")

    # 2. 네이버 스마트스토어
    try:
        n = refresh_naver_image_cache()
        total += n
        log(f"네이버: {n}개")
    except Exception as e:
        log(f"네이버 실패: {type(e).__name__}: {e}")

    # 3. 쿠팡 윙
    try:
        n = refresh_coupang_image_cache()
        total += n
        log(f"쿠팡: {n}개")
    except Exception as e:
        log(f"쿠팡 실패: {type(e).__name__}: {e}")

    log(f"완료 — 총 {total}개 이미지 매핑")
    return 0


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
