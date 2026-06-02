"""환경변수 Bootstrap — Streamlit Cloud 와 로컬 양쪽 지원.

로컬 개발:
    .env 파일 → python-dotenv 가 자동 로드

Streamlit Cloud 배포:
    앱 Settings → Secrets 에서 등록 → st.secrets 로 접근
    이 모듈이 st.secrets 값을 os.environ 으로 승격하여
    기존 os.getenv() 코드가 수정 없이 동작하게 함.

호출 위치: utils/ui.py 의 setup_page() 첫 줄에서 bootstrap_env()
"""
from __future__ import annotations

import os


def bootstrap_env() -> None:
    """st.secrets → os.environ 승격. 이미 환경변수 있으면 덮어쓰지 않음."""
    try:
        import streamlit as st
    except ImportError:
        return

    # st.secrets 의 모든 key/value 를 안전하게 순회
    try:
        # Streamlit 1.x 의 secrets 는 Mapping 인터페이스 지원
        items = []
        try:
            items = list(st.secrets.items())
        except Exception:
            # 일부 버전에서는 to_dict() 필요
            try:
                items = list(dict(st.secrets).items())
            except Exception:
                return

        for key, value in items:
            # nested section 은 dict-like — top-level primitive 만 승격
            if isinstance(value, (str, int, float, bool)):
                if key not in os.environ:
                    os.environ[key] = str(value)
    except Exception:
        # secrets 접근 자체 실패 (로컬에 파일 없음) → 조용히 스킵
        pass

    # ----------------------------------------------------------
    # [supabase_1688] 섹션 → SUPABASE_URL / SUPABASE_KEY 승격
    # (Cafe24 토큰 공유 저장소로 사용 — rotation 동기화)
    # ----------------------------------------------------------
    try:
        import streamlit as _st
        _sup = {}
        try:
            _sup = dict(_st.secrets.get("supabase_1688", {}))
        except Exception:
            _sup = {}
        _url = str(_sup.get("url", "")).rstrip("/")
        # service_role key 전용 — anon key 는 토큰 테이블 접근 금지(RLS).
        # service_key 없으면 SUPABASE_KEY 미설정 → Supabase 토큰 동기화 skip
        # → 기존 파일/secrets 방식으로 안전하게 fallback.
        _key = _sup.get("service_key") or _sup.get("service_role_key") or ""
        if _url and "SUPABASE_URL" not in os.environ:
            os.environ["SUPABASE_URL"] = _url
        if _key and "SUPABASE_KEY" not in os.environ:
            os.environ["SUPABASE_KEY"] = str(_key)
    except Exception:
        pass

    # ----------------------------------------------------------
    # Cafe24 토큰 파일 복원 (Streamlit Cloud — ephemeral fs)
    # ----------------------------------------------------------
    # CAFE24_TOKENS_JSON env 가 있으면 data/cafe24_tokens.json 으로 씀.
    # 답글 작성처럼 페이지에서 직접 client 로드할 때 토큰 필요.
    try:
        from pathlib import Path
        tokens_env = os.getenv("CAFE24_TOKENS_JSON", "").strip()
        if tokens_env:
            tf = Path(__file__).parent.parent / "data" / "cafe24_tokens.json"
            tf.parent.mkdir(parents=True, exist_ok=True)
            # 기존 파일이 없거나 빈 파일이면 환경변수에서 복원
            if not tf.exists() or tf.stat().st_size < 50:
                tf.write_text(tokens_env, encoding="utf-8")
    except Exception:
        pass


# ==========================================================
# 자동 실행 — import 즉시 env 승격
# 이렇게 하면 api/* 모듈들이 top-level 에서 os.getenv 호출해도
# 이미 값이 세팅돼 있음.
# ==========================================================
bootstrap_env()
