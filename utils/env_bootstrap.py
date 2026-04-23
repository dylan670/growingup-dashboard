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

    try:
        # st.secrets 접근 — Streamlit Cloud 나 로컬 secrets.toml 있을 때만 존재
        secrets = st.secrets
    except Exception:
        return

    try:
        for key in secrets:
            value = secrets[key]
            # 이미 .env 등으로 설정된 값은 보존
            if key not in os.environ and isinstance(value, (str, int, float)):
                os.environ[key] = str(value)
    except Exception:
        # secrets 접근 실패 (로컬에 파일 없음) → 조용히 스킵
        pass
