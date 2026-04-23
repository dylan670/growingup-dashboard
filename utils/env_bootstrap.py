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


# ==========================================================
# 자동 실행 — import 즉시 env 승격
# 이렇게 하면 api/* 모듈들이 top-level 에서 os.getenv 호출해도
# 이미 값이 세팅돼 있음.
# ==========================================================
bootstrap_env()
