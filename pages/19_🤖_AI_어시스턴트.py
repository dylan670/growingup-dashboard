"""AI 어시스턴트 — SOON (개발 예정)."""
from __future__ import annotations

import streamlit as st

from utils.ui import setup_page


setup_page(
    page_title="AI 어시스턴트",
    page_icon="🤖",
    header_title="🤖 AI 어시스턴트",
    header_subtitle="데이터 기반 의사결정 · 인사이트 자동 도출",
)


st.markdown(
    """
<div style="background: linear-gradient(135deg, #ede9fe 0%, #ddd6fe 100%);
            border-left: 5px solid #7c3aed;
            border-radius: 14px; padding: 24px 28px; margin: 24px 0;">
    <div style="font-size:0.8rem; font-weight:700; color:#6b21a8;
                letter-spacing:0.08em; text-transform:uppercase;">
        SOON · 개발 예정
    </div>
    <div style="font-size:1.4rem; font-weight:700; color:#581c87;
                margin-top:8px;">
        🤖 AI 어시스턴트
    </div>
    <div style="font-size:0.92rem; color:#6b21a8; margin-top:10px;
                line-height:1.6;">
        자연어로 데이터 질문 · 자동 인사이트 도출 · 의사결정 보조.<br><br>
        <b>계획 기능</b>:
        <ul style="margin-top:6px; padding-left:20px;">
            <li>자연어 질문 ("지난주 롤라루 매출 어땠어?")</li>
            <li>이상 패턴 자동 감지 (매출 dip / 광고 ROAS 하락)</li>
            <li>SKU 확장 후보 자동 추천</li>
            <li>회의록 자동 요약 / 액션 아이템 추출</li>
        </ul>
    </div>
</div>
    """,
    unsafe_allow_html=True,
)
