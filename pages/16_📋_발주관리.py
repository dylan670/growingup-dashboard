"""발주관리 — SOON (개발 예정)."""
from __future__ import annotations

import streamlit as st

from utils.ui import setup_page


setup_page(
    page_title="발주관리",
    page_icon="📋",
    header_title="📋 발주관리",
    header_subtitle="채널별 발주 요청 · 진행 상태 · 확정 일정 관리",
)


st.markdown(
    """
<div style="background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
            border-left: 5px solid #f59e0b;
            border-radius: 14px; padding: 24px 28px; margin: 24px 0;">
    <div style="font-size:0.8rem; font-weight:700; color:#b45309;
                letter-spacing:0.08em; text-transform:uppercase;">
        SOON · 개발 예정
    </div>
    <div style="font-size:1.4rem; font-weight:700; color:#78350f;
                margin-top:8px;">
        📋 발주관리
    </div>
    <div style="font-size:0.92rem; color:#92400e; margin-top:10px;
                line-height:1.6;">
        본업 거래처 / 공급사에 보내는 <b>발주서 작성·전송·상태 추적</b>을
        한 화면에서 관리할 수 있는 페이지입니다.<br><br>
        <b>계획 기능</b>:
        <ul style="margin-top:6px; padding-left:20px;">
            <li>발주 요청 작성 (브랜드별 / SKU별)</li>
            <li>발주 진행 상태 추적 (요청→생산→입고)</li>
            <li>거래처 응답 / 확정 일정 관리</li>
            <li>이지어드민 입고대기 자동 연동</li>
        </ul>
    </div>
</div>
    """,
    unsafe_allow_html=True,
)
