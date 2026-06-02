"""행사관리 — SOON (개발 예정)."""
from __future__ import annotations

import streamlit as st

from utils.ui import setup_page


setup_page(
    page_title="행사관리",
    page_icon="🎉",
    header_title="🎉 행사관리",
    header_subtitle="프로모션 · 세일 · 쿠폰 행사 기획 및 일정 관리",
)


st.markdown(
    """
<div style="background: linear-gradient(135deg, #fce7f3 0%, #fbcfe8 100%);
            border-left: 5px solid #ec4899;
            border-radius: 14px; padding: 24px 28px; margin: 24px 0;">
    <div style="font-size:0.8rem; font-weight:700; color:#be185d;
                letter-spacing:0.08em; text-transform:uppercase;">
        SOON · 개발 예정
    </div>
    <div style="font-size:1.4rem; font-weight:700; color:#831843;
                margin-top:8px;">
        🎉 행사관리
    </div>
    <div style="font-size:0.92rem; color:#9d174d; margin-top:10px;
                line-height:1.6;">
        프로모션 · 세일 · 쿠폰 등 행사 기획에서 효과 측정까지
        통합 관리하는 페이지입니다.<br><br>
        <b>계획 기능</b>:
        <ul style="margin-top:6px; padding-left:20px;">
            <li>행사 캘린더 (진행중 / 예정 / 종료)</li>
            <li>행사별 매출 영향 측정 (기간 전후 비교)</li>
            <li>채널별 행사 일정 (자사몰 / 네이버 / 쿠팡)</li>
            <li>쿠폰 코드 발급 / 사용량 추적</li>
        </ul>
    </div>
</div>
    """,
    unsafe_allow_html=True,
)
