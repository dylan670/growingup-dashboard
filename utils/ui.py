"""대시보드 공용 UI 컴포넌트.

모든 페이지에서 import 해서 호출:
    from utils.ui import setup_page, render_page_header, render_sidebar_brand

브랜드 컬러 · 로고 · 헤더 · 전역 CSS 일관성 유지.
"""
from __future__ import annotations

import textwrap
from datetime import datetime

import streamlit as st


# ==========================================================
# 브랜드 컬러 토큰
# ==========================================================
BRAND_PRIMARY = "#2563eb"
BRAND_PRIMARY_LIGHT = "#dbeafe"
BRAND_ACCENT_ORANGE = "#f59e0b"
BRAND_ACCENT_GREEN = "#16a34a"
TEXT_MAIN = "#0f172a"
TEXT_MUTED = "#64748b"
BORDER_SUBTLE = "#e2e8f0"
BG_CARD = "#ffffff"
BG_PAGE_ALT = "#f8fafc"

_WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


# ==========================================================
# 전역 CSS
# ==========================================================
def inject_global_css() -> None:
    """브랜드 톤 전역 CSS. set_page_config 직후 호출."""
    st.markdown(
        f"""
        <style>
        /* ---------- 타이포그래피 ---------- */
        html, body, [class*="css"] {{
            -webkit-font-smoothing: antialiased;
        }}

        /* ---------- 메인 영역 여백 ---------- */
        .block-container {{
            padding-top: 1.5rem !important;
            padding-bottom: 3rem !important;
            max-width: 1400px;
        }}

        /* ---------- KPI 카드 (st.metric) ---------- */
        div[data-testid="stMetric"] {{
            background: {BG_CARD};
            border: 1px solid {BORDER_SUBTLE};
            padding: 18px 22px;
            border-radius: 14px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03);
            transition: box-shadow 0.15s ease;
        }}
        div[data-testid="stMetric"]:hover {{
            box-shadow: 0 2px 8px rgba(15, 23, 42, 0.06);
        }}
        div[data-testid="stMetric"] label {{
            font-size: 0.82rem;
            color: {TEXT_MUTED};
            font-weight: 500;
        }}
        div[data-testid="stMetricValue"] {{
            font-size: 1.75rem;
            font-weight: 700;
            color: {TEXT_MAIN};
            letter-spacing: -0.02em;
            line-height: 1.2;
        }}
        div[data-testid="stMetricDelta"] {{
            font-size: 0.8rem;
            font-weight: 500;
        }}

        /* ---------- 섹션 헤더 ---------- */
        h1 {{ font-weight: 700; letter-spacing: -0.03em; }}
        h2, h3, h4 {{ font-weight: 650; letter-spacing: -0.02em; }}
        h2 {{ padding-top: 0.4rem; }}

        /* ---------- 컨테이너 (border=True) ---------- */
        div[data-testid="stVerticalBlockBorderWrapper"] {{
            border-radius: 14px !important;
            border-color: {BORDER_SUBTLE} !important;
        }}

        /* ========== 사이드바 커스텀 네비게이션 ========== */
        /* Streamlit 기본 pages 자동 네비 숨김 */
        [data-testid="stSidebarNav"] {{
            display: none !important;
        }}

        section[data-testid="stSidebar"] {{
            background: {BG_PAGE_ALT};
            border-right: 1px solid {BORDER_SUBTLE};
        }}
        section[data-testid="stSidebar"] > div:first-child {{
            padding-top: 1.2rem;
        }}

        /* 섹션 라벨 (OVERVIEW / ANALYTICS / SYSTEM) */
        .nav-section-header {{
            font-size: 0.72rem;
            color: {TEXT_MUTED};
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin: 16px 0 6px 8px;
        }}

        /* st.page_link 기본 스타일 재정의 */
        section[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"] {{
            padding: 8px 12px !important;
            border-radius: 10px !important;
            margin: 2px 0 !important;
            transition: all 0.15s ease;
            font-weight: 500 !important;
            color: {TEXT_MAIN} !important;
            text-decoration: none !important;
        }}
        section[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"]:hover {{
            background: rgba(37, 99, 235, 0.06) !important;
        }}
        /* 현재 페이지 하이라이트 */
        section[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"][aria-current="page"] {{
            background: rgba(37, 99, 235, 0.1) !important;
            border: 1px solid rgba(37, 99, 235, 0.25) !important;
            color: {BRAND_PRIMARY} !important;
            font-weight: 600 !important;
        }}

        /* 사이드바 h3 (기타 섹션 라벨) */
        section[data-testid="stSidebar"] h3 {{
            font-size: 0.78rem;
            color: {TEXT_MUTED};
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-top: 1.2rem;
            margin-bottom: 0.4rem;
        }}

        /* ---------- 탭 ---------- */
        button[data-baseweb="tab"] {{
            font-size: 1rem !important;
            font-weight: 600 !important;
            padding: 10px 18px !important;
        }}
        button[data-baseweb="tab"][aria-selected="true"] {{
            color: {BRAND_PRIMARY} !important;
            border-bottom-color: {BRAND_PRIMARY} !important;
        }}

        /* ---------- 테이블 ---------- */
        div[data-testid="stDataFrame"] {{
            border-radius: 12px;
            overflow: hidden;
        }}

        /* ---------- Caption ---------- */
        div[data-testid="stCaptionContainer"] {{
            color: {TEXT_MUTED};
        }}

        /* ========== 📱 모바일 반응형 ========== */
        @media (max-width: 768px) {{
            /* 페이지 패딩 축소 */
            .block-container {{
                padding: 1rem 0.8rem 2rem 0.8rem !important;
            }}
            /* KPI 카드 여백 축소 */
            div[data-testid="stMetric"] {{
                padding: 14px 16px !important;
            }}
            div[data-testid="stMetricValue"] {{
                font-size: 1.4rem !important;
            }}
            /* 큰 커스텀 카드 숫자 축소 */
            .stApp div[style*="font-size:2.6rem"] {{
                font-size: 2rem !important;
            }}
            .stApp div[style*="font-size:1.9rem"] {{
                font-size: 1.5rem !important;
            }}
            .stApp div[style*="font-size:1.75rem"] {{
                font-size: 1.4rem !important;
            }}
            /* h1/h2 축소 */
            h1 {{ font-size: 1.6rem !important; }}
            h2 {{ font-size: 1.3rem !important; }}
            h3 {{ font-size: 1.1rem !important; }}
            /* 탭 라벨 작게 */
            button[data-baseweb="tab"] {{
                font-size: 0.85rem !important;
                padding: 8px 12px !important;
            }}
            /* 테이블 가로 스크롤 */
            div[data-testid="stDataFrame"] {{
                overflow-x: auto !important;
            }}
            /* 사이드바 기본 접힘 (모바일에서) — Streamlit 자동 처리 */
        }}

        /* 작은 화면 전용 (480px 이하) */
        @media (max-width: 480px) {{
            .block-container {{
                padding: 0.8rem 0.5rem 2rem 0.5rem !important;
            }}
            /* KPI 한 줄에 2개만 (Streamlit columns 강제 재배치는 한계 있음) */
            div[data-testid="stMetric"] {{
                padding: 10px 12px !important;
            }}
            div[data-testid="stMetricValue"] {{
                font-size: 1.2rem !important;
            }}
            /* 브랜드 진도 카드 축소 */
            .stApp div[style*="font-size:2.6rem"] {{
                font-size: 1.8rem !important;
            }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ==========================================================
# 사이드바 로고
# ==========================================================
def render_sidebar_brand() -> None:
    """사이드바 최상단 브랜드 로고 + 소속."""
    html = f"""
<div style="display:flex; gap:12px; align-items:center; padding:6px 2px 16px 2px; border-bottom: 1px solid {BORDER_SUBTLE}; margin-bottom: 8px;">
<div style="width:44px; height:44px; flex-shrink:0; border-radius:12px; background: linear-gradient(135deg, {BRAND_PRIMARY} 0%, #7c3aed 100%); display:flex; align-items:center; justify-content:center; color:white; font-size:1.3rem; font-weight:800; box-shadow: 0 2px 6px rgba(37,99,235,0.25);">G</div>
<div style="min-width:0;">
<div style="font-weight:700; font-size:0.95rem; color:{TEXT_MAIN}; line-height:1.2;">그로잉업팀 대시보드</div>
<div style="font-size:0.7rem; color:{TEXT_MUTED}; margin-top:3px;">OZKIZ · Marketing Analytics</div>
</div>
</div>
"""
    st.sidebar.markdown(_clean_html(html), unsafe_allow_html=True)


# ==========================================================
# 커스텀 사이드바 네비게이션 — OVERVIEW / ANALYTICS / SYSTEM
# ==========================================================
# 페이지 등록 정보: (섹션, 아이콘, 표시명, 파일 경로)
_NAV_PAGES: list[tuple[str, str, str, str]] = [
    ("OVERVIEW",  "🏠", "대시보드",       "app.py"),

    ("ANALYTICS", "💰", "매출 분석",      "pages/2_💰_매출_분석.py"),
    ("ANALYTICS", "📣", "광고 분석",      "pages/1_📣_광고_분석.py"),
    ("ANALYTICS", "📦", "제품 분석",      "pages/3_📦_제품_분석.py"),
    ("ANALYTICS", "👥", "CRM",            "pages/5_👥_CRM.py"),
    ("ANALYTICS", "🚦", "알림 센터",      "pages/4_🚦_알림_센터.py"),

    ("SYSTEM",    "⚙️", "설정",           "pages/6_⚙️_설정.py"),
    ("SYSTEM",    "🔌", "API 연결",       "pages/7_🔌_API_연결.py"),
]


def render_sidebar_nav() -> None:
    """사이드바 섹션형 네비게이션 (OVERVIEW / ANALYTICS / SYSTEM).

    Streamlit 기본 pages 자동 네비는 CSS 로 숨겨져 있음.
    """
    current_section: str | None = None
    for section, icon, label, path in _NAV_PAGES:
        if section != current_section:
            st.sidebar.markdown(
                f"<div class='nav-section-header'>{section}</div>",
                unsafe_allow_html=True,
            )
            current_section = section
        st.sidebar.page_link(path, label=label, icon=icon)


# ==========================================================
# 페이지 헤더 (제목 + 부제 + 오늘 날짜)
# ==========================================================
def render_page_header(
    title: str,
    subtitle: str | None = None,
    show_date: bool = True,
) -> None:
    """페이지 상단 헤더.

    좌측: 제목 + 부제
    우측: 오늘 날짜 + 프리컴퓨트 마지막 업데이트 시각
    """
    col_l, col_r = st.columns([3, 1])
    with col_l:
        st.markdown(
            f"<h2 style='margin:0; font-size:1.6rem; color:{TEXT_MAIN};'>{title}</h2>",
            unsafe_allow_html=True,
        )
        if subtitle:
            st.markdown(
                f"<div style='color:{TEXT_MUTED}; font-size:0.9rem; margin-top:4px;'>"
                f"{subtitle}</div>",
                unsafe_allow_html=True,
            )
    with col_r:
        if show_date:
            today = datetime.now()
            date_str = (
                f"{today.year}년 {today.month}월 {today.day}일 "
                f"{_WEEKDAY_KR[today.weekday()]}"
            )
            # 프리컴퓨트 마지막 업데이트 시각 (있으면)
            try:
                from utils.precomputed import get_last_updated
                last = get_last_updated()
                if last:
                    minutes_ago = int((today - last).total_seconds() / 60)
                    if minutes_ago < 60:
                        freshness = f"🔄 {minutes_ago}분 전"
                    elif minutes_ago < 1440:
                        freshness = f"🔄 {minutes_ago // 60}시간 전"
                    else:
                        freshness = f"🔄 {last.strftime('%m/%d %H:%M')}"
                else:
                    freshness = "⚠️ 프리컴퓨트 없음"
            except Exception:
                freshness = ""

            header_html = (
                f"<div style='text-align:right; padding-top:10px;'>"
                f"<div style='font-size:0.85rem; color:{TEXT_MUTED}; font-weight:500;'>"
                f"📅 {date_str}</div>"
            )
            if freshness:
                header_html += (
                    f"<div style='font-size:0.72rem; color:{TEXT_MUTED}; "
                    f"margin-top:2px; opacity:0.7;'>{freshness}</div>"
                )
            header_html += "</div>"
            st.markdown(header_html, unsafe_allow_html=True)
    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)


# ==========================================================
# 편의 통합 함수
# ==========================================================
def inject_dark_mode_css() -> None:
    """다크모드 CSS — session_state.dark_mode=True 일 때만 적용."""
    st.markdown(
        """
        <style>
        /* ========== 앱 전체 배경 ========== */
        .stApp, body, html {
            background: #0f172a !important;
            color: #f1f5f9 !important;
        }
        /* Streamlit 상단 헤더 바 (흰색 기본값 → 투명) */
        header[data-testid="stHeader"],
        .stApp > header {
            background: transparent !important;
        }
        /* Main container */
        div[data-testid="stMainBlockContainer"],
        section.main {
            background: #0f172a !important;
        }

        /* ========== 사이드바 ========== */
        section[data-testid="stSidebar"] {
            background: #1e293b !important;
            border-right-color: #334155 !important;
        }
        /* 사이드바 내부 모든 텍스트 기본값 밝게 — 로고 포함 */
        section[data-testid="stSidebar"] div[style*="color:#0f172a"],
        section[data-testid="stSidebar"] div[style*="color: #0f172a"] {
            color: #f1f5f9 !important;
        }
        section[data-testid="stSidebar"] div[style*="color:#64748b"],
        section[data-testid="stSidebar"] div[style*="color: #64748b"] {
            color: #94a3b8 !important;
        }
        /* 사이드바 네비 링크 */
        section[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"] {
            color: #cbd5e1 !important;
        }
        section[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"][aria-current="page"] {
            background: rgba(96, 165, 250, 0.15) !important;
            border-color: rgba(96, 165, 250, 0.35) !important;
            color: #60a5fa !important;
        }
        /* 사이드바 로고 섹션의 구분선 */
        section[data-testid="stSidebar"] div[style*="border-bottom: 1px solid #e2e8f0"] {
            border-bottom-color: #334155 !important;
        }
        .nav-section-header {
            color: #64748b !important;
        }
        /* 사이드바 selectbox/slider 등 */
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span {
            color: #e2e8f0 !important;
        }

        /* ========== 메인 텍스트 ========== */
        .stApp h1, .stApp h2, .stApp h3, .stApp h4,
        .stApp p, .stApp label {
            color: #f1f5f9 !important;
        }
        .stApp div[data-testid="stMarkdownContainer"] p,
        .stApp div[data-testid="stMarkdownContainer"] span:not([style*="color"]) {
            color: #f1f5f9 !important;
        }
        /* caption — 아예 별도 클래스 */
        div[data-testid="stCaptionContainer"],
        .stApp small {
            color: #94a3b8 !important;
        }
        /* 인라인 스타일 컬러 오버라이드 (메인 영역) */
        .stApp div[style*="color:#0f172a"],
        .stApp div[style*="color: #0f172a"] {
            color: #f1f5f9 !important;
        }
        .stApp div[style*="color:#64748b"],
        .stApp div[style*="color: #64748b"],
        .stApp div[style*="color:#475569"],
        .stApp div[style*="color: #475569"] {
            color: #94a3b8 !important;
        }

        /* ========== 카드 배경 ========== */
        .stApp div[style*="background:#ffffff"],
        .stApp div[style*="background: #ffffff"] {
            background: #1e293b !important;
            border-color: #334155 !important;
        }
        .stApp div[style*="background:#f8fafc"],
        .stApp div[style*="background: #f8fafc"] {
            background: #1e293b !important;
        }
        /* progress bar 배경 */
        .stApp div[style*="background:#f1f5f9"] {
            background: #334155 !important;
        }

        /* ========== st.metric ========== */
        div[data-testid="stMetric"] {
            background: #1e293b !important;
            border-color: #334155 !important;
        }
        div[data-testid="stMetric"] label {
            color: #94a3b8 !important;
        }
        div[data-testid="stMetricValue"] {
            color: #f1f5f9 !important;
        }

        /* ========== st.container(border=True) ========== */
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: #1e293b !important;
            border-color: #334155 !important;
        }

        /* ========== 탭 ========== */
        button[data-baseweb="tab"] {
            color: #cbd5e1 !important;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            color: #60a5fa !important;
            border-bottom-color: #60a5fa !important;
        }
        div[data-baseweb="tab-list"] {
            border-bottom-color: #334155 !important;
        }

        /* ========== 테이블 ========== */
        div[data-testid="stDataFrame"] {
            background: #1e293b !important;
        }
        div[data-testid="stDataFrame"] table {
            background: #1e293b !important;
            color: #f1f5f9 !important;
        }

        /* ========== 입력 필드 ========== */
        input, select, textarea,
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"],
        div[data-baseweb="base-input"] {
            background: #0f172a !important;
            color: #f1f5f9 !important;
            border-color: #334155 !important;
        }

        /* ========== 버튼 ========== */
        button[kind="primary"] {
            background: #3b82f6 !important;
        }
        button[kind="secondary"] {
            background: #334155 !important;
            color: #f1f5f9 !important;
            border-color: #475569 !important;
        }

        /* ========== Alert / Info 박스 ========== */
        div[data-testid="stAlertContainer"] {
            background: #1e293b !important;
        }

        /* ========== Plotly 차트 배경 투명 ========== */
        .js-plotly-plot .plotly .main-svg {
            background: transparent !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_theme_toggle() -> None:
    """사이드바 최하단 다크모드 토글."""
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False

    with st.sidebar:
        st.markdown(
            f"<div style='margin-top:24px; padding-top:16px; "
            f"border-top:1px solid {BORDER_SUBTLE};'></div>",
            unsafe_allow_html=True,
        )
        dark = st.toggle(
            "🌙 다크모드",
            value=st.session_state.dark_mode,
            key="dark_mode_toggle",
            help="야간/저조도 환경용 어두운 테마",
        )
        if dark != st.session_state.dark_mode:
            st.session_state.dark_mode = dark
            st.rerun()


def setup_page(
    page_title: str,
    page_icon: str = "📊",
    header_title: str | None = None,
    header_subtitle: str | None = None,
    layout: str = "wide",
) -> None:
    """페이지 기본 세팅을 한 번에."""
    # Streamlit Cloud 대응: st.secrets → os.environ
    try:
        from utils.env_bootstrap import bootstrap_env
        bootstrap_env()
    except Exception:
        pass

    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout=layout,
        initial_sidebar_state="expanded",
    )
    inject_global_css()
    # 다크모드 ON 이면 오버라이드 CSS
    if st.session_state.get("dark_mode", False):
        inject_dark_mode_css()
    render_sidebar_brand()
    render_sidebar_nav()
    if header_title:
        render_page_header(header_title, header_subtitle)
    render_theme_toggle()


# ==========================================================
# 브랜드 섹션 헤더 (스택 뷰용)
# ==========================================================
BRAND_BADGES = {
    "똑똑연구소": {
        "icon": "🍙",   # 김·떡뻥 유아식 → 주먹밥
        "bg_from": "#dbeafe",
        "bg_to": "#eff6ff",
        "border": "#2563eb",
        "text": "#1e40af",
    },
    "롤라루": {
        "icon": "🧳",   # 여행용 캐리어
        "bg_from": "#fef3c7",
        "bg_to": "#fffbeb",
        "border": "#f59e0b",
        "text": "#b45309",
    },
    "루티니스트": {
        "icon": "👟",   # 런닝용품
        "bg_from": "#dcfce7",
        "bg_to": "#f0fdf4",
        "border": "#16a34a",
        "text": "#15803d",
    },
}


def render_brand_banner(brand: str, subtitle: str = "") -> None:
    """브랜드별 그라데이션 섹션 헤더."""
    cfg = BRAND_BADGES.get(brand)
    if not cfg:
        st.markdown(f"### {brand}")
        return
    html = f"""
<div style="background: linear-gradient(90deg, {cfg['bg_from']}, {cfg['bg_to']}); padding: 14px 20px; border-radius: 14px; border-left: 4px solid {cfg['border']}; margin: 24px 0 14px 0;">
<h2 style="margin:0; color:{cfg['text']}; font-size:1.3rem; font-weight:700;">{cfg['icon']} {brand}</h2>
<p style="margin:4px 0 0; color:{TEXT_MUTED}; font-size:0.85rem;">{subtitle}</p>
</div>
"""
    st.markdown(_clean_html(html), unsafe_allow_html=True)


# ==========================================================
# 공용 카드 컴포넌트 (홈/매출/광고/제품 페이지 공통)
# ==========================================================
def format_won_compact(value: float) -> str:
    """₩1.2억 / ₩5,400만 / ₩1,234 간결 포맷."""
    v = int(value)
    if abs(v) >= 100_000_000:
        return f"₩{v / 100_000_000:.1f}억"
    if abs(v) >= 10_000:
        return f"₩{v / 10_000:,.0f}만"
    return f"₩{v:,}"


def _clean_html(html: str) -> str:
    """Markdown 이 HTML 을 코드 블록으로 오인하지 않도록 들여쓰기 + 줄바꿈 제거."""
    # textwrap.dedent 만으로는 부족 — 각 줄 앞 공백 완전 제거 후 한 줄로 합침
    lines = [ln.strip() for ln in html.strip().split("\n")]
    return "".join(lines)


def kpi_card(
    label: str,
    value: str,
    sub: str = "",
    value_color: str = TEXT_MAIN,
    icon: str = "",
) -> str:
    """상단 KPI 대형 카드 HTML (st.markdown 으로 출력)."""
    icon_html = f"<span style='margin-right:4px;'>{icon}</span>" if icon else ""
    html = f"""
<div style="background:{BG_CARD}; border:1px solid {BORDER_SUBTLE}; border-radius:14px; padding:18px 20px; box-shadow: 0 1px 3px rgba(15,23,42,0.04); height:100%;">
<div style="color:{TEXT_MUTED}; font-size:0.78rem; font-weight:600; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:10px;">{icon_html}{label}</div>
<div style="font-size:1.9rem; font-weight:800; color:{value_color}; line-height:1.1; letter-spacing:-0.03em;">{value}</div>
<div style="color:{TEXT_MUTED}; font-size:0.82rem; margin-top:6px; min-height:1.2em;">{sub}</div>
</div>
"""
    return _clean_html(html)


def icon_card(
    icon: str, label: str, main_value: str, sub_value: str = "",
    main_color: str = TEXT_MAIN, accent_color: str = "#e0f2fe",
) -> str:
    """오늘의 성과 하이라이트 스타일 카드 (아이콘 박스 포함)."""
    html = f"""
<div style="background:{BG_CARD}; border:1px solid {BORDER_SUBTLE}; border-radius:14px; padding:20px; box-shadow: 0 1px 3px rgba(15,23,42,0.04); height:100%;">
<div style="display:flex; align-items:center; gap:8px; margin-bottom:12px;">
<div style="background:{accent_color}; width:36px; height:36px; border-radius:10px; display:flex; align-items:center; justify-content:center; font-size:1.2rem;">{icon}</div>
<div style="color:{TEXT_MUTED}; font-size:0.8rem; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;">{label}</div>
</div>
<div style="font-size:1.5rem; font-weight:700; color:{main_color}; line-height:1.2; letter-spacing:-0.02em;">{main_value}</div>
<div style="color:{TEXT_MUTED}; font-size:0.85rem; margin-top:4px; min-height:1.2em;">{sub_value}</div>
</div>
"""
    return _clean_html(html)


def status_color(pct: float) -> tuple[str, str, str]:
    """달성률/효율 지표 → (컬러, 이모지, 상태 라벨)."""
    if pct >= 100:
        return "#16a34a", "✓", "목표 달성"
    if pct >= 85:
        return "#16a34a", "↗", "목표 근접"
    if pct >= 60:
        return "#f59e0b", "▲", "노력 필요"
    return "#dc2626", "▼", "미달 주의"


def status_badge(pct: float) -> str:
    """상태 배지 HTML (카드 우상단용)."""
    color, icon, label = status_color(pct)
    html = f"""
<div style="background:{color}15; color:{color}; padding:4px 10px; border-radius:20px; font-size:0.75rem; font-weight:600; display:inline-flex; align-items:center; gap:4px;">
<span>{icon}</span><span>{label}</span>
</div>
"""
    return _clean_html(html)
