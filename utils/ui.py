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
# 컬러 디자인 토큰 — 전 페이지 일관 적용
# ==========================================================
BRAND_PRIMARY = "#2563eb"
BRAND_PRIMARY_LIGHT = "#dbeafe"
BRAND_ACCENT_ORANGE = "#f59e0b"
BRAND_ACCENT_GREEN = "#16a34a"
TEXT_MAIN = "#0f172a"
TEXT_MUTED = "#64748b"
TEXT_FAINT = "#94a3b8"
BORDER_SUBTLE = "#e2e8f0"
BORDER_MEDIUM = "#cbd5e1"
BG_CARD = "#ffffff"
BG_PAGE_ALT = "#f8fafc"
BG_SUBTLE = "#f1f5f9"

# 브랜드 고정 컬러 — 모든 차트·카드·배지 전역 일관
BRAND_COLORS: dict[str, dict[str, str]] = {
    "똑똑연구소":  {"primary": "#2563eb", "bg": "#dbeafe", "bg_soft": "#eff6ff",
                    "text": "#1e40af", "accent": "#3b82f6"},
    "롤라루":      {"primary": "#f59e0b", "bg": "#fef3c7", "bg_soft": "#fffbeb",
                    "text": "#b45309", "accent": "#fb923c"},
    "루티니스트":  {"primary": "#16a34a", "bg": "#dcfce7", "bg_soft": "#f0fdf4",
                    "text": "#15803d", "accent": "#22c55e"},
}

# 상태 컬러 (광고/매출 평가)
STATUS_COLORS = {
    "critical":    {"fg": "#dc2626", "bg": "#fee2e2", "border": "#fecaca"},
    "warning":     {"fg": "#ea580c", "bg": "#fff7ed", "border": "#fed7aa"},
    "caution":     {"fg": "#d97706", "bg": "#fef3c7", "border": "#fde68a"},
    "success":     {"fg": "#16a34a", "bg": "#dcfce7", "border": "#bbf7d0"},
    "opportunity": {"fg": "#7c3aed", "bg": "#ede9fe", "border": "#ddd6fe"},
    "info":        {"fg": "#2563eb", "bg": "#dbeafe", "border": "#bfdbfe"},
    "neutral":     {"fg": "#64748b", "bg": "#f1f5f9", "border": "#e2e8f0"},
}

# 채널별 고정 컬러 (차트용)
CHANNEL_COLORS: dict[str, str] = {
    "자사몰": "#2563eb",       # 파랑 (브랜드 메인)
    "네이버":  "#22c55e",      # 초록
    "네이버 스마트스토어": "#22c55e",
    "쿠팡":    "#f97316",      # 주황
    "메타":    "#8b5cf6",      # 보라
    "무신사":  "#0ea5e9",      # 하늘
    "이지웰":  "#14b8a6",      # 청록
    "오늘의집": "#ec4899",     # 핑크
    "오프라인": "#64748b",     # 회색
}

# 지표별 컬러 (차트 · 통일)
METRIC_COLORS = {
    "revenue":   "#2563eb",    # 매출 — 파랑
    "target":    "#dc2626",    # 목표 — 빨강 (주로 점선)
    "orders":    "#16a34a",    # 주문 — 초록
    "customers": "#8b5cf6",    # 고객 — 보라
    "spend":     "#f97316",    # 광고비 — 주황
    "roas":      "#dc2626",    # ROAS — 빨강 (경고·효율)
    "impressions": "#94a3b8",  # 노출 — 회색
    "clicks":    "#0ea5e9",    # 클릭 — 하늘
}

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
            /* Streamlit 상단 툴바(Share/Star/GitHub/⋮ 버튼)가 sticky 로
               1번째 요소를 덮는 문제 → 상단 여유 대폭 확대 */
            padding-top: 3.5rem !important;
            padding-bottom: 3rem !important;
            max-width: 1400px;
        }}
        /* 상단 Streamlit 기본 header 영역 배경 투명화 (고정 바와 겹침 방지) */
        [data-testid="stHeader"] {{
            background: rgba(255, 255, 255, 0.8);
            backdrop-filter: blur(6px);
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

        /* ---------- 테이블 — 얼룩무늬 + 고정 헤더 + 숫자 우측정렬 ---------- */
        div[data-testid="stDataFrame"] {{
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid {BORDER_SUBTLE};
        }}
        /* 테이블 헤더 sticky */
        div[data-testid="stDataFrame"] thead th {{
            background: {BG_SUBTLE} !important;
            color: {TEXT_MAIN} !important;
            font-weight: 700 !important;
            letter-spacing: -0.01em;
            border-bottom: 2px solid {BORDER_MEDIUM} !important;
            position: sticky; top: 0; z-index: 2;
        }}
        /* zebra striping — 짝수 행 배경 미세 차이 */
        div[data-testid="stDataFrame"] tbody tr:nth-child(even) td {{
            background: {BG_SUBTLE}80 !important;
        }}
        /* 행 호버 하이라이트 */
        div[data-testid="stDataFrame"] tbody tr:hover td {{
            background: {BRAND_PRIMARY_LIGHT}80 !important;
        }}

        /* ---------- Caption ---------- */
        div[data-testid="stCaptionContainer"] {{
            color: {TEXT_MUTED};
        }}

        /* ---------- 섹션 간 여백 (줄간격 개선) ---------- */
        hr, [data-testid="stHorizontalBlock"] + [data-testid="stHorizontalBlock"] {{
            margin-top: 0.2rem !important;
        }}
        /* 컨테이너 간격 */
        div[data-testid="stVerticalBlockBorderWrapper"] {{
            margin-bottom: 14px;
        }}
        /* divider (hr) 스타일 */
        hr {{
            border-top: 1px solid {BORDER_SUBTLE} !important;
            margin: 20px 0 !important;
            opacity: 0.8;
        }}

        /* ---------- 버튼 — 프로페셔널 톤 ---------- */
        button[kind="primary"] {{
            background: {BRAND_PRIMARY} !important;
            border-color: {BRAND_PRIMARY} !important;
            font-weight: 600 !important;
            letter-spacing: -0.01em;
        }}
        button[kind="primary"]:hover {{
            background: #1d4ed8 !important;
            box-shadow: 0 4px 12px rgba(37,99,235,0.25) !important;
        }}
        button[kind="secondary"] {{
            border-radius: 10px !important;
            font-weight: 500 !important;
        }}

        /* ---------- Alert 박스 — 일관된 톤 ---------- */
        div[data-testid="stAlert"] {{
            border-radius: 12px !important;
            border-width: 1px !important;
        }}

        /* ---------- Expander — 깔끔한 톤 ---------- */
        details {{
            border-radius: 12px !important;
            border: 1px solid {BORDER_SUBTLE} !important;
        }}
        details summary {{
            font-weight: 600 !important;
            padding: 12px 16px !important;
        }}

        /* ---------- Selectbox / Date input 일관 ---------- */
        div[data-baseweb="select"] > div {{
            border-radius: 10px !important;
            border-color: {BORDER_MEDIUM} !important;
        }}
        input[type="text"], input[type="date"] {{
            border-radius: 10px !important;
        }}

        /* ---------- 라디오 버튼 깔끔하게 ---------- */
        div[role="radiogroup"] label {{
            font-weight: 500 !important;
        }}

        /* ========== 📱 태블릿 (1024px 이하) ========== */
        @media (max-width: 1024px) {{
            .block-container {{
                padding-left: 1.2rem !important;
                padding-right: 1.2rem !important;
                max-width: 100% !important;
            }}
            /* 탭 간격 축소 */
            button[data-baseweb="tab"] {{
                padding: 8px 14px !important;
                font-size: 0.92rem !important;
            }}
        }}

        /* ========== 📱 모바일 (768px 이하) ========== */
        @media (max-width: 768px) {{
            /* 페이지 패딩 — 모바일 여백 최소화 */
            .block-container {{
                padding: 1.2rem 0.7rem 2rem 0.7rem !important;
                max-width: 100% !important;
            }}
            /* KPI 카드 */
            div[data-testid="stMetric"] {{
                padding: 12px 14px !important;
                border-radius: 10px !important;
            }}
            div[data-testid="stMetricValue"] {{
                font-size: 1.35rem !important;
                line-height: 1.15;
            }}
            div[data-testid="stMetric"] label {{
                font-size: 0.72rem !important;
            }}
            /* 커스텀 카드 숫자 스케일 */
            .stApp div[style*="font-size:2.6rem"] {{
                font-size: 1.9rem !important;
            }}
            .stApp div[style*="font-size:1.9rem"] {{
                font-size: 1.5rem !important;
            }}
            .stApp div[style*="font-size:1.75rem"] {{
                font-size: 1.4rem !important;
            }}
            .stApp div[style*="font-size:1.7rem"] {{
                font-size: 1.35rem !important;
            }}
            .stApp div[style*="font-size:1.5rem"] {{
                font-size: 1.25rem !important;
            }}
            /* 제목 계층 */
            h1 {{ font-size: 1.5rem !important; letter-spacing: -0.02em; }}
            h2 {{ font-size: 1.25rem !important; }}
            h3 {{ font-size: 1.05rem !important; }}
            h4 {{ font-size: 0.95rem !important; }}
            /* 탭 */
            button[data-baseweb="tab"] {{
                font-size: 0.82rem !important;
                padding: 8px 10px !important;
            }}
            /* 테이블 가로 스크롤 + 최소 너비 */
            div[data-testid="stDataFrame"] {{
                overflow-x: auto !important;
            }}
            /* 컨테이너 간격 */
            div[data-testid="stVerticalBlockBorderWrapper"] {{
                margin-bottom: 10px;
                padding: 12px !important;
            }}
            /* Plotly 차트 높이 축소 */
            .js-plotly-plot {{
                max-height: 280px;
            }}
            /* 사이드바 — 모바일에서 호버 시 그림자 */
            section[data-testid="stSidebar"] {{
                box-shadow: 2px 0 8px rgba(0,0,0,0.08);
            }}
            /* 페이지 헤더 컬럼 세로 적층 (모바일) */
            div[data-testid="stHorizontalBlock"]:first-of-type {{
                flex-wrap: wrap !important;
                gap: 8px !important;
            }}
            /* Alert/info 박스 패딩 축소 */
            div[data-testid="stAlert"] {{
                padding: 10px 12px !important;
            }}
            /* 버튼 모바일 터치 최적화 */
            button[kind="primary"], button[kind="secondary"] {{
                min-height: 40px !important;
                font-size: 0.88rem !important;
            }}
        }}

        /* ========== 📱 소형 모바일 (480px 이하) ========== */
        @media (max-width: 480px) {{
            .block-container {{
                padding: 1rem 0.5rem 2rem 0.5rem !important;
            }}
            /* KPI 한 줄에 2개만 */
            div[data-testid="stMetric"] {{
                padding: 8px 10px !important;
            }}
            div[data-testid="stMetricValue"] {{
                font-size: 1.15rem !important;
            }}
            div[data-testid="stMetric"] label {{
                font-size: 0.68rem !important;
            }}
            /* 큰 숫자 축소 */
            .stApp div[style*="font-size:2.6rem"] {{
                font-size: 1.6rem !important;
            }}
            .stApp div[style*="font-size:1.9rem"] {{
                font-size: 1.25rem !important;
            }}
            .stApp div[style*="font-size:1.7rem"] {{
                font-size: 1.15rem !important;
            }}
            /* 브랜드 배너 패딩 축소 */
            .stApp div[style*="padding: 14px 20px"] {{
                padding: 10px 14px !important;
            }}
            /* 캡션 더 작게 */
            div[data-testid="stCaptionContainer"] p {{
                font-size: 0.72rem !important;
            }}
            /* 제목 */
            h1 {{ font-size: 1.3rem !important; }}
            h2 {{ font-size: 1.15rem !important; }}
            h3 {{ font-size: 1rem !important; }}
            /* Plotly 차트 */
            .js-plotly-plot {{
                max-height: 240px;
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
<div style="font-size:0.7rem; color:{TEXT_MUTED}; margin-top:3px;">OPENHAN · Marketing Analytics</div>
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
    ("SYSTEM",    "📤", "CSV 업로드",     "pages/8_📤_CSV_업로드.py"),
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

    # ----------------------------------------------------------
    # 🔄 전체 업데이트 버튼
    # ----------------------------------------------------------
    render_global_refresh_button()


def render_global_refresh_button() -> None:
    """사이드바 글로벌 데이터 갱신 버튼.

    동작 환경별:
      - 로컬 PC (sync_all.bat + .venv 존재): 5개 API + 시트 + precompute 백그라운드 실행
      - Streamlit Cloud: 캐시만 강제 클리어 (parquet 다시 읽음)
    """
    import subprocess
    import platform
    from pathlib import Path

    ROOT = Path(__file__).parent.parent
    sync_bat = ROOT / "sync_all.bat"
    venv_python = ROOT / ".venv" / "Scripts" / "python.exe"
    is_local = sync_bat.exists() and venv_python.exists()

    st.sidebar.markdown(
        "<div class='nav-section-header'>UPDATE</div>",
        unsafe_allow_html=True,
    )

    # 마지막 sync 시각 + CSV 업로드 시각 표시
    try:
        from utils.precomputed import get_last_updated
        last = get_last_updated()
        if last:
            st.sidebar.caption(
                f"🕒 마지막 갱신: {last.strftime('%m/%d %H:%M')}"
            )
    except Exception:
        pass

    # CSV 업로드 폴더의 가장 최근 파일 mtime
    try:
        from datetime import datetime as _dt
        latest_csv = None
        for folder in ["coupang_sales_upload", "coupang_ads_upload"]:
            d = ROOT / "data" / folder
            if not d.exists():
                continue
            for f in d.iterdir():
                if not f.is_file():
                    continue
                if f.suffix.lower() not in (".csv", ".xlsx", ".xls"):
                    continue
                m = f.stat().st_mtime
                if latest_csv is None or m > latest_csv:
                    latest_csv = m
        if latest_csv:
            csv_dt = _dt.fromtimestamp(latest_csv)
            st.sidebar.caption(
                f"📤 CSV 마지막 업로드: {csv_dt.strftime('%m/%d %H:%M')}"
            )
    except Exception:
        pass

    if is_local:
        # 로컬 모드 — 진짜 sync 트리거
        if st.sidebar.button(
            "🔄 전체 API + 시트 갱신",
            use_container_width=True,
            help=(
                "5개 API (Naver/Coupang/Cafe24/Meta/시트) + 광고 캠페인 + "
                "프리컴퓨트 + git push 까지 자동. 5~10분 소요. "
                "별도 콘솔창이 열려 진행 상황 표시됨."
            ),
            key="global_sync_btn",
        ):
            try:
                if platform.system() == "Windows":
                    # Windows: 새 콘솔창에서 실행 (Streamlit 프로세스 차단 방지)
                    subprocess.Popen(
                        ["cmd", "/c", "start", "sync_all", str(sync_bat)],
                        cwd=str(ROOT),
                        creationflags=subprocess.CREATE_NEW_CONSOLE,
                    )
                else:
                    # Unix: nohup background
                    subprocess.Popen(
                        ["bash", str(sync_bat)],
                        cwd=str(ROOT),
                        start_new_session=True,
                    )
                st.cache_data.clear()
                st.sidebar.success(
                    "✅ Sync 백그라운드 시작!\n\n"
                    "5~10분 후 새로고침하면 최신 데이터가 반영됩니다."
                )
            except Exception as e:
                st.sidebar.error(f"❌ 실행 실패: {type(e).__name__}: {e}")
    else:
        # Cloud 모드 — 캐시만 클리어
        if st.sidebar.button(
            "🔄 캐시 새로고침",
            use_container_width=True,
            help=(
                "Streamlit 캐시 강제 비움 → parquet/JSON 다시 로드. "
                "API 재수집은 매일 10시 자동 sync 또는 로컬 PC sync_all.bat 실행."
            ),
            key="global_cache_clear_btn",
        ):
            st.cache_data.clear()
            st.sidebar.success("✅ 캐시 비움 — 페이지 새로고침됩니다.")
            st.rerun()
        st.sidebar.caption(
            ":blue[💡 API 재수집은 로컬 PC 전용. "
            "매일 10:00 자동 sync 작동 중.]"
        )


# ==========================================================
# 페이지 헤더 (제목 + 부제 + 오늘 날짜)
# ==========================================================
def render_page_header(
    title: str,
    subtitle: str | None = None,
    show_date: bool = True,
) -> None:
    """페이지 상단 헤더 — 정보 계층 강화.

    좌측: 큰 제목 + 부제
    우측: 오늘 날짜 + 데이터 신선도 배지 (프리컴퓨트 기반)
    """
    col_l, col_r = st.columns([3, 1.2])
    with col_l:
        st.markdown(
            f"<h2 style='margin:0; font-size:1.6rem; color:{TEXT_MAIN}; "
            f"letter-spacing:-0.025em; line-height:1.2;'>{title}</h2>",
            unsafe_allow_html=True,
        )
        if subtitle:
            st.markdown(
                f"<div style='color:{TEXT_MUTED}; font-size:0.88rem; "
                f"margin-top:6px; line-height:1.4;'>{subtitle}</div>",
                unsafe_allow_html=True,
            )
    with col_r:
        if show_date:
            today = datetime.now()
            date_str = (
                f"{today.year}년 {today.month}월 {today.day}일 "
                f"{_WEEKDAY_KR[today.weekday()]}"
            )
            # 프리컴퓨트 마지막 업데이트 → 신선도 배지로
            freshness_badge = ""
            try:
                from utils.precomputed import get_last_updated
                last = get_last_updated()
                if last:
                    minutes_ago = int((today - last).total_seconds() / 60)
                    if minutes_ago < 90:
                        fresh_sev = "success"
                        fresh_icon = "🟢"
                        if minutes_ago < 60:
                            fresh_txt = f"{minutes_ago}분 전 동기화"
                        else:
                            fresh_txt = f"{minutes_ago // 60}시간 전 동기화"
                    elif minutes_ago < 1800:   # 30시간
                        fresh_sev = "caution"
                        fresh_icon = "🟡"
                        fresh_txt = f"{minutes_ago // 60}시간 전"
                    else:
                        fresh_sev = "warning"
                        fresh_icon = "🟠"
                        fresh_txt = f"{minutes_ago // 60 // 24}일 전 — sync 필요"
                    cfg = STATUS_COLORS.get(fresh_sev, STATUS_COLORS["neutral"])
                    freshness_badge = (
                        f"<div style='display:inline-flex; align-items:center; "
                        f"gap:4px; background:{cfg['bg']}; color:{cfg['fg']}; "
                        f"border:1px solid {cfg['border']}; "
                        f"padding:3px 10px; border-radius:999px; "
                        f"font-size:0.72rem; font-weight:600; margin-top:6px;'>"
                        f"<span>{fresh_icon}</span><span>{fresh_txt}</span></div>"
                    )
            except Exception:
                pass

            header_html = (
                f"<div style='text-align:right; padding-top:4px;'>"
                f"<div style='font-size:0.82rem; color:{TEXT_MUTED}; "
                f"font-weight:500;'>📅 {date_str}</div>"
                f"{freshness_badge}"
                f"</div>"
            )
            st.markdown(header_html, unsafe_allow_html=True)
    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)


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
        # "auto" = 모바일에서 자동 접힘, 데스크톱에서 열림
        initial_sidebar_state="auto",
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


# ==========================================================
# 상태 Pill (severity 기반 — critical/warning/success/info/neutral)
# ==========================================================
def render_status_pill(
    severity: str, label: str, icon: str | None = None,
) -> str:
    """severity 기반 둥근 배지 HTML.

    severity: critical / warning / caution / success / opportunity / info / neutral
    """
    cfg = STATUS_COLORS.get(severity, STATUS_COLORS["neutral"])
    icon_map = {
        "critical": "🚨", "warning": "⚠️", "caution": "⚡",
        "success": "✓", "opportunity": "✨", "info": "ℹ️", "neutral": "•",
    }
    ico = icon or icon_map.get(severity, "•")
    html = f"""
<span style="display:inline-flex; align-items:center; gap:5px;
background:{cfg['bg']}; color:{cfg['fg']}; border:1px solid {cfg['border']};
padding:3px 10px; border-radius:999px; font-size:0.72rem; font-weight:600;
letter-spacing:-0.01em; line-height:1;">
<span>{ico}</span><span>{label}</span></span>
"""
    return _clean_html(html)


# ==========================================================
# 오늘의 하이라이트 액션 카드 (홈 최상단용)
# ==========================================================
def render_insight_card(
    severity: str,
    title: str,
    detail: str,
    metric_value: str = "",
    metric_label: str = "",
    action_hint: str = "",
) -> str:
    """홈 '오늘의 하이라이트' 액션 카드 — 클릭 유도형 HTML 카드.

    severity: critical / warning / success / opportunity / info
    """
    cfg = STATUS_COLORS.get(severity, STATUS_COLORS["info"])
    icon_map = {
        "critical": "🚨", "warning": "⚠️", "caution": "⚡",
        "success": "✨", "opportunity": "🎯", "info": "💡", "neutral": "•",
    }
    icon = icon_map.get(severity, "•")
    metric_html = ""
    if metric_value:
        metric_html = f"""
<div style="margin-top:10px; padding-top:10px; border-top:1px dashed {cfg['border']};">
<div style="font-size:0.7rem; color:{cfg['fg']}; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;">{metric_label}</div>
<div style="font-size:1.5rem; font-weight:800; color:{cfg['fg']}; line-height:1.1; margin-top:3px; letter-spacing:-0.02em;">{metric_value}</div>
</div>
"""
    action_html = ""
    if action_hint:
        action_html = f"""
<div style="margin-top:10px; font-size:0.78rem; color:{cfg['fg']}; font-weight:600; display:flex; align-items:center; gap:4px; opacity:0.85;">
<span>→</span><span>{action_hint}</span>
</div>
"""
    html = f"""
<div style="background:{cfg['bg']}; border:1px solid {cfg['border']}; border-radius:14px; padding:16px 18px; height:100%; position:relative; overflow:hidden;">
<div style="position:absolute; top:-10px; right:-10px; font-size:3rem; opacity:0.12;">{icon}</div>
<div style="display:flex; align-items:center; gap:8px; margin-bottom:8px;">
<span style="font-size:1.1rem;">{icon}</span>
<span style="font-size:0.72rem; color:{cfg['fg']}; font-weight:700; text-transform:uppercase; letter-spacing:0.05em;">{severity.upper()}</span>
</div>
<div style="font-size:1rem; font-weight:700; color:{TEXT_MAIN}; line-height:1.3; margin-bottom:4px;">{title}</div>
<div style="font-size:0.85rem; color:{TEXT_MUTED}; line-height:1.45;">{detail}</div>
{metric_html}
{action_html}
</div>
"""
    return _clean_html(html)


# ==========================================================
# 빈 상태 안내 — "무엇을 해야 하는지" 명확히
# ==========================================================
def render_empty_state(
    title: str,
    description: str,
    icon: str = "📭",
    action_label: str = "",
) -> None:
    """데이터가 없을 때 표시. 단순 '데이터 없음' 대신 다음 액션 안내."""
    html = f"""
<div style="background:{BG_SUBTLE}; border:1px dashed {BORDER_MEDIUM}; border-radius:14px; padding:32px 24px; text-align:center; margin:12px 0;">
<div style="font-size:2.2rem; margin-bottom:8px; opacity:0.6;">{icon}</div>
<div style="font-weight:700; color:{TEXT_MAIN}; font-size:1.05rem; margin-bottom:6px;">{title}</div>
<div style="color:{TEXT_MUTED}; font-size:0.88rem; line-height:1.5; max-width:560px; margin:0 auto;">{description}</div>
{f'<div style="margin-top:14px; color:{BRAND_PRIMARY}; font-size:0.85rem; font-weight:600;">→ {action_label}</div>' if action_label else ''}
</div>
"""
    st.markdown(_clean_html(html), unsafe_allow_html=True)


# ==========================================================
# 통합 기간 선택기 — 전 페이지 동일 UI/위치
# ==========================================================
def render_period_picker(
    max_date,
    min_date=None,
    key_prefix: str = "",
    default_option: str = "최근 30일",
    show_custom: bool = True,
) -> dict:
    """페이지 제목 바로 아래 통일된 기간 선택 UI.

    Returns:
        dict with keys: period, start_date, end_date, days
    """
    from datetime import date as _date, timedelta as _td
    import pandas as _pd

    today_real = _date.today()
    if max_date is None:
        max_date = today_real
    if min_date is None:
        min_date = _date(today_real.year - 1, 1, 1)
    # max_date 가 today 초과 시 제한
    if max_date > today_real:
        max_date = today_real

    options = ["최근 7일", "최근 14일", "최근 30일", "최근 90일", "이번 달", "올해"]
    if show_custom:
        options.append("사용자 지정")

    default_idx = options.index(default_option) if default_option in options else 2

    c1, c2, c3, _ = st.columns([1.1, 1.1, 1.1, 1.7])
    with c1:
        period = st.selectbox(
            "🗓️ 조회 기간", options, index=default_idx,
            key=f"{key_prefix}_period",
        )
    with c2:
        end_date = st.date_input(
            "종료일", value=max_date,
            min_value=min_date, max_value=today_real,
            key=f"{key_prefix}_end_date",
            help="실제 오늘까지 선택 가능",
        )

    # 기간 계산
    today = end_date
    if period == "이번 달":
        start_date = _pd.Timestamp(today.replace(day=1))
        end_ts = _pd.Timestamp(today)
        days = (end_ts - start_date).days + 1
    elif period == "올해":
        start_date = _pd.Timestamp(_date(today.year, 1, 1))
        end_ts = _pd.Timestamp(today)
        days = (end_ts - start_date).days + 1
    elif period == "사용자 지정":
        with c3:
            start_picked = st.date_input(
                "시작일", value=today - _td(days=6),
                min_value=min_date, max_value=today,
                key=f"{key_prefix}_start_date",
            )
        start_date = _pd.Timestamp(start_picked)
        end_ts = _pd.Timestamp(today)
        days = (end_ts - start_date).days + 1
    else:
        days_map = {"최근 7일": 7, "최근 14일": 14,
                    "최근 30일": 30, "최근 90일": 90}
        days = days_map[period]
        end_ts = _pd.Timestamp(today)
        start_date = end_ts - _pd.Timedelta(days=days - 1)

    # 기간 안내 캡션 (통일)
    st.markdown(
        f"<div style='color:{TEXT_MUTED}; font-size:0.82rem; "
        f"margin:-4px 0 12px 0;'>📅 <b style='color:{TEXT_MAIN};'>"
        f"{start_date.date()} ~ {end_ts.date()}</b> "
        f"<span style='color:{TEXT_FAINT};'>({days}일)</span></div>",
        unsafe_allow_html=True,
    )

    return {
        "period": period,
        "start_date": start_date,
        "end_date": end_ts,
        "days": days,
    }


# ==========================================================
# 비교 모드 토글 — 전주/전월/전년 델타 계산 기준
# ==========================================================
# ==========================================================
# 엑셀/CSV 내보내기 버튼 — 전 페이지 통일 UI
# ==========================================================
def render_download_button(
    df,
    filename_base: str,
    label: str = "📥 CSV 다운로드",
    key: str = "",
    include_index: bool = False,
) -> None:
    """DataFrame → CSV 다운로드 버튼 (UTF-8-sig, 한글 엑셀 호환).

    Args:
        df: pd.DataFrame
        filename_base: 파일명 베이스 (날짜 자동 추가)
        label: 버튼 레이블
        key: streamlit key (중복 방지)
        include_index: index 포함 여부
    """
    from datetime import datetime as _dt
    import pandas as _pd

    if df is None or (hasattr(df, "empty") and df.empty):
        return

    ts = _dt.now().strftime("%Y%m%d_%H%M")
    filename = f"{filename_base}_{ts}.csv"
    try:
        csv_data = df.to_csv(index=include_index, encoding="utf-8-sig").encode("utf-8-sig")
    except Exception:
        # Fallback — string form
        csv_data = df.to_csv(index=include_index).encode("utf-8-sig")

    st.download_button(
        label=label,
        data=csv_data,
        file_name=filename,
        mime="text/csv",
        key=key or f"dl_{filename_base}",
        help=f"선택 기간 데이터를 CSV 파일로 저장 (엑셀 호환 UTF-8 BOM)",
    )


def render_comparison_toggle(
    key_prefix: str = "", current_end=None,
) -> dict:
    """비교 기준 선택 UI — segmented button 스타일.

    Returns:
        dict with keys: mode, prev_start, prev_end, label
    """
    import pandas as _pd
    from datetime import timedelta as _td, date as _date

    options = ["직전 기간", "전주 동기", "전월 동기", "전년 동기"]
    help_text = (
        "직전 기간: 이번 기간 바로 이전 같은 일수\n"
        "전주 동기: 1주일 전 같은 요일 구간\n"
        "전월 동기: 1개월 전 같은 일자 구간\n"
        "전년 동기: 1년 전 같은 일자 구간"
    )
    mode = st.radio(
        "🔀 비교 기준",
        options, index=0, horizontal=True,
        key=f"{key_prefix}_cmp_mode",
        help=help_text,
    )
    return {"mode": mode}


def compute_comparison_range(
    start, end, mode: str,
):
    """비교 기간 범위 계산.

    Args:
        start, end: 이번 기간 (pd.Timestamp)
        mode: '직전 기간' / '전주 동기' / '전월 동기' / '전년 동기'
    Returns:
        (prev_start, prev_end) — pd.Timestamp
    """
    import pandas as _pd
    from datetime import timedelta as _td

    days = (end - start).days + 1
    if mode == "직전 기간":
        prev_end = start - _pd.Timedelta(days=1)
        prev_start = prev_end - _pd.Timedelta(days=days - 1)
    elif mode == "전주 동기":
        prev_start = start - _pd.Timedelta(days=7)
        prev_end = end - _pd.Timedelta(days=7)
    elif mode == "전월 동기":
        # 단순 30일 shift (월별 일수 복잡성 회피)
        prev_start = start - _pd.Timedelta(days=30)
        prev_end = end - _pd.Timedelta(days=30)
    elif mode == "전년 동기":
        prev_start = start - _pd.DateOffset(years=1)
        prev_end = end - _pd.DateOffset(years=1)
        # 결과를 Timestamp 로 확실히
        prev_start = _pd.Timestamp(prev_start)
        prev_end = _pd.Timestamp(prev_end)
    else:
        prev_end = start - _pd.Timedelta(days=1)
        prev_start = prev_end - _pd.Timedelta(days=days - 1)
    return prev_start, prev_end


# ==========================================================
# 큰 수치 전용 메트릭 (가독성 최우선)
# ==========================================================
def render_big_metric(
    label: str, value: str, delta: str = "",
    delta_color: str = "", value_color: str = TEXT_MAIN,
) -> str:
    """라벨 작게 / 메인 수치 크고 볼드 / delta 중간 — 정보 계층 명확.

    value_color / delta_color 는 hex 컬러 또는 빈 문자열.
    """
    delta_html = ""
    if delta:
        dc = delta_color or TEXT_MUTED
        delta_html = (
            f"<div style='font-size:0.8rem; font-weight:600; "
            f"color:{dc}; margin-top:4px;'>{delta}</div>"
        )
    html = f"""
<div style="padding:4px 0;">
<div style="font-size:0.75rem; color:{TEXT_FAINT}; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:2px;">{label}</div>
<div style="font-size:1.7rem; font-weight:800; color:{value_color}; line-height:1.15; letter-spacing:-0.02em;">{value}</div>
{delta_html}
</div>
"""
    return _clean_html(html)
