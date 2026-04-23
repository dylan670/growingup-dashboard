"""매출 통합 분석 — 브랜드 탭(전체/똑똑연구소/롤라루) × 스토어별 성과 및 액션."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.data import load_orders, load_coupang_inbound
from utils.actions import store_sales_actions, THRESHOLDS
from utils.products import (
    filter_orders_by_brand,
    BRAND_ORDER_STORES,
    BRAND_MONTHLY_TARGETS,
    BRAND_STORE_MONTHLY_TARGETS,
    store_display_name,
)
from utils.ui import (
    setup_page, render_brand_banner,
    format_won_compact, kpi_card,
    render_period_picker, render_status_pill,
    render_comparison_toggle, compute_comparison_range,
    METRIC_COLORS, CHANNEL_COLORS, TEXT_MAIN, TEXT_MUTED,
)
from api.google_sheets import load_sheet_daily_sales, get_brand_channels


setup_page(
    page_title="매출 분석",
    page_icon="💰",
    header_title="💰 매출 분석",
    header_subtitle="3개 브랜드 · 다중 채널 매출 및 목표 달성률 (구글 시트 실시간 연동)",
)

orders = load_orders()
# orders 범위는 데이터 있는 구간
orders_min = orders["date"].min().date() if not orders.empty else None
orders_max = orders["date"].max().date() if not orders.empty else None
# 실제 오늘 (데이터 없어도 선택 가능)
from datetime import date as _today_func
today_real = _today_func.today()
# min 은 orders 첫날 or 작년 시작
min_allowed = orders_min if orders_min else _today_func(today_real.year - 1, 1, 1)
# max 는 실제 오늘 (데이터 없어도 선택 가능)
max_allowed = today_real

# ==========================================================
# 기간 선택 (통합 picker — 전 페이지 동일 UI)
# ==========================================================
_pp = render_period_picker(
    max_date=orders_max if orders_max else today_real,
    min_date=min_allowed,
    key_prefix="sales",
    default_option="최근 30일",
)
period = _pp["period"]
start_date = _pp["start_date"]
end_date = _pp["end_date"].date()
days = _pp["days"]

# 비교 기준 토글 (직전 기간 / 전주 / 전월 / 전년)
_cmp = render_comparison_toggle(key_prefix="sales",
                                current_end=pd.Timestamp(end_date))
prev_start, prev_end = compute_comparison_range(
    start_date, pd.Timestamp(end_date), _cmp["mode"],
)
# 비교 범위 안내 캡션
st.markdown(
    f"<div style='color:{TEXT_MUTED}; font-size:0.78rem; "
    f"margin:-8px 0 12px 0;'>🔀 비교: <b>{_cmp['mode']}</b> · "
    f"{prev_start.date()} ~ {prev_end.date()}</div>",
    unsafe_allow_html=True,
)


# ==========================================================
# 스토어 라벨
# ==========================================================
# 스토어 카드의 (메인 라벨, 부제) — brand_context 에 따라 메인 라벨 자동 단축
STORE_SUBTITLES = {
    "똑똑연구소":         "네이버 스마트스토어",
    "롤라루":             "네이버 스마트스토어",
    "쿠팡":               "쿠팡 (브랜드 분류 전)",
    # Wing Open API 가 업체배송(판매자 판매) + 로켓그로스 주문 모두 수집
    "쿠팡_똑똑연구소":    "쿠팡 판매자 판매 · 로켓그로스 (Wing)",
    "쿠팡_롤라루":        "쿠팡 판매자 판매 · 로켓그로스 (Wing)",
    "자사몰_똑똑연구소":  "Cafe24 자사몰",
    "자사몰_롤라루":      "Cafe24 자사몰",
}


# ==========================================================
# 렌더링 함수 — 브랜드별 매출 분석
# ==========================================================
# ==========================================================
# 일별 매출 달성 헬퍼
# ==========================================================
WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _days_in_month(ts: pd.Timestamp) -> int:
    return pd.Period(ts, freq="M").days_in_month


def _pct_color(pct: float) -> str:
    """달성률 → 컬러 (참고 시트 스타일)."""
    if pct >= 100:
        return "#16a34a"   # green
    if pct >= 80:
        return "#ca8a04"   # amber
    if pct >= 50:
        return "#ea580c"   # orange
    return "#dc2626"       # red


@st.cache_data(ttl=300, show_spinner="구글 시트에서 매출 데이터 가져오는 중…")
def _cached_sheet_sales() -> pd.DataFrame:
    """시트 데이터 5분 캐싱. 실패 시 올바른 dtype 의 빈 DF 반환."""
    try:
        df = load_sheet_daily_sales()
        if not df.empty and "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame({
            "date": pd.Series([], dtype="datetime64[ns]"),
            "brand": pd.Series([], dtype="object"),
            "channel": pd.Series([], dtype="object"),
            "target": pd.Series([], dtype="int64"),
            "actual": pd.Series([], dtype="int64"),
        })


def _render_daily_achievement(
    sheet_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    brand: str,
):
    """일별 매출 달성 표 — Google Sheets 직접 로드.

    시트 소스: 팀이 매일 수동 업데이트하는 '2026 그로잉업팀 일간통계'
              → 목표·달성 100% 일치 (대시보드 계산식 사용 X).
    """
    # 해당 브랜드 + 기간 필터
    df = sheet_df[
        (sheet_df["brand"] == brand)
        & (sheet_df["date"] >= start)
        & (sheet_df["date"] <= end)
    ].copy()

    if df.empty:
        st.info(
            f"{brand}: 시트에 선택 기간 데이터 없음. "
            f"(팀에서 매일 오후 시트 업데이트 후 5분 이내 반영)"
        )
        return

    # 날짜별로 피벗 — 각 채널을 컬럼으로
    date_range = pd.date_range(start.date(), end.date(), freq="D")
    channels = sorted(df["channel"].unique().tolist())

    rows: list[dict] = []
    for d in date_range:
        d_ts = pd.Timestamp(d)
        day_data = df[df["date"] == d_ts]

        # 이 날 각 채널 target/actual
        day_target = int(day_data["target"].sum())
        day_actual = int(day_data["actual"].sum())
        pct = (day_actual / day_target * 100) if day_target > 0 else 0

        row: dict = {
            "날짜": d.date().isoformat(),
            "요일": WEEKDAY_KR[d.weekday()],
            "일 목표": day_target,
            "일 달성": day_actual,
            "달성률(%)": round(pct, 0),
        }
        # 채널별 목표·달성
        for ch in channels:
            ch_data = day_data[day_data["channel"] == ch]
            t = int(ch_data["target"].sum()) if not ch_data.empty else 0
            a = int(ch_data["actual"].sum()) if not ch_data.empty else 0
            row[f"{ch} 목표"] = t
            row[f"{ch} 달성"] = a
        rows.append(row)

    table = pd.DataFrame(rows)

    # ---------- 요약 ----------
    sum_target = int(table["일 목표"].sum())
    sum_actual = int(table["일 달성"].sum())
    sum_pct = (sum_actual / sum_target * 100) if sum_target else 0
    days_achieved = int((table["달성률(%)"] >= 100).sum())
    days_total = len(table)

    st.markdown(
        f"#### 📅 {brand} 일별 매출 달성 현황  "
        f"<span style='font-size:0.75rem; color:#64748b; font-weight:400;'>"
        f"(소스: 구글 시트 · 5분 캐싱)</span>",
        unsafe_allow_html=True,
    )
    m1, m2, m3, m4 = st.columns(4)
    # kpi_card: compact 메인 + 전체 숫자 sub — 짤림 없이 가독성 확보
    m1.markdown(
        kpi_card(
            "기간 목표",
            format_won_compact(sum_target),
            sub=f"{sum_target:,}원",
        ),
        unsafe_allow_html=True,
    )
    m2.markdown(
        kpi_card(
            "기간 달성",
            format_won_compact(sum_actual),
            sub=f"{sum_actual:,}원",
            value_color="#2563eb",
        ),
        unsafe_allow_html=True,
    )
    m3.metric(
        "기간 달성률",
        f"{sum_pct:.0f}%",
        delta=f"{sum_pct - 100:+.0f}%p vs 목표" if sum_target > 0 else None,
    )
    m4.metric(
        "목표 도달 일수",
        f"{days_achieved}/{days_total}일",
        help="일 목표 100% 이상 달성한 일수 / 전체 일수",
    )

    # ---------- 표 ----------
    col_cfg = {
        "날짜": st.column_config.TextColumn("날짜", width="small"),
        "요일": st.column_config.TextColumn("요일", width="small"),
        "일 목표": st.column_config.NumberColumn("일 목표", format="%d원"),
        "일 달성": st.column_config.NumberColumn("일 달성", format="%d원"),
        "달성률(%)": st.column_config.ProgressColumn(
            "달성률", format="%d%%", min_value=0, max_value=200,
        ),
    }
    for ch in channels:
        col_cfg[f"{ch} 목표"] = st.column_config.NumberColumn(
            f"{ch} 목표", format="%d원",
        )
        col_cfg[f"{ch} 달성"] = st.column_config.NumberColumn(
            f"{ch} 달성", format="%d원",
        )

    st.dataframe(
        table,
        width="stretch",
        hide_index=True,
        column_config=col_cfg,
        height=min(520, 50 + len(table) * 36),
    )

    # ---------- 채널별 기간 요약 ----------
    channel_summary = (
        df.groupby("channel")
        .agg(목표=("target", "sum"), 달성=("actual", "sum"))
        .reset_index()
    )
    channel_summary["달성률(%)"] = (
        channel_summary["달성"] / channel_summary["목표"].replace(0, pd.NA) * 100
    ).round(0).fillna(0).astype(int)
    channel_summary = channel_summary.sort_values("목표", ascending=False)

    with st.expander(f"📊 {brand} 채널별 기간 합계 (시트 기준)"):
        st.dataframe(
            channel_summary.rename(columns={"channel": "채널"}),
            width="stretch", hide_index=True,
            column_config={
                "목표": st.column_config.NumberColumn("목표", format="%d원"),
                "달성": st.column_config.NumberColumn("달성", format="%d원"),
                "달성률(%)": st.column_config.ProgressColumn(
                    "달성률", format="%d%%", min_value=0, max_value=200,
                ),
            },
        )


def render_sales_overview(
    orders_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    prev_start: pd.Timestamp,
    prev_end: pd.Timestamp,
    brand: str | None = None,
):
    """브랜드별 매출 렌더링."""
    brand_label = brand if brand else "전체"

    curr = orders_df[(orders_df["date"] >= start) & (orders_df["date"] <= end)]
    prev = orders_df[(orders_df["date"] >= prev_start) & (orders_df["date"] <= prev_end)]

    # ---------- 전체 KPI ----------
    total_orders = len(curr)
    total_rev = int(curr["revenue"].sum())
    total_customers = curr["customer_id"].nunique()
    avg_aov = int(total_rev / total_orders) if total_orders else 0

    prev_rev = int(prev["revenue"].sum())
    rev_change = ((total_rev - prev_rev) / prev_rev * 100) if prev_rev else 0

    st.markdown(f"#### 📈 {brand_label} 매출 요약")
    k1, k2, k3, k4 = st.columns(4)

    # 전 기간 대비 변화 서브
    rev_sub = (
        f"전 기간 대비 {rev_change:+.0f}%" if prev_rev else "전 기간 없음"
    )
    rev_color = "#16a34a" if rev_change >= 0 else "#dc2626"

    k1.markdown(
        kpi_card(
            "총 매출",
            format_won_compact(total_rev),
            sub=rev_sub,
            value_color=rev_color if prev_rev else "#2563eb",
        ),
        unsafe_allow_html=True,
    )
    k2.markdown(
        kpi_card(
            "총 주문",
            f"{total_orders:,}건",
            sub=f"주문 기간 {(end - start).days + 1}일",
        ),
        unsafe_allow_html=True,
    )
    k3.markdown(
        kpi_card(
            "고유 고객",
            f"{total_customers:,}명",
            sub=f"1인당 {(total_orders / total_customers):.1f}회" if total_customers else "—",
        ),
        unsafe_allow_html=True,
    )
    k4.markdown(
        kpi_card(
            "평균 객단가",
            format_won_compact(avg_aov),
            sub=f"{avg_aov:,}원" if avg_aov else "—",
        ),
        unsafe_allow_html=True,
    )

    # ---------- 일별 매출 달성 표 (구글 시트 직접 로드) ----------
    if brand in ("똑똑연구소", "롤라루", "루티니스트"):
        st.divider()
        try:
            sheet_df = _cached_sheet_sales()
            _render_daily_achievement(sheet_df, start, end, brand)
        except Exception as e:
            st.warning(
                f"구글 시트 로드 실패: {type(e).__name__}: {e}\n\n"
                f"시트 접근 권한 · 네트워크 확인 필요."
            )

    # ---------- 스토어별 카드 ----------
    st.divider()
    st.markdown(f"#### 🏪 {brand_label} 스토어별 성과 및 액션")

    # 이 브랜드에 존재하는 스토어 — 고정 순서 (자사몰 → 네이버 → 쿠팡)
    _ORDER_RANK = [
        "자사몰_똑똑연구소", "자사몰_롤라루",
        "똑똑연구소", "롤라루",
        "쿠팡_똑똑연구소", "쿠팡_롤라루", "쿠팡",  # 쿠팡은 브랜드 분류 전 레거시용
    ]
    existing_stores = (
        orders_df["store"].dropna().unique().tolist()
        if "store" in orders_df.columns else []
    )
    display_stores = [s for s in _ORDER_RANK if s in existing_stores]

    if not display_stores:
        from utils.ui import render_empty_state
        render_empty_state(
            title=f"{brand_label}: 해당 기간 주문 데이터 없음",
            description=(
                f"선택된 기간 ({start.date()} ~ {end.date()}) 에 "
                f"{brand_label} 스토어의 주문이 한 건도 없습니다. "
                f"기간을 넓혀보거나 상단 종료일을 조정해보세요."
            ),
            icon="📭",
            action_label="기간 필터 조정 or 다른 브랜드 탭 확인",
        )
        return

    # 쿠팡 벤더 발주 데이터 (로켓배송 B2B) — 쿠팡 카드에 합산 표시용
    inbound_all = load_coupang_inbound()
    inbound_curr = (
        inbound_all[
            (inbound_all["date"] >= start) & (inbound_all["date"] <= end)
        ] if not inbound_all.empty else pd.DataFrame()
    )

    for store in display_stores:
        store_curr = curr[curr["store"] == store]
        store_prev = prev[prev["store"] == store]
        store_all = orders_df[orders_df["store"] == store]

        # ---- 쿠팡 카드 특수 처리: 벤더 발주 매출 합산 ----
        is_coupang = store.startswith("쿠팡")
        vendor_store = f"{store}_벤더" if is_coupang else None
        vendor_curr = pd.DataFrame()
        vendor_revenue = 0
        if is_coupang and not inbound_curr.empty and vendor_store:
            vendor_curr = inbound_curr[inbound_curr["store"] == vendor_store]
            vendor_revenue = int(vendor_curr["revenue"].sum()) if not vendor_curr.empty else 0

        # 판매자판매(Wing) + 로켓그로스 주문 없고 벤더 발주도 없으면 skip
        if store_curr.empty and vendor_revenue == 0:
            continue

        orders_n = len(store_curr)
        wing_revenue = int(store_curr["revenue"].sum()) if not store_curr.empty else 0
        combined_revenue = wing_revenue + vendor_revenue   # 쿠팡은 합산
        revenue = combined_revenue if is_coupang else wing_revenue
        customers_n = store_curr["customer_id"].nunique() if not store_curr.empty else 0
        aov = int(wing_revenue / orders_n) if orders_n else 0

        cust_order_counts = store_all.groupby("customer_id").size() if not store_all.empty else pd.Series(dtype=int)
        rep_rate = (cust_order_counts >= 2).mean() * 100 if len(cust_order_counts) else 0

        # 매출 상위 상품 — 쿠팡은 Wing + 벤더 합쳐서
        if is_coupang and not vendor_curr.empty:
            combined_for_top = pd.concat([store_curr, vendor_curr], ignore_index=True) \
                if not store_curr.empty else vendor_curr
        else:
            combined_for_top = store_curr
        top_products = (
            combined_for_top.groupby("product")
            .agg(qty=("quantity", "sum"), rev=("revenue", "sum"))
            .sort_values("rev", ascending=False)
            .head(3)
        ) if not combined_for_top.empty else pd.DataFrame()

        prev_orders_n = len(store_prev)

        metrics = {
            "store": store,
            "orders": orders_n,
            "revenue": revenue,
            "customers": customers_n,
            "aov": aov,
            "repurchase_rate": rep_rate,
            "orders_prev_period": prev_orders_n,
        }
        actions = store_sales_actions(metrics, store_all)
        label = store_display_name(store, brand_context=brand)
        sublabel = STORE_SUBTITLES.get(store, "")

        with st.container(border=True):
            col_info, col_prod, col_action = st.columns([2, 2, 3])

            with col_info:
                # 좁은 col_info (2/7 너비) 에서 '네이버 스마트스토어 (똑똑)' 등 긴 타이틀이
                # 두 줄로 잘리는 문제 해결 — 폰트 축소 + 한 줄 강제 + 초과 시 ellipsis
                st.markdown(
                    f"<h3 style='margin:0 0 2px 0; font-size:1.15rem; "
                    f"font-weight:700; color:#0f172a; "
                    f"white-space:nowrap; overflow:hidden; "
                    f"text-overflow:ellipsis;' title='{label}'>{label}</h3>",
                    unsafe_allow_html=True,
                )
                if sublabel:
                    st.caption(sublabel)

                if is_coupang:
                    # 쿠팡: 합산 매출만 표시 (주문/고객/AOV 숨김 — 벤더 B2B 라 의미 상이)
                    st.metric("매출 (합계)", f"{revenue:,}원")
                    breakdown_lines = []
                    if wing_revenue > 0:
                        breakdown_lines.append(
                            f"• 판매자 판매 · 로켓그로스 (Wing): **{wing_revenue:,}원**"
                        )
                    if vendor_revenue > 0:
                        breakdown_lines.append(
                            f"• 벤더 발주 · 로켓배송 (Supplier Hub): **{vendor_revenue:,}원**"
                        )
                    if breakdown_lines:
                        st.caption("\n".join(breakdown_lines))
                else:
                    # 자사몰/네이버: 기존 매출/주문/고객/AOV/재구매율
                    st.metric(
                        "매출", f"{revenue:,}원",
                        delta=(
                            f"{((orders_n - prev_orders_n) / prev_orders_n * 100):+.0f}% 주문"
                            if prev_orders_n else None
                        ),
                    )
                    mc1, mc2 = st.columns(2)
                    mc1.metric("주문", f"{orders_n:,}건")
                    mc2.metric("고객", f"{customers_n:,}명")
                    st.metric("AOV", f"{aov:,}원")
                    st.caption(f"재구매율 (전 기간) **{rep_rate:.1f}%**")

            with col_prod:
                st.markdown("**매출 상위 상품 (이번 기간)**")
                if not top_products.empty:
                    for prod, row in top_products.iterrows():
                        short_name = prod[:30] + ("…" if len(prod) > 30 else "")
                        st.caption(
                            f"**{short_name}**  \n"
                            f"수량 {int(row['qty'])} · 매출 {int(row['rev']):,}원"
                        )
                else:
                    st.caption("-")

            with col_action:
                st.markdown("**추천 액션**")
                for a in actions:
                    sev = a["severity"]
                    body = f"**{a['label']}**  \n{a['detail']}"
                    if sev == "critical":
                        st.error(body)
                    elif sev == "warning":
                        st.warning(body)
                    elif sev == "opportunity":
                        st.success(body)
                    elif sev == "info":
                        st.info(body)
                    else:
                        st.caption(body)

    # ---------- 시트 전용 채널 카드 (주문 API 없는 채널) ----------
    # 롤라루: 쿠팡 로켓배송, 무신사, 오프라인, 이지웰, 오늘의집
    # 똑똑연구소: 시트상 채널 모두 orders.csv 에 포함 (Wing API) → 추가 카드 없음
    # 쿠팡 로켓배송은 메인 쿠팡 카드(Wing + 벤더 발주)에 합산 표시되므로 여기 제외
    sheet_only_channels_by_brand: dict[str, list[tuple[str, str]]] = {
        "롤라루": [
            ("무신사",        "무신사 입점 (API 미연동 — 시트 기반)"),
            ("오프라인",      "오프라인 판매 (시트 기반)"),
            ("이지웰",        "복지몰 이지웰 (시트 기반)"),
            ("오늘의집",      "오늘의집 입점 (시트 기반)"),
        ],
    }
    if brand in sheet_only_channels_by_brand:
        try:
            sheet_df = _cached_sheet_sales()
        except Exception:
            sheet_df = pd.DataFrame()

        if not sheet_df.empty:
            rendered_any = False
            for ch_name, sub in sheet_only_channels_by_brand[brand]:
                sub_df = sheet_df[
                    (sheet_df["brand"] == brand)
                    & (sheet_df["channel"] == ch_name)
                    & (sheet_df["date"] >= start)
                    & (sheet_df["date"] <= end)
                ]
                if sub_df.empty or int(sub_df["actual"].sum()) == 0:
                    continue
                if not rendered_any:
                    st.markdown(
                        "##### 📋 시트 기반 채널 (API 미연동)"
                    )
                    rendered_any = True
                _render_sheet_only_channel_card(sub_df, ch_name, sub, brand)

    # ---------- 채널별 성과 비교 그래프 (매출 · 주문 · 고객) ----------
    # 브랜드 탭에서만 (전체 탭은 스토어/브랜드 섞여 비교 의미 희박)
    if brand and brand in ("똑똑연구소", "롤라루", "루티니스트"):
        _render_channel_comparison_chart(
            curr, brand, start, end, sheet_only_channels_by_brand.get(brand, []),
        )


def _render_channel_comparison_chart(
    curr: pd.DataFrame,
    brand: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    sheet_only: list[tuple[str, str]],
) -> None:
    """채널별 매출·주문·고객 비교 — grouped bar chart (3 subplot).

    - orders.csv 기반 채널 (자사몰/네이버/쿠팡 판매자판매)
    - 시트 기반 채널 (쿠팡 로켓배송/무신사/오프라인/이지웰/오늘의집)
      → 매출만 (주문/고객 없음)
    """
    from plotly.subplots import make_subplots

    # ---- orders.csv 기반 채널 집계 (store 단위 → 간결한 채널명으로 그룹) ----
    channel_data: dict[str, dict] = {}

    if not curr.empty:
        for store in curr["store"].dropna().unique():
            sdf = curr[curr["store"] == store]
            label = store_display_name(store, brand_context=brand)
            if label not in channel_data:
                channel_data[label] = {"revenue": 0, "orders": 0,
                                       "customers": set()}
            channel_data[label]["revenue"] += int(sdf["revenue"].sum())
            channel_data[label]["orders"] += len(sdf)
            channel_data[label]["customers"].update(
                sdf["customer_id"].dropna().unique()
            )

    # ---- 쿠팡 벤더 발주 매출 합산 (로켓배송 B2B) → '쿠팡' 라벨에 통합 ----
    # 메인 쿠팡 카드와 동일한 방식으로 합산: 매출만 반영 (주문/고객 없음)
    inbound_all = load_coupang_inbound()
    if not inbound_all.empty:
        inbound_sub = inbound_all[
            (inbound_all["date"] >= start) & (inbound_all["date"] <= end)
        ]
        for store in inbound_sub["store"].dropna().unique():
            # '쿠팡_롤라루_벤더' → '쿠팡_롤라루' 로 라벨 통합
            base_store = store.replace("_벤더", "") if store.endswith("_벤더") else store
            label = store_display_name(base_store, brand_context=brand)
            sdf = inbound_sub[inbound_sub["store"] == store]
            rev = int(sdf["revenue"].sum())
            if rev <= 0:
                continue
            if label not in channel_data:
                channel_data[label] = {"revenue": 0, "orders": 0,
                                       "customers": set()}
            channel_data[label]["revenue"] += rev

    # ---- 시트 기반 채널 매출 추가 (주문/고객 없음) ----
    if sheet_only:
        try:
            sheet_df = _cached_sheet_sales()
        except Exception:
            sheet_df = pd.DataFrame()
        if not sheet_df.empty:
            for ch_name, _sub in sheet_only:
                sub = sheet_df[
                    (sheet_df["brand"] == brand)
                    & (sheet_df["channel"] == ch_name)
                    & (sheet_df["date"] >= start)
                    & (sheet_df["date"] <= end)
                ]
                rev = int(sub["actual"].sum()) if not sub.empty else 0
                if rev > 0:
                    channel_data[ch_name] = {
                        "revenue": rev, "orders": 0, "customers": set(),
                        "sheet_only": True,
                    }

    if not channel_data:
        return

    # 정렬 — 매출순
    sorted_channels = sorted(
        channel_data.items(), key=lambda x: x[1]["revenue"], reverse=True,
    )
    labels = [c[0] for c in sorted_channels]
    revenues = [c[1]["revenue"] for c in sorted_channels]
    orders_cnt = [c[1]["orders"] for c in sorted_channels]
    customers_cnt = [len(c[1]["customers"]) for c in sorted_channels]
    is_sheet = [c[1].get("sheet_only", False) for c in sorted_channels]

    # 통일 팔레트 적용 — METRIC_COLORS (revenue/orders/customers) 기준
    # 시트 전용 채널은 회색 톤 (데이터 불완전 명시)
    rev_colors = ["#cbd5e1" if sh else METRIC_COLORS["revenue"] for sh in is_sheet]
    ord_colors = ["#cbd5e1" if sh else METRIC_COLORS["orders"] for sh in is_sheet]
    cus_colors = ["#cbd5e1" if sh else METRIC_COLORS["customers"] for sh in is_sheet]

    st.divider()
    st.markdown(f"##### 📊 {brand} 채널별 성과 비교 (매출 · 주문 · 고객)")
    st.caption(
        "시트 기반 채널(회색)은 API 미연동으로 매출만 집계되며 주문/고객 수치는 0 으로 표시."
    )

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=("매출 (원)", "주문 (건)", "고객 (명)"),
        horizontal_spacing=0.08,
    )
    fig.add_trace(
        go.Bar(
            x=labels, y=revenues, marker_color=rev_colors, name="매출",
            text=[f"{v:,}" for v in revenues], textposition="outside",
            hovertemplate="<b>%{x}</b><br>매출 %{y:,}원<extra></extra>",
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Bar(
            x=labels, y=orders_cnt, marker_color=ord_colors, name="주문",
            text=[f"{v:,}" if v > 0 else "—" for v in orders_cnt],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>주문 %{y:,}건<extra></extra>",
        ),
        row=1, col=2,
    )
    fig.add_trace(
        go.Bar(
            x=labels, y=customers_cnt, marker_color=cus_colors, name="고객",
            text=[f"{v:,}" if v > 0 else "—" for v in customers_cnt],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>고객 %{y:,}명<extra></extra>",
        ),
        row=1, col=3,
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=50, b=40),
        showlegend=False,
        plot_bgcolor="white",
        font=dict(family="Pretendard, -apple-system, sans-serif", size=12),
    )
    fig.update_xaxes(tickangle=-20, tickfont=dict(size=10))
    fig.update_yaxes(gridcolor="#f1f5f9", tickformat=",")
    # Subplot 제목 폰트 통일
    for ann in fig.layout.annotations:
        ann.font = dict(size=13, color=TEXT_MAIN)
    st.plotly_chart(
        fig, width="stretch",
        key=f"channel_compare_{brand}",
    )


def _render_sheet_only_channel_card(
    sub_df: pd.DataFrame, channel: str, sub: str, brand: str,
):
    """시트 전용 채널 카드 (쿠팡 로켓배송/무신사/오프라인 등)."""
    total_actual = int(sub_df["actual"].sum())
    total_target = int(sub_df["target"].sum())
    pct = (total_actual / total_target * 100) if total_target else 0
    days_with_data = int((sub_df["actual"] > 0).sum())

    with st.container(border=True):
        col_info, col_chart, col_action = st.columns([2, 2, 3])
        with col_info:
            st.markdown(
                f"<h3 style='margin:0 0 2px 0; font-size:1.15rem; "
                f"font-weight:700; color:#0f172a; "
                f"white-space:nowrap; overflow:hidden; "
                f"text-overflow:ellipsis;' title='{channel}'>{channel}</h3>",
                unsafe_allow_html=True,
            )
            st.caption(sub)
            st.metric("이번 기간 매출", f"{total_actual:,}원")
            mc1, mc2 = st.columns(2)
            mc1.metric("목표", f"{total_target:,}원" if total_target else "—")
            mc2.metric(
                "달성률",
                f"{pct:.0f}%" if total_target else "—",
                delta=(
                    f"{pct - 100:+.0f}%p vs 목표"
                    if total_target else None
                ),
            )
            st.caption(f"기록 일수 {days_with_data}/{len(sub_df)}일")

        with col_chart:
            st.markdown("**일별 매출·목표 추이**")
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=sub_df["date"], y=sub_df["actual"],
                mode="lines+markers",
                line=dict(color="#2563eb", width=2),
                marker=dict(size=5),
                name="실적",
                hovertemplate="%{x|%m/%d}<br>%{y:,.0f}원<extra></extra>",
            ))
            if total_target:
                fig.add_trace(go.Scatter(
                    x=sub_df["date"], y=sub_df["target"],
                    mode="lines",
                    line=dict(color="#dc2626", width=1.5, dash="dash"),
                    name="목표",
                    hovertemplate="%{x|%m/%d}<br>목표 %{y:,.0f}원<extra></extra>",
                ))
            fig.update_layout(
                height=200, margin=dict(l=10, r=10, t=10, b=10),
                showlegend=True,
                legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                            font=dict(size=10)),
                plot_bgcolor="white",
                xaxis=dict(showgrid=False, tickformat="%m/%d"),
                yaxis=dict(gridcolor="#f1f5f9", tickformat=","),
            )
            st.plotly_chart(
                fig, width="stretch",
                key=f"sheet_ch_{brand}_{channel}",
            )

        with col_action:
            st.markdown("**요약**")
            note_lines = [
                f"📊 **구글 시트 기반 집계** — 팀 직접 입력",
                f"• 매출: **{total_actual:,}원** / 목표: {total_target:,}원",
                f"• 달성률: **{pct:.0f}%**",
            ]
            if total_target:
                gap = total_target - total_actual
                if pct >= 100:
                    st.success(
                        f"✨ **목표 초과 달성** ({pct:.0f}%)\n\n"
                        f"잔여 기간 동력 유지 — 재고/마케팅 점검"
                    )
                elif pct >= 70:
                    st.info(
                        f"📈 **목표 근접 중** ({pct:.0f}%)\n\n"
                        f"목표까지 {gap:,}원 · 남은 기간 속도 조정"
                    )
                else:
                    st.warning(
                        f"⚠️ **목표 미달 우려** ({pct:.0f}%)\n\n"
                        f"목표까지 {gap:,}원 부족 · 프로모션/노출 강화 필요"
                    )
            else:
                st.caption("\n\n".join(note_lines))


# ==========================================================
# 브랜드 탭
# ==========================================================
tab_all, tab_ddok, tab_rolla, tab_ruti = st.tabs([
    "📊 전체",
    "🍙 똑똑연구소",
    "🧳 롤라루",
    "👟 루티니스트",
])

with tab_all:
    st.caption("전체 브랜드 합산 매출 · 모든 스토어 카드 + 시트 기반 기간 합계는 각 브랜드 탭 참조")
    render_sales_overview(
        orders, start_date, pd.Timestamp(end_date),
        prev_start, prev_end, brand=None,
    )

with tab_ddok:
    render_brand_banner(
        "똑똑연구소",
        "네이버 스마트스토어 · 자사몰 · 쿠팡 로켓그로스",
    )
    render_sales_overview(
        filter_orders_by_brand(orders, "똑똑연구소"),
        start_date, pd.Timestamp(end_date),
        prev_start, prev_end, brand="똑똑연구소",
    )

with tab_rolla:
    render_brand_banner(
        "롤라루",
        "자사몰 · 네이버 스마트스토어 · 쿠팡 로켓배송 · 쿠팡 판매자 판매 · 무신사 · 오프라인 · 이지웰 · 오늘의집",
    )
    render_sales_overview(
        filter_orders_by_brand(orders, "롤라루"),
        start_date, pd.Timestamp(end_date),
        prev_start, prev_end, brand="롤라루",
    )

with tab_ruti:
    render_brand_banner(
        "루티니스트",
        "자사몰 · 네이버 스마트스토어 (API 미연동 · 구글 시트 기반 집계)",
    )
    # 루티니스트는 orders.csv 가 없으므로 시트 데이터만 사용
    try:
        sheet_df_r = _cached_sheet_sales()
        ruti_data = sheet_df_r[
            (sheet_df_r["brand"] == "루티니스트")
            & (sheet_df_r["date"] >= start_date)
            & (sheet_df_r["date"] <= pd.Timestamp(end_date))
        ]

        if ruti_data.empty:
            st.info("루티니스트: 선택 기간 시트 데이터 없음.")
        else:
            # ---------- 매출 요약 KPI 4개 ----------
            total_target = int(ruti_data["target"].sum())
            total_actual = int(ruti_data["actual"].sum())
            pct = (total_actual / total_target * 100) if total_target else 0
            n_days = ruti_data["date"].nunique()
            daily = (
                ruti_data.groupby(ruti_data["date"].dt.date)["actual"]
                .sum().reset_index()
            )
            avg_daily = int(daily["actual"].mean()) if not daily.empty else 0
            best_day = (
                daily.loc[daily["actual"].idxmax()]
                if not daily.empty else None
            )

            st.markdown("#### 📈 루티니스트 매출 요약")
            k1, k2, k3, k4 = st.columns(4)

            k1.markdown(
                kpi_card(
                    "기간 매출",
                    format_won_compact(total_actual),
                    sub=f"{total_actual:,}원",
                    value_color="#2563eb",
                ),
                unsafe_allow_html=True,
            )
            k2.markdown(
                kpi_card(
                    "기간 목표",
                    format_won_compact(total_target),
                    sub=f"{total_target:,}원",
                ),
                unsafe_allow_html=True,
            )
            from utils.ui import status_color as _sc
            pct_color, _, pct_label = _sc(pct)
            k3.markdown(
                kpi_card(
                    "달성률",
                    f"{pct:.0f}%",
                    sub=pct_label,
                    value_color=pct_color,
                ),
                unsafe_allow_html=True,
            )
            k4.markdown(
                kpi_card(
                    "일 평균 매출",
                    format_won_compact(avg_daily),
                    sub=f"{n_days}일 기록",
                ),
                unsafe_allow_html=True,
            )

            # ---------- 채널별 카드 ----------
            st.divider()
            st.markdown("#### 🏪 루티니스트 채널별 성과 (시트 기준)")

            channel_agg = (
                ruti_data.groupby("channel")
                .agg(target=("target", "sum"), actual=("actual", "sum"))
                .reset_index()
            )
            channel_agg["pct"] = (
                channel_agg["actual"]
                / channel_agg["target"].replace(0, pd.NA)
                * 100
            ).fillna(0).astype(int)
            channel_agg = channel_agg.sort_values("target", ascending=False)

            ch_cols = st.columns(max(len(channel_agg), 1))
            for i, (_, row) in enumerate(channel_agg.iterrows()):
                if i >= len(ch_cols):
                    break
                ch_pct = row["pct"]
                ch_color, _, ch_label = _sc(ch_pct)
                with ch_cols[i]:
                    with st.container(border=True):
                        st.markdown(
                            f"<div style='color:#64748b; font-size:0.82rem; "
                            f"margin-bottom:4px;'>{row['channel']}</div>",
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f"<div style='font-size:1.3rem; font-weight:700; "
                            f"color:#0f172a;'>{format_won_compact(int(row['actual']))}</div>",
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f"<div style='color:#475569; font-size:0.82rem; "
                            f"margin-top:2px;'>목표 {format_won_compact(int(row['target']))}</div>",
                            unsafe_allow_html=True,
                        )
                        # 진도 바
                        bar_pct = min(ch_pct, 120) / 120 * 100
                        st.markdown(
                            f"<div style='background:#f1f5f9; height:8px; "
                            f"border-radius:4px; margin-top:8px; overflow:hidden;'>"
                            f"<div style='width:{bar_pct}%; height:100%; "
                            f"background:{ch_color};'></div></div>"
                            f"<div style='text-align:right; font-size:0.75rem; "
                            f"font-weight:700; color:{ch_color}; margin-top:4px;'>"
                            f"{ch_pct}% · {ch_label}</div>",
                            unsafe_allow_html=True,
                        )

            # ---------- 일별 달성표 ----------
            st.divider()
            _render_daily_achievement(
                sheet_df_r, start_date, pd.Timestamp(end_date), "루티니스트",
            )
    except Exception as e:
        st.warning(f"시트 로드 실패: {type(e).__name__}: {e}")


# ==========================================================
# 재구매 안내 + 기준 요약
# ==========================================================
st.divider()
st.info(
    "**CRM 리마인더 대상 목록, 이탈 고객, 채널별 재구매 패턴 상세**는 "
    "왼쪽 사이드바의 **👥 CRM** 페이지에서 브랜드/스토어 필터와 함께 확인하세요."
)

with st.expander("우리 기준 (현재 값)"):
    st.markdown(f"""
- **재구매 주기**: {THRESHOLDS['repurchase_cycle_days']}일 (이후 CRM 대상)
- **주문 감소 경고**: 전 기간 대비 {THRESHOLDS['orders_drop_alert'] * 100:.0f}% 이하
- **주문 성장 기회**: 전 기간 대비 +{THRESHOLDS['orders_grow_alert'] * 100:.0f}% 이상
- **재구매율 건강선**: {THRESHOLDS['retention_healthy_pct']}% 이상, 우려선 {THRESHOLDS['retention_low_pct']}% 미만
- **탑 20% 집중도 경고**: 상위 20% 고객이 매출의 {THRESHOLDS['top20_concentration_warn']}% 이상일 때

기준 조정은 `utils/actions.py`의 `THRESHOLDS`.
""")
