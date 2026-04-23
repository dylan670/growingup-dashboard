"""룰 기반 자동 인사이트 생성기.

AI 호출 없이 데이터에서 바로 드러나는 '사실' 중심의 요약 카드를 생성합니다.

철학:
  - 추정·의견 X → 측정 가능한 수치만
  - 방향성 명확할 때만 (임계값 넘은 경우만 노출)
  - 의사결정에 직접 도움되는 액션 포함

각 인사이트 dict:
  {
    "icon": "📈",
    "title": "...",                   # 한 줄 헤드라인
    "detail": "...",                  # 1~2문장 세부
    "severity": "info" | "good" | "warning" | "urgent",
    "priority": int                   # 정렬용 (낮을수록 상단)
  }
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date

import pandas as pd

from utils.products import (
    filter_ads_by_brand, filter_orders_by_brand,
    BRAND_MONTHLY_TARGETS,
)
from utils.metrics import calc_repurchase


# ==========================================================
# 개별 인사이트 생성기 (각 함수 → list[dict])
# ==========================================================

def _pace_forecast(sheet_df: pd.DataFrame, today: date) -> list[dict]:
    """이번 달 페이스 예측 — 남은 일수로 예상 달성률 계산."""
    results: list[dict] = []
    if sheet_df.empty:
        return results

    month_start = today.replace(day=1)
    _, last_day = monthrange(today.year, today.month)
    days_elapsed = today.day
    days_remaining = last_day - today.day

    month_data = sheet_df[
        (sheet_df["date"] >= pd.Timestamp(month_start))
        & (sheet_df["date"] <= pd.Timestamp(today))
    ]

    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        b = month_data[month_data["brand"] == brand]
        if b.empty:
            continue
        actual_so_far = int(b["actual"].sum())
        target_month = int(
            sheet_df[
                (sheet_df["brand"] == brand)
                & (sheet_df["date"].dt.year == today.year)
                & (sheet_df["date"].dt.month == today.month)
            ]["target"].sum()
        )
        if target_month == 0 or days_elapsed == 0:
            continue

        daily_pace = actual_so_far / days_elapsed
        projected = daily_pace * last_day
        projected_pct = projected / target_month * 100

        if projected_pct >= 105:
            severity = "good"
            icon = "🚀"
            title = f"{brand} — 이번 달 목표 초과 달성 예상 ({projected_pct:.0f}%)"
        elif projected_pct >= 90:
            severity = "info"
            icon = "🎯"
            title = f"{brand} — 이번 달 목표 근접 예상 ({projected_pct:.0f}%)"
        elif projected_pct >= 60:
            severity = "warning"
            icon = "⚠️"
            title = f"{brand} — 이번 달 목표 미달 예상 ({projected_pct:.0f}%)"
        else:
            severity = "urgent"
            icon = "🚨"
            title = f"{brand} — 이번 달 목표 큰 폭 미달 예상 ({projected_pct:.0f}%)"

        detail = (
            f"현재 {days_elapsed}일차 실적 {actual_so_far / 10_000:,.0f}만원 "
            f"(일 평균 {daily_pace / 10_000:,.0f}만원). "
            f"남은 {days_remaining}일 같은 페이스 유지 시 월말 예상 "
            f"{projected / 10_000:,.0f}만원 · 목표 {target_month / 10_000:,.0f}만원."
        )
        results.append({
            "icon": icon, "title": title, "detail": detail,
            "severity": severity, "priority": 10,
        })

    return results


def _top_contributor(sheet_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    """기간 내 최고 기여 브랜드·채널 — 매출 비중."""
    sub = sheet_df[(sheet_df["date"] >= start) & (sheet_df["date"] <= end)]
    if sub.empty or sub["actual"].sum() == 0:
        return []

    total = sub["actual"].sum()
    by_brand = sub.groupby("brand")["actual"].sum().sort_values(ascending=False)
    if by_brand.iloc[0] == 0:
        return []

    top_brand = by_brand.index[0]
    top_brand_pct = by_brand.iloc[0] / total * 100

    results: list[dict] = []
    if top_brand_pct >= 50:
        results.append({
            "icon": "👑",
            "title": f"{top_brand}가 전체 매출의 {top_brand_pct:.0f}% 견인 중",
            "detail": (
                f"기간 총 매출 ₩{total / 100_000_000:.1f}억 중 "
                f"{top_brand} ₩{by_brand.iloc[0] / 100_000_000:.1f}억 기여. "
                f"단일 브랜드 의존도 높음 — 다른 브랜드 성장 레버리지 검토 권장."
            ),
            "severity": "info",
            "priority": 30,
        })

    # 특정 브랜드 내 최고 채널
    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        b = sub[sub["brand"] == brand]
        if b.empty or b["actual"].sum() == 0:
            continue
        by_ch = b.groupby("channel")["actual"].sum().sort_values(ascending=False)
        if by_ch.iloc[0] == 0:
            continue
        top_ch = by_ch.index[0]
        top_ch_pct = by_ch.iloc[0] / b["actual"].sum() * 100
        if top_ch_pct >= 70:
            results.append({
                "icon": "🎯",
                "title": f"{brand} 매출의 {top_ch_pct:.0f}%가 {top_ch} 단일 채널",
                "detail": (
                    f"{brand} 기간 매출 ₩{b['actual'].sum() / 10_000:,.0f}만원 중 "
                    f"{top_ch} ₩{by_ch.iloc[0] / 10_000:,.0f}만원. "
                    f"채널 리스크 분산 위해 2위 채널 성장 전략 필요."
                ),
                "severity": "warning",
                "priority": 40,
            })

    return results


def _worst_channel(sheet_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    """목표 대비 달성률 최악 채널 (목표 있는데 실적 낮음)."""
    sub = sheet_df[(sheet_df["date"] >= start) & (sheet_df["date"] <= end)]
    if sub.empty:
        return []

    results: list[dict] = []
    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        b = sub[sub["brand"] == brand]
        if b.empty:
            continue
        by_ch = (
            b.groupby("channel")
            .agg(target=("target", "sum"), actual=("actual", "sum"))
            .reset_index()
        )
        by_ch = by_ch[by_ch["target"] > 0]
        if by_ch.empty:
            continue
        by_ch["pct"] = by_ch["actual"] / by_ch["target"] * 100
        worst = by_ch.sort_values("pct").iloc[0]
        if worst["pct"] < 15 and worst["target"] >= 1_000_000:
            results.append({
                "icon": "🚨",
                "title": (
                    f"{brand} {worst['channel']} 달성률 {worst['pct']:.0f}% — 긴급 점검"
                ),
                "detail": (
                    f"기간 목표 ₩{worst['target'] / 10_000:,.0f}만원 대비 "
                    f"실적 ₩{worst['actual'] / 10_000:,.0f}만원. "
                    f"채널 운영 중단/축소 또는 문제 원인 파악 필요."
                ),
                "severity": "urgent",
                "priority": 5,
            })

    return results


def _ad_efficiency(ads_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    """광고 효율 인사이트 — 최고·최저 ROAS 채널."""
    period = ads_df[(ads_df["date"] >= start) & (ads_df["date"] <= end)]
    if period.empty:
        return []

    results: list[dict] = []

    # 브랜드별 통합 ROAS 최고
    for brand in ["똑똑연구소", "롤라루"]:
        b = filter_ads_by_brand(ads_df, brand)
        bp = b[(b["date"] >= start) & (b["date"] <= end)]
        if bp.empty:
            continue
        spend = int(bp["spend"].sum())
        rev = int(bp["revenue"].sum())
        if spend == 0:
            continue
        roas = rev / spend * 100
        if roas >= 300:
            results.append({
                "icon": "✨",
                "title": f"{brand} 광고 ROAS {roas:.0f}% — 예산 증액 여지",
                "detail": (
                    f"기간 광고비 ₩{spend / 10_000:,.0f}만원 → "
                    f"매출 ₩{rev / 10_000:,.0f}만원. "
                    f"광고비 1원당 매출 ₩{roas / 100:.1f}. "
                    f"과거 추이 유지 확인 후 증액 검토."
                ),
                "severity": "good",
                "priority": 20,
            })
        elif roas < 50 and spend >= 500_000:
            results.append({
                "icon": "🚨",
                "title": f"{brand} 광고 ROAS {roas:.0f}% — 손실 상태",
                "detail": (
                    f"기간 광고비 ₩{spend / 10_000:,.0f}만원 집행했으나 "
                    f"매출 ₩{rev / 10_000:,.0f}만원 회수. "
                    f"캠페인 일시정지 또는 소재 전면 교체 권장."
                ),
                "severity": "urgent",
                "priority": 5,
            })

    return results


def _crm_opportunity(orders_df: pd.DataFrame, today: date) -> list[dict]:
    """재구매 타이밍 고객 — CRM 기회."""
    cust = calc_repurchase(orders_df, cycle_days=30)
    reached = cust[cust["cycle_reached"]]
    if len(reached) == 0:
        return []

    avg_rev = reached["total_revenue"].mean()
    expected_30 = int(avg_rev * len(reached) * 0.3)

    severity = "good" if len(reached) >= 100 else "info"
    icon = "💌" if severity == "good" else "👥"

    return [{
        "icon": icon,
        "title": f"재구매 타이밍 고객 {len(reached):,}명 — 예상 매출 ₩{expected_30 / 10_000:,.0f}만원",
        "detail": (
            f"최근 주문 30~60일 경과 고객 {len(reached):,}명. "
            f"평균 객단가 ₩{avg_rev / 10_000:,.0f}만원 × 30% 전환 가정. "
            f"CRM 페이지에서 CSV 다운로드 → 알림톡/이메일 발송 가능."
        ),
        "severity": severity,
        "priority": 15,
    }]


def _uncollected_target(sheet_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    """목표는 있는데 실적 0인 채널 (운영 공백 감지)."""
    sub = sheet_df[(sheet_df["date"] >= start) & (sheet_df["date"] <= end)]
    if sub.empty:
        return []

    by_bc = (
        sub.groupby(["brand", "channel"])
        .agg(target=("target", "sum"), actual=("actual", "sum"))
        .reset_index()
    )
    zero_actual = by_bc[
        (by_bc["target"] >= 1_000_000)  # 유의미한 목표
        & (by_bc["actual"] == 0)
    ]
    if zero_actual.empty:
        return []

    # 채널별 리스트
    ch_list = [
        f"{row['brand']} {row['channel']} (목표 ₩{row['target'] / 10_000:,.0f}만)"
        for _, row in zero_actual.iterrows()
    ]

    return [{
        "icon": "🕳️",
        "title": f"실적 0원 채널 {len(zero_actual)}개 — 운영 공백 가능성",
        "detail": (
            f"{', '.join(ch_list[:3])}"
            + (f" 외 {len(ch_list) - 3}개" if len(ch_list) > 3 else "")
            + ". 데이터 미입력 또는 채널 폐쇄 상태 확인 필요."
        ),
        "severity": "warning",
        "priority": 25,
    }]


# ==========================================================
# 메인 집계 함수
# ==========================================================
def generate_insights(
    sheet_df: pd.DataFrame,
    ads_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    max_count: int = 6,
) -> list[dict]:
    """모든 룰 돌려서 상위 N개 인사이트 반환.

    우선순위 낮은 것(urgent·critical)이 위로 오도록 정렬.
    """
    today = end.date()

    all_insights: list[dict] = []
    all_insights.extend(_worst_channel(sheet_df, start, end))
    all_insights.extend(_ad_efficiency(ads_df, start, end))
    all_insights.extend(_pace_forecast(sheet_df, today))
    all_insights.extend(_crm_opportunity(orders_df, today))
    all_insights.extend(_top_contributor(sheet_df, start, end))
    all_insights.extend(_uncollected_target(sheet_df, start, end))

    # 우선순위 순 (낮은 번호 = 먼저)
    all_insights.sort(key=lambda x: x.get("priority", 100))
    return all_insights[:max_count]


# ==========================================================
# 렌더링 스타일 매핑 (severity → 카드 컬러)
# ==========================================================
SEVERITY_STYLES = {
    "urgent":  {"bg": "#fef2f2", "border": "#dc2626", "label_bg": "#fee2e2", "label_color": "#991b1b", "label_text": "긴급"},
    "warning": {"bg": "#fffbeb", "border": "#f59e0b", "label_bg": "#fef3c7", "label_color": "#92400e", "label_text": "주의"},
    "good":    {"bg": "#f0fdf4", "border": "#16a34a", "label_bg": "#dcfce7", "label_color": "#166534", "label_text": "기회"},
    "info":    {"bg": "#eff6ff", "border": "#2563eb", "label_bg": "#dbeafe", "label_color": "#1e40af", "label_text": "참고"},
}
