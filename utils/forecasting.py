"""가중 통계법 기반 매출 예측 + 캠페인-제품 상관 분석.

핵심 원칙:
    ❌ 산술평균 (actual/days_passed × total_days) — 요일 편차 무시
    ✅ 가중 통계법:
        1. 지수 감쇄 (exponential decay) — 최근 관측 가중
        2. 요일 계절성 (day-of-week seasonality) — 주중/주말 편차
        3. 추세 보정 (EWMA trend) — 최근 상승/하락 반영
        4. 신뢰구간 (confidence band) — 가중 분산 기반 ±σ

수식:
    w_i = exp(-Δt_i / τ)   (Δt_i: today 까지 일수, τ: half-life / ln2)
    weekday_avg[wd] = Σ(actual_i × w_i) / Σ(w_i) for wd
    forecast_day(d) = weekday_avg[d.weekday()] × trend_mult
    projected_total = actual_so_far + Σ forecast_day(d) for d in remaining
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd


# ==========================================================
# 1. 월말 매출 예측 (가중 통계법)
# ==========================================================
def weighted_month_end_forecast(
    daily_df: pd.DataFrame,
    today: pd.Timestamp,
    month_end: pd.Timestamp | None = None,
    history_weeks: int = 8,
    half_life_days: float = 14.0,
    trend_window: int = 7,
    trend_baseline: int = 28,
) -> dict:
    """가중 통계법으로 월말 매출 예측.

    Args:
        daily_df: columns=[date, actual] — 일별 매출 원천
        today: 예측 기준일 (오늘)
        month_end: 월말 Timestamp (None 이면 today 기준 해당 월 말일)
        history_weeks: 학습 과거 주수 (8주 = 56일)
        half_life_days: 지수 감쇄 half-life (14일 → 14일 전 관측은 가중치 절반)
        trend_window: 최근 추세 계산 윈도 (7일)
        trend_baseline: 추세 기준선 윈도 (28일 평균 대비)

    Returns:
        {
            'actual_so_far': int,         # 경과일까지 실제 매출
            'forecast_remaining': int,    # 남은 일수 예측 합계
            'projected_total': int,       # 월말 예상 총합 (point estimate)
            'projected_low': int,         # 보수적 추정 (-1σ)
            'projected_high': int,        # 낙관적 추정 (+1σ)
            'days_passed': int,
            'days_remaining': int,
            'trend_multiplier': float,    # 1.0=정상 / >1=상승세 / <1=하락세
            'weekday_baseline': dict,     # {0: 평균, 1: ... 6: ...} (debug용)
            'confidence_std': int,        # 가중 분산의 표준편차
            'method': str,                # 사용된 방법 요약
        }
    """
    if daily_df.empty or "actual" not in daily_df.columns:
        return _empty_forecast()

    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.groupby("date", as_index=False)["actual"].sum()
    df = df.sort_values("date")

    today = pd.Timestamp(today)
    if month_end is None:
        month_end = pd.Timestamp(
            today.replace(day=1) + pd.offsets.MonthEnd(0)
        )
    month_start = pd.Timestamp(today.replace(day=1))

    # ---- 경과 실적 ----
    current_month = df[
        (df["date"] >= month_start) & (df["date"] <= today)
    ]
    actual_so_far = int(current_month["actual"].sum())
    days_passed = int((today - month_start).days) + 1
    days_remaining = int((month_end - today).days)
    if days_remaining <= 0:
        # 월말 도달 — 실적 확정
        return {
            "actual_so_far": actual_so_far,
            "forecast_remaining": 0,
            "projected_total": actual_so_far,
            "projected_low": actual_so_far,
            "projected_high": actual_so_far,
            "days_passed": days_passed,
            "days_remaining": 0,
            "trend_multiplier": 1.0,
            "weekday_baseline": {},
            "confidence_std": 0,
            "method": "월 마감 — 실적 확정",
        }

    # ---- 과거 N주 학습 ----
    history_start = today - pd.Timedelta(days=history_weeks * 7)
    history = df[(df["date"] >= history_start) & (df["date"] <= today)].copy()
    if len(history) < 7:
        # 히스토리 부족 → 단순 평균 fallback
        simple_avg = actual_so_far / max(days_passed, 1)
        forecast_remaining = int(simple_avg * days_remaining)
        return {
            "actual_so_far": actual_so_far,
            "forecast_remaining": forecast_remaining,
            "projected_total": actual_so_far + forecast_remaining,
            "projected_low": int((actual_so_far + forecast_remaining) * 0.85),
            "projected_high": int((actual_so_far + forecast_remaining) * 1.15),
            "days_passed": days_passed,
            "days_remaining": days_remaining,
            "trend_multiplier": 1.0,
            "weekday_baseline": {},
            "confidence_std": 0,
            "method": "데이터 부족 — 단순 평균",
        }

    # ---- 지수 감쇄 가중치 ----
    history["days_ago"] = (today - history["date"]).dt.days
    # τ = half_life_days / ln(2) — half_life 일 전 관측 가중치가 절반
    tau = half_life_days / math.log(2)
    history["weight"] = np.exp(-history["days_ago"] / tau)

    # ---- 요일별 가중평균 ----
    history["weekday"] = history["date"].dt.weekday
    weekday_baseline: dict[int, float] = {}
    weekday_var: dict[int, float] = {}
    for wd, group in history.groupby("weekday"):
        total_w = group["weight"].sum()
        if total_w <= 0:
            continue
        weighted_mean = (group["actual"] * group["weight"]).sum() / total_w
        weekday_baseline[int(wd)] = float(weighted_mean)
        # 가중 분산
        weighted_var = (
            (group["weight"] * (group["actual"] - weighted_mean) ** 2).sum()
            / total_w
        )
        weekday_var[int(wd)] = float(weighted_var)

    # 요일별 누락 보정 — 전체 가중평균으로 fallback
    overall_mean = (
        (history["actual"] * history["weight"]).sum() / history["weight"].sum()
    )
    overall_var = (
        (history["weight"] * (history["actual"] - overall_mean) ** 2).sum()
        / history["weight"].sum()
    )
    for wd in range(7):
        if wd not in weekday_baseline:
            weekday_baseline[wd] = float(overall_mean)
            weekday_var[wd] = float(overall_var)

    # ---- 추세 승수 (최근 N일 vs 기준 M일 가중평균 비율) ----
    recent = history[history["days_ago"] <= trend_window]
    baseline = history[history["days_ago"] <= trend_baseline]
    if not recent.empty and not baseline.empty:
        recent_w_avg = (
            (recent["actual"] * recent["weight"]).sum()
            / max(recent["weight"].sum(), 1e-9)
        )
        baseline_w_avg = (
            (baseline["actual"] * baseline["weight"]).sum()
            / max(baseline["weight"].sum(), 1e-9)
        )
        trend_mult = recent_w_avg / baseline_w_avg if baseline_w_avg > 0 else 1.0
        # 극단값 방지 — [0.5, 2.0] 클램프
        trend_mult = max(0.5, min(2.0, trend_mult))
    else:
        trend_mult = 1.0

    # ---- 남은 일자 예측 ----
    remaining_days = pd.date_range(
        today + pd.Timedelta(days=1), month_end, freq="D",
    )
    forecast_sum = 0.0
    var_sum = 0.0
    for day in remaining_days:
        wd = int(day.weekday())
        day_forecast = weekday_baseline[wd] * trend_mult
        forecast_sum += day_forecast
        # 분산 합산 (독립 가정) — 신뢰구간 계산용
        var_sum += weekday_var[wd] * (trend_mult ** 2)

    forecast_remaining = int(forecast_sum)
    projected_total = actual_so_far + forecast_remaining
    # 신뢰구간 (±1σ 근사 — 65% 구간)
    confidence_std = int(math.sqrt(var_sum))
    projected_low = max(0, projected_total - confidence_std)
    projected_high = projected_total + confidence_std

    return {
        "actual_so_far": actual_so_far,
        "forecast_remaining": forecast_remaining,
        "projected_total": projected_total,
        "projected_low": projected_low,
        "projected_high": projected_high,
        "days_passed": days_passed,
        "days_remaining": days_remaining,
        "trend_multiplier": round(trend_mult, 3),
        "weekday_baseline": {int(k): int(v) for k, v in weekday_baseline.items()},
        "confidence_std": confidence_std,
        "method": (
            f"EWMA(τ={half_life_days:.0f}일) × "
            f"요일 계절성({history_weeks}주) × "
            f"추세 보정({trend_window}/{trend_baseline}일)"
        ),
    }


def _empty_forecast() -> dict:
    return {
        "actual_so_far": 0,
        "forecast_remaining": 0,
        "projected_total": 0,
        "projected_low": 0,
        "projected_high": 0,
        "days_passed": 0,
        "days_remaining": 0,
        "trend_multiplier": 1.0,
        "weekday_baseline": {},
        "confidence_std": 0,
        "method": "데이터 없음",
    }


# ==========================================================
# 2. 캠페인 → 제품 연결 상관 분석 (가중 Pearson)
# ==========================================================
def weighted_pearson(x: np.ndarray, y: np.ndarray, weights: np.ndarray) -> float:
    """가중 Pearson 상관계수.

    ρ_w = Σ w(x-μ_x)(y-μ_y) / √(Σw(x-μ_x)² × Σw(y-μ_y)²)
    """
    w = weights / weights.sum() if weights.sum() > 0 else weights
    mx = (w * x).sum()
    my = (w * y).sum()
    dx = x - mx
    dy = y - my
    cov = (w * dx * dy).sum()
    var_x = (w * dx ** 2).sum()
    var_y = (w * dy ** 2).sum()
    denom = math.sqrt(var_x * var_y) if (var_x > 0 and var_y > 0) else 0
    if denom == 0:
        return 0.0
    return float(cov / denom)


def campaign_product_correlation(
    ads_daily: pd.DataFrame,
    orders_daily: pd.DataFrame,
    today: pd.Timestamp,
    half_life_days: float = 14.0,
    min_days: int = 14,
) -> pd.DataFrame:
    """(campaign, product) 쌍의 일별 가중 상관계수 산출.

    Args:
        ads_daily: columns=[date, campaign_name, spend]
        orders_daily: columns=[date, product, revenue]
        today: 기준일
        half_life_days: 최근 관측 가중 half-life
        min_days: 최소 공통 관측일

    Returns:
        DataFrame[campaign_name, product, corr, joint_days, total_spend, total_rev]
        corr 내림차순 정렬 (높은 양의 상관 = attribution 가능성 큼)
    """
    if ads_daily.empty or orders_daily.empty:
        return pd.DataFrame()

    today = pd.Timestamp(today)
    ads_d = ads_daily.copy()
    ord_d = orders_daily.copy()
    for df in (ads_d, ord_d):
        df["date"] = pd.to_datetime(df["date"])

    # 가중치 준비 (최근 관측 가중)
    tau = half_life_days / math.log(2)

    results = []
    for campaign, c_df in ads_d.groupby("campaign_name"):
        c_daily = c_df.groupby("date")["spend"].sum().reset_index()
        total_spend = int(c_daily["spend"].sum())
        if total_spend < 10_000:   # 노이즈 제거 — 광고비 1만원 미만 캠페인 skip
            continue

        for product, p_df in ord_d.groupby("product"):
            p_daily = p_df.groupby("date")["revenue"].sum().reset_index()

            # 공통 날짜 join
            joined = c_daily.merge(p_daily, on="date", how="inner")
            if len(joined) < min_days:
                continue

            joined["days_ago"] = (today - joined["date"]).dt.days
            joined["weight"] = np.exp(-joined["days_ago"] / tau)

            corr = weighted_pearson(
                joined["spend"].values,
                joined["revenue"].values,
                joined["weight"].values,
            )
            total_rev = int(joined["revenue"].sum())

            results.append({
                "campaign_name": campaign,
                "product": product,
                "corr": round(corr, 3),
                "joint_days": len(joined),
                "total_spend": total_spend,
                "total_rev": total_rev,
            })

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("corr", ascending=False).reset_index(drop=True)
    return df


# ==========================================================
# 3. 일별 가중 이동평균 (차트 overlay 용)
# ==========================================================
def weighted_moving_average(
    series: pd.Series,
    window: int = 7,
    alpha: float = 0.3,
) -> pd.Series:
    """지수 가중 이동평균 (EWMA).

    Args:
        series: 일별 수치
        window: 최소 관측 수
        alpha: smoothing factor (0-1, 클수록 최근 비중 ↑)

    Returns:
        같은 인덱스의 EWMA 값
    """
    return series.ewm(alpha=alpha, min_periods=max(1, window // 2)).mean()
