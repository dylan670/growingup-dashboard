"""지표 계산 로직."""
import numpy as np
import pandas as pd


TARGET_ROAS = {
    "네이버": 4.0,
    "쿠팡": 5.0,
    "자사몰": 2.5,
}

# 월 매출 목표 (그로잉업팀 전체 / 2026년 기준)
# 현재 90일 실적 75,353,270원 = 월 약 2,500만원 → 목표 월 3,000만원 설정
MONTHLY_REVENUE_TARGET = 30_000_000


def calc_target_achievement(
    revenue: float, days_elapsed: int,
    monthly_target: int = MONTHLY_REVENUE_TARGET,
) -> dict:
    """선택 기간의 매출 목표 달성률.

    Args:
        revenue: 해당 기간 실제 매출 합계
        days_elapsed: 해당 기간 일수
    Returns:
        dict(pro_rated_target, achievement_pct, daily_pace)
    """
    if days_elapsed <= 0:
        return {"pro_rated_target": 0, "achievement_pct": 0, "daily_pace": 0}
    pro_rated = monthly_target * days_elapsed / 30
    pct = (revenue / pro_rated * 100) if pro_rated > 0 else 0
    daily_pace = revenue / days_elapsed if days_elapsed > 0 else 0
    return {
        "pro_rated_target": pro_rated,
        "achievement_pct": pct,
        "daily_pace": daily_pace,
    }


def _filter_period(df: pd.DataFrame, start=None, end=None, col: str = "date") -> pd.DataFrame:
    if start is not None:
        df = df[df[col] >= pd.Timestamp(start)]
    if end is not None:
        df = df[df[col] <= pd.Timestamp(end)]
    return df


def _safe_div(num: pd.Series, denom: pd.Series) -> pd.Series:
    """0으로 나누기 방지. denom=0 인 위치는 NaN."""
    return num.astype(float) / denom.replace(0, np.nan).astype(float)


def calc_channel_metrics(ads_df: pd.DataFrame, orders_df: pd.DataFrame = None,
                         start_date=None, end_date=None) -> pd.DataFrame:
    """채널별 핵심 지표."""
    df = _filter_period(ads_df, start_date, end_date)

    agg = df.groupby("channel").agg(
        spend=("spend", "sum"),
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        conversions=("conversions", "sum"),
        revenue=("revenue", "sum"),
    ).reset_index()

    agg["ctr"] = (_safe_div(agg["clicks"], agg["impressions"]) * 100).round(2)
    agg["cvr"] = (_safe_div(agg["conversions"], agg["clicks"]) * 100).round(2)
    agg["cpc"] = _safe_div(agg["spend"], agg["clicks"]).round(0)
    agg["cpa"] = _safe_div(agg["spend"], agg["conversions"]).round(0)
    agg["roas"] = (_safe_div(agg["revenue"], agg["spend"]) * 100).round(0)
    agg["aov"] = _safe_div(agg["revenue"], agg["conversions"]).round(0)
    agg["target_roas"] = agg["channel"].map(lambda c: TARGET_ROAS.get(c, 3.0) * 100)
    agg["roas_gap"] = agg["roas"] - agg["target_roas"]

    return agg


def calc_total_metrics(ads_df: pd.DataFrame, orders_df: pd.DataFrame,
                       start_date=None, end_date=None) -> dict:
    """전체 브랜드 레벨 지표."""
    df_ads = _filter_period(ads_df, start_date, end_date)
    df_ord = _filter_period(orders_df, start_date, end_date)

    total_spend = df_ads["spend"].sum()
    total_revenue = df_ord["revenue"].sum()
    total_orders = len(df_ord)
    unique_customers = df_ord["customer_id"].nunique()

    mer = (total_revenue / total_spend) if total_spend > 0 else 0
    aov = (total_revenue / total_orders) if total_orders > 0 else 0

    return {
        "total_spend": total_spend,
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "unique_customers": unique_customers,
        "mer": mer,
        "aov": aov,
    }


def calc_repurchase(orders_df: pd.DataFrame, cycle_days: int = 30) -> pd.DataFrame:
    """고객별 재구매 요약. store 컬럼이 있으면 함께 aggregate."""
    today = orders_df["date"].max()

    agg_dict = {
        "first_order": ("date", "min"),
        "last_order": ("date", "max"),
        "total_orders": ("order_id", "count"),
        "total_revenue": ("revenue", "sum"),
        "channel": ("channel", "first"),
    }
    if "store" in orders_df.columns:
        agg_dict["store"] = ("store", "first")

    cust = orders_df.groupby("customer_id").agg(**agg_dict).reset_index()

    cust["days_since_last"] = (today - cust["last_order"]).dt.days
    cust["is_repurchaser"] = cust["total_orders"] >= 2
    cust["cycle_reached"] = (cust["days_since_last"] >= cycle_days) & (cust["days_since_last"] < cycle_days * 2)
    cust["churned"] = cust["days_since_last"] >= cycle_days * 2

    return cust


def calc_daily_trend(ads_df: pd.DataFrame, channel: str = None) -> pd.DataFrame:
    """일별 성과 트렌드."""
    df = ads_df[ads_df["channel"] == channel] if channel else ads_df
    daily = df.groupby("date").agg(
        spend=("spend", "sum"),
        revenue=("revenue", "sum"),
        conversions=("conversions", "sum"),
        clicks=("clicks", "sum"),
        impressions=("impressions", "sum"),
    ).reset_index().sort_values("date")

    daily["roas"] = (_safe_div(daily["revenue"], daily["spend"]) * 100).round(0)
    daily["ctr"] = (_safe_div(daily["clicks"], daily["impressions"]) * 100).round(2)
    return daily
