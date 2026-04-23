"""의사결정 알림 룰 엔진."""
import pandas as pd

from utils.metrics import TARGET_ROAS, calc_daily_trend, calc_repurchase


def check_all_alerts(ads_df: pd.DataFrame, orders_df: pd.DataFrame) -> list[dict]:
    """모든 룰을 평가해 감지된 알림 리스트 반환."""
    alerts: list[dict] = []
    today = ads_df["date"].max()

    for channel in sorted(ads_df["channel"].unique()):
        alerts.extend(_check_roas_alerts(ads_df, channel))
        alerts.extend(_check_ctr_fatigue(ads_df, channel))

    alerts.extend(_check_channel_reallocation(ads_df, today))
    alerts.extend(_check_retention(orders_df))
    alerts.extend(_check_order_drop(orders_df))

    severity_order = {"urgent": 0, "opportunity": 1, "info": 2}
    alerts.sort(key=lambda a: severity_order.get(a["severity"], 3))
    return alerts


def _check_roas_alerts(ads_df: pd.DataFrame, channel: str) -> list[dict]:
    """최근 3일 ROAS로 증액/축소 신호 감지."""
    results: list[dict] = []
    daily = calc_daily_trend(ads_df, channel).tail(3)
    if len(daily) < 3:
        return results

    target = TARGET_ROAS.get(channel, 3.0) * 100
    roas_list = daily["roas"].fillna(0).tolist()

    if all(r >= target * 1.3 for r in roas_list) and daily["conversions"].is_monotonic_increasing:
        results.append({
            "severity": "opportunity",
            "type": "예산_증액",
            "channel": channel,
            "title": f"[{channel}] 예산 증액 여지",
            "message": f"ROAS 3일 연속 {target * 1.3:.0f}% 이상 + 전환 증가. 예산 20% 증액 검토 구간.",
            "evidence": f"최근 3일 ROAS: {', '.join(f'{r:.0f}%' for r in roas_list)}",
        })

    if all(r < target for r in roas_list):
        results.append({
            "severity": "urgent",
            "type": "예산_축소",
            "channel": channel,
            "title": f"[{channel}] ROAS 3일 연속 목표 미달",
            "message": f"목표 {target:.0f}% 대비 3일 연속 하회. 소재 교체 또는 일시 정지 검토.",
            "evidence": f"최근 3일 ROAS: {', '.join(f'{r:.0f}%' for r in roas_list)}",
        })
    return results


def _check_ctr_fatigue(ads_df: pd.DataFrame, channel: str) -> list[dict]:
    """CTR 최근 7일 vs 직전 7일 30%+ 하락 시 소재 피로도 경고."""
    daily = calc_daily_trend(ads_df, channel)
    if len(daily) < 14:
        return []

    recent = daily.tail(7)["ctr"].mean()
    prev = daily.iloc[-14:-7]["ctr"].mean()

    if prev > 0 and (prev - recent) / prev > 0.30:
        return [{
            "severity": "urgent",
            "type": "소재_피로도",
            "channel": channel,
            "title": f"[{channel}] 광고 소재 피로도 감지",
            "message": "최근 7일 CTR이 직전 7일 대비 30% 이상 하락. 크리에이티브 교체 필요.",
            "evidence": f"직전 7일 CTR {prev:.2f}% → 최근 7일 {recent:.2f}% ({(recent - prev) / prev * 100:+.0f}%)",
        }]
    return []


def _check_channel_reallocation(ads_df: pd.DataFrame, today) -> list[dict]:
    """채널 간 목표 대비 격차가 크면 재배분 제안."""
    recent = ads_df[ads_df["date"] >= today - pd.Timedelta(days=7)]
    if len(recent) == 0:
        return []

    by_ch = recent.groupby("channel").agg(
        spend=("spend", "sum"),
        revenue=("revenue", "sum"),
    )
    by_ch["roas"] = by_ch["revenue"] / by_ch["spend"].replace(0, pd.NA) * 100
    by_ch["target"] = by_ch.index.map(lambda c: TARGET_ROAS.get(c, 3.0) * 100)
    by_ch["gap"] = by_ch["roas"] - by_ch["target"]
    by_ch = by_ch.dropna(subset=["gap"])

    if len(by_ch) < 2:
        return []

    best = by_ch["gap"].idxmax()
    worst = by_ch["gap"].idxmin()

    if by_ch.loc[best, "gap"] > 50 and by_ch.loc[worst, "gap"] < -50:
        return [{
            "severity": "opportunity",
            "type": "채널_재배분",
            "channel": f"{worst} → {best}",
            "title": "채널 예산 재배분 제안",
            "message": f"{best}는 목표 초과 달성, {worst}는 목표 미달. 예산 일부 이동 검토.",
            "evidence": (
                f"{best}: ROAS {by_ch.loc[best, 'roas']:.0f}% (목표 {by_ch.loc[best, 'target']:.0f}%) · "
                f"{worst}: ROAS {by_ch.loc[worst, 'roas']:.0f}% (목표 {by_ch.loc[worst, 'target']:.0f}%)"
            ),
        }]
    return []


def _check_retention(orders_df: pd.DataFrame, cycle_days: int = 30) -> list[dict]:
    """재구매 주기 도달 고객 존재 시 CRM 기회 알림."""
    cust = calc_repurchase(orders_df, cycle_days)
    reached = cust[cust["cycle_reached"]]
    if len(reached) == 0:
        return []

    expected = int(reached["total_revenue"].mean() * len(reached) * 0.3)
    return [{
        "severity": "opportunity",
        "type": "재구매_기회",
        "channel": "전체",
        "title": f"재구매 주기 도달 고객 {len(reached):,}명",
        "message": f"지난 주문 후 {cycle_days}–{cycle_days * 2}일 경과. 카톡/이메일 리마인더 발송 적기.",
        "evidence": f"30% 전환 가정 시 예상 재구매 매출 약 {expected:,}원",
    }]


def _check_order_drop(orders_df: pd.DataFrame) -> list[dict]:
    """최근 7일 주문수 직전 7일 대비 20%+ 감소."""
    today = orders_df["date"].max()
    recent = orders_df[orders_df["date"] >= today - pd.Timedelta(days=6)]
    prev = orders_df[(orders_df["date"] >= today - pd.Timedelta(days=13)) &
                     (orders_df["date"] < today - pd.Timedelta(days=6))]

    if len(prev) == 0:
        return []

    change = (len(recent) - len(prev)) / len(prev)
    if change < -0.2:
        return [{
            "severity": "urgent",
            "type": "주문_감소",
            "channel": "전체",
            "title": f"최근 7일 주문 {change * 100:.0f}% 감소",
            "message": "주문 급감. 외부 요인(명절/경쟁사 할인) 또는 광고 효율 하락 확인 필요.",
            "evidence": f"직전 7일 {len(prev)}건 → 최근 7일 {len(recent)}건",
        }]
    return []
