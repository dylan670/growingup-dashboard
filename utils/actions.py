"""액션 추천 엔진 — 광고/매출 지표를 '우리 기준'과 비교해서 의사결정 제안.

THRESHOLDS를 수정하면 자동 추천 로직에 바로 반영됩니다.
향후 '설정' 페이지에서 UI로 조정 가능하게 확장 예정.
"""
from __future__ import annotations

import pandas as pd


# ==========================================================
# 우리 기준 (Phase 3에서 설정 페이지로 이동)
# ==========================================================
THRESHOLDS = {
    # 채널별 목표 ROAS (백분율)
    "target_roas": {
        "네이버": 400,
        "쿠팡":   500,
        "자사몰": 250,
    },
    # 재구매 분석
    "repurchase_cycle_days": 30,

    # 매출 하락 경고 (전 기간 대비 % 변화)
    "orders_drop_alert": -0.20,
    # 매출 증가 기회 임계
    "orders_grow_alert": 0.20,

    # 광고 소재 피로도 (최근 7일 CTR이 직전 7일 대비 % 변화)
    "ctr_fatigue": -0.30,

    # 증액/축소 판정 (ROAS 3일 연속)
    "roas_up_ratio": 1.30,   # 목표 × 1.3 이상 3일 → 증액
    "roas_down_ratio": 1.0,  # 목표 미달 3일 → 축소

    # 재구매율 건강 기준
    "retention_healthy_pct": 30,
    "retention_low_pct": 15,

    # 탑 20% 고객 의존도
    "top20_concentration_warn": 70,
}


def ad_channel_actions(metrics: dict) -> list[dict]:
    """광고 채널 한 개에 대한 액션 추천.

    metrics 키 (모두 옵션이지만 있으면 더 정교한 판단):
        channel: 채널명
        spend: 총 광고비 (원)
        revenue: 총 전환매출 (원)
        conversions: 전환수
        clicks, impressions
        roas: 현재 ROAS (%)
        roas_trend_3d: 최근 3일 일별 ROAS 리스트
        ctr_recent_7d: 최근 7일 CTR
        ctr_prev_7d: 직전 7일 CTR
    """
    actions: list[dict] = []
    channel = metrics.get("channel", "")
    target = THRESHOLDS["target_roas"].get(channel, 300)
    roas = metrics.get("roas", 0)
    spend = metrics.get("spend", 0)
    conversions = metrics.get("conversions", 0)

    # ---------- 1. 활동 없음 ----------
    if spend == 0 and conversions == 0:
        actions.append({
            "severity": "neutral",
            "label": "활동 없음",
            "detail": "해당 기간 광고 집행 없음. 채널 사용 계획 확인 필요.",
        })
        return actions

    # ---------- 2. 3일 연속 트렌드 ----------
    trend = metrics.get("roas_trend_3d") or []
    if len(trend) >= 3:
        up_threshold = target * THRESHOLDS["roas_up_ratio"]
        if all(r >= up_threshold for r in trend):
            new_spend = int(spend / (len(trend) or 3) * 1.2 * (len(trend) or 3))
            actions.append({
                "severity": "opportunity",
                "label": "예산 증액 검토",
                "detail": (
                    f"ROAS 3일 연속 {up_threshold:.0f}% 이상. "
                    f"일 광고비 +20% 시도 (기간 예산 약 {new_spend:,}원)"
                ),
            })
        elif all(r < target for r in trend):
            new_spend = int(spend * 0.8)
            actions.append({
                "severity": "critical",
                "label": "예산 축소 또는 소재 교체",
                "detail": (
                    f"ROAS 3일 연속 목표 {target:.0f}% 미달. "
                    f"예산 -20% (→ {new_spend:,}원) 또는 신규 크리에이티브 전환"
                ),
            })

    # ---------- 3. CTR 피로도 ----------
    ctr_recent = metrics.get("ctr_recent_7d", 0)
    ctr_prev = metrics.get("ctr_prev_7d", 0)
    if ctr_prev > 0:
        drop_ratio = (ctr_recent - ctr_prev) / ctr_prev
        if drop_ratio <= THRESHOLDS["ctr_fatigue"]:
            actions.append({
                "severity": "warning",
                "label": "광고 소재 피로도",
                "detail": (
                    f"최근 7일 CTR {ctr_recent:.2f}% (직전 7일 {ctr_prev:.2f}%, "
                    f"{drop_ratio*100:+.0f}%). 새 크리에이티브 준비"
                ),
            })

    # ---------- 4. 단순 목표 비교 (트렌드 없을 때) ----------
    if not trend and roas > 0:
        if roas >= target * THRESHOLDS["roas_up_ratio"]:
            actions.append({
                "severity": "opportunity",
                "label": "성과 초과 달성",
                "detail": f"ROAS {roas:.0f}% (목표 {target:.0f}%). 증액 여지 있음",
            })
        elif roas < target:
            actions.append({
                "severity": "warning",
                "label": "목표 ROAS 미달",
                "detail": f"현재 ROAS {roas:.0f}% vs 목표 {target:.0f}%. 소재/타겟 개선 필요",
            })

    if not actions:
        actions.append({
            "severity": "neutral",
            "label": "정상 범위",
            "detail": "특이사항 없음. 현재 운영 유지.",
        })
    return actions


def store_sales_actions(metrics: dict, orders_df: pd.DataFrame) -> list[dict]:
    """매출 스토어 한 개에 대한 액션 추천.

    metrics 키:
        store, orders, revenue, customers, aov, repurchase_rate
        orders_prev_period (전 기간 주문수, 옵션)

    orders_df: 해당 스토어의 주문 전체 DataFrame
    """
    actions: list[dict] = []
    cycle = THRESHOLDS["repurchase_cycle_days"]
    store = metrics.get("store", "")

    if metrics.get("orders", 0) == 0:
        actions.append({
            "severity": "neutral",
            "label": "주문 없음",
            "detail": "해당 기간 주문 데이터 없음.",
        })
        return actions

    # ---------- 1. 재구매 주기 도달 고객 (CRM 대상) ----------
    if len(orders_df) > 0 and "date" in orders_df.columns:
        df_tmp = orders_df.copy()
        df_tmp["date"] = pd.to_datetime(df_tmp["date"])
        today = df_tmp["date"].max()

        last_order = df_tmp.groupby("customer_id").agg(
            last_date=("date", "max"),
            total_rev=("revenue", "sum"),
        ).reset_index()
        last_order["days_since"] = (today - last_order["last_date"]).dt.days

        reached = last_order[
            (last_order["days_since"] >= cycle) &
            (last_order["days_since"] < cycle * 2)
        ]
        if len(reached) > 0:
            avg_rev = last_order["total_rev"].mean()
            expected = int(avg_rev * len(reached) * 0.3)
            actions.append({
                "severity": "opportunity",
                "label": f"CRM 리마인더 대상 · {len(reached)}명",
                "detail": (
                    f"지난 주문 후 {cycle}~{cycle*2}일 경과 고객. "
                    f"카카오알림톡/쿠폰 발송 시 예상 매출 ~{expected:,}원 (30% 전환 가정)"
                ),
            })

    # ---------- 2. 주문 수 변화 ----------
    prev = metrics.get("orders_prev_period", 0)
    curr = metrics.get("orders", 0)
    if prev > 0:
        change = (curr - prev) / prev
        if change <= THRESHOLDS["orders_drop_alert"]:
            actions.append({
                "severity": "critical",
                "label": "주문 감소 경고",
                "detail": (
                    f"이번 기간 {curr}건 vs 직전 기간 {prev}건 ({change*100:+.0f}%). "
                    "광고 효율·가격·경쟁사 프로모션 점검"
                ),
            })
        elif change >= THRESHOLDS["orders_grow_alert"]:
            actions.append({
                "severity": "opportunity",
                "label": "주문 성장세",
                "detail": (
                    f"직전 기간 대비 +{change*100:.0f}%. 현재 동력 유지 — "
                    "광고 증액 또는 재고 사전 확보 검토"
                ),
            })

    # ---------- 3. 재구매율 수준 ----------
    rep_rate = metrics.get("repurchase_rate", 0)
    customers = metrics.get("customers", 0)
    if customers >= 20:
        if rep_rate < THRESHOLDS["retention_low_pct"]:
            actions.append({
                "severity": "warning",
                "label": "재구매율 낮음",
                "detail": (
                    f"재구매율 {rep_rate:.0f}% (건강 기준 {THRESHOLDS['retention_healthy_pct']}% 이상). "
                    "첫 구매 후 자동 쿠폰/신제품 알림 시나리오 구축 필요"
                ),
            })
        elif rep_rate >= THRESHOLDS["retention_healthy_pct"]:
            actions.append({
                "severity": "opportunity",
                "label": "재구매 강세",
                "detail": (
                    f"재구매율 {rep_rate:.0f}%. 충성 고객 풀 견고 — "
                    "VIP 전용 혜택/조기 신상품 제안으로 LTV 확대"
                ),
            })

    # ---------- 4. 탑 고객 집중도 ----------
    if len(orders_df) > 10:
        cust_rev = orders_df.groupby("customer_id")["revenue"].sum().sort_values(ascending=False)
        top20_n = max(1, int(len(cust_rev) * 0.2))
        top20_rev = cust_rev.head(top20_n).sum()
        total = cust_rev.sum()
        if total > 0:
            top20_pct = top20_rev / total * 100
            if top20_pct >= THRESHOLDS["top20_concentration_warn"]:
                actions.append({
                    "severity": "warning",
                    "label": "매출 소수 집중",
                    "detail": (
                        f"상위 20% 고객이 전체 매출의 {top20_pct:.0f}%. "
                        "이탈 시 타격 큼 — 신규 고객 확보/저가 라인업으로 분산"
                    ),
                })

    # ---------- 5. AOV 특이성 ----------
    aov = metrics.get("aov", 0)
    if aov >= 50000:
        actions.append({
            "severity": "info",
            "label": f"고객단가 높음 (AOV {aov:,.0f}원)",
            "detail": "프리미엄 고객군. 번들/추가 구매 제안 효과적",
        })

    if not actions:
        actions.append({
            "severity": "neutral",
            "label": "안정 운영",
            "detail": "특별 조치 없음. 현재 흐름 유지.",
        })
    return actions
