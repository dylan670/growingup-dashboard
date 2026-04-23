"""네이버 검색광고 소재별 심화 분석 (광고그룹·키워드 브레이크다운).

구매완료(purchase) 기준 전환/매출을 사용합니다.
  - 노출·클릭·광고비: /stats API (generic, 당일은 15분~1시간 지연)
  - 구매·매출: /stat-reports AD_CONVERSION_DETAIL TSV의 `purchase` 행만 합산
    (= 네이버 UI의 "구매완료 전환수" / "구매완료 전환매출액")
"""
from __future__ import annotations

from datetime import date
from typing import Literal

import numpy as np
import pandas as pd


Level = Literal["adgroup", "keyword"]


_STATUS_KO = {
    "ELIGIBLE": "운영 중",
    "PAUSED": "일시정지",
    "PENDING": "심사 중",
    "DELETED": "삭제됨",
    "DISABLED": "비활성",
    "OFF_ON_OPERATOR": "운영자 중지",
    "CAMPAIGN_OFF": "캠페인 중지",
    "ADGROUP_OFF": "그룹 중지",
    "APPROVED": "승인됨",
}


def _status_label(raw: str | None) -> str:
    if not raw:
        return "-"
    return _STATUS_KO.get(raw, raw)


def fetch_breakdown(client, level: Level, since: date, until: date,
                    progress_cb=None) -> pd.DataFrame:
    """광고그룹 또는 키워드 레벨로 기간 집계 + 액션 제안.

    데이터 소스:
      - 노출/클릭/비용: /stats (엔티티 ID 배치 조회)
      - 구매/매출: /stat-reports AD_CONVERSION_DETAIL, purchase 필터링

    Args:
        client: NaverSearchAdClient
        level: "adgroup" 또는 "keyword"
        since, until: datetime.date
        progress_cb: 선택 callback(day_idx, total_days, date_str)

    Returns columns:
        id, 이름, 상태, 노출, 클릭, CTR(%), 비용, 구매, CVR(%), CPA, 매출, ROAS(%), 제안
    """
    if level == "adgroup":
        entities = client.get_adgroups()
        id_key = "nccAdgroupId"
        name_key = "name"
        tsv_group_col = "adgroup_id"
    elif level == "keyword":
        entities = client.get_keywords()
        id_key = "nccKeywordId"
        name_key = "keyword"
        tsv_group_col = "keyword_id"
    else:
        raise ValueError(f"unknown level: {level}")

    entities = [e for e in entities if not e.get("deleted") and not e.get("delFlag")]
    ids = [e[id_key] for e in entities]

    if not ids:
        return pd.DataFrame()

    # 1. /stats로 노출·클릭·비용만
    stats_map = client.get_stats_batch(
        ids, since.isoformat(), until.isoformat(),
        fields=["impCnt", "clkCnt", "salesAmt"],
    )

    # 2. /stat-reports로 구매 전환 데이터 수집 (기간 전체)
    purchase_df = client.fetch_purchase_range(since, until, progress_cb=progress_cb)

    # 3. 레벨별 groupby
    if purchase_df.empty:
        purchase_agg: dict[str, dict] = {}
    else:
        # keyword 레벨이면 "-" 행 (ad-level only) 제외
        if level == "keyword":
            purchase_df = purchase_df[purchase_df["keyword_id"] != "-"]

        if purchase_df.empty:
            purchase_agg = {}
        else:
            agg_df = (
                purchase_df.groupby(tsv_group_col)
                .agg(conversions=("conv_count", "sum"),
                     revenue=("conv_amount", "sum"))
                .reset_index()
            )
            purchase_agg = {
                row[tsv_group_col]: {
                    "conversions": int(row["conversions"]),
                    "revenue": int(row["revenue"]),
                }
                for _, row in agg_df.iterrows()
            }

    # 4. Entity 메타 + /stats + purchase merge
    rows: list[dict] = []
    for e in entities:
        eid = e[id_key]
        s = stats_map.get(eid, {})
        p = purchase_agg.get(eid, {})
        rows.append({
            "id": eid,
            "이름": e.get(name_key, "-"),
            "상태": _status_label(e.get("status") or e.get("inspectStatus")),
            "노출": int(s.get("impCnt", 0) or 0),
            "클릭": int(s.get("clkCnt", 0) or 0),
            "비용": int(round(s.get("salesAmt", 0) or 0)),
            "구매": int(p.get("conversions", 0)),
            "매출": int(p.get("revenue", 0)),
        })

    df = pd.DataFrame(rows)

    def _div(a: pd.Series, b: pd.Series) -> pd.Series:
        return a.astype(float) / b.replace(0, np.nan).astype(float)

    df["CTR(%)"] = (_div(df["클릭"], df["노출"]) * 100).round(2)
    df["CVR(%)"] = (_div(df["구매"], df["클릭"]) * 100).round(2)
    df["CPA"] = _div(df["비용"], df["구매"]).round(0)
    df["ROAS(%)"] = (_div(df["매출"], df["비용"]) * 100).round(0)

    df["제안"] = _generate_hints(df, level)

    cols = ["이름", "상태", "노출", "클릭", "CTR(%)", "비용", "구매", "CVR(%)",
            "CPA", "매출", "ROAS(%)", "제안", "id"]
    return df[cols]


def _generate_hints(df: pd.DataFrame, level: Level) -> pd.Series:
    """룰 기반 의사결정 제안. 구매 0건이면서 비용 발생한 경우를 강조."""
    active = (df["클릭"] > 0) | (df["비용"] > 0)

    roas_series = df.loc[active, "ROAS(%)"].dropna()
    spend_series = df.loc[active, "비용"]
    ctr_series = df.loc[active, "CTR(%)"].dropna()

    median_roas = float(roas_series.median()) if len(roas_series) else 0.0
    median_spend = float(spend_series.median()) if len(spend_series) else 0.0
    median_ctr = float(ctr_series.median()) if len(ctr_series) else 0.0

    hints: list[str] = []
    for _, r in df.iterrows():
        notes: list[str] = []
        spend = r["비용"]
        clicks = r["클릭"]
        purchases = r["구매"]
        imp = r["노출"]
        roas = r["ROAS(%)"] if pd.notna(r["ROAS(%)"]) else 0
        ctr = r["CTR(%)"] if pd.notna(r["CTR(%)"]) else 0

        if spend == 0 and clicks == 0:
            notes.append("활동 없음")
        else:
            # 비용 많이 쓰는데 구매 없음 → 낭비 중
            if purchases == 0 and clicks >= 5 and spend > max(5000, median_spend * 0.5):
                notes.append("구매 0 — 낭비 중, 제외어·랜딩 점검" if level == "keyword"
                             else "구매 0 — 소재/랜딩 재검토")

            # ROAS 매우 낮음 + 비용 많이 씀 → 축소 대상
            elif roas > 0 and roas < median_roas * 0.3 and spend >= median_spend:
                notes.append("ROAS 하위 — 제외어 검토" if level == "keyword"
                             else "그룹 구조 재검토")

            # ROAS 매우 높음 + 구매 있음 → 확대 기회
            elif roas > median_roas * 2 and purchases >= 2:
                notes.append("ROAS 상위 — 입찰 상향" if level == "keyword"
                             else "예산·입찰 증액")

            # CTR 낮음 → 소재 개선
            if ctr > 0 and ctr < max(0.5, median_ctr * 0.4) and imp >= 1000:
                notes.append("CTR 낮음 — 소재 개선")

            # CTR 높은데 CVR 낮음 (그룹 레벨)
            if level == "adgroup" and ctr > median_ctr * 1.3 and purchases > 0:
                cvr = purchases / clicks * 100 if clicks > 0 else 0
                if cvr < 2.0:
                    notes.append("CTR↑ CVR↓ — 랜딩 최적화")

        hints.append(" · ".join(notes) if notes else "—")

    return pd.Series(hints, index=df.index)


def summarize(df: pd.DataFrame) -> dict:
    """전체 요약 (상단 KPI용). 구·신 스키마 모두 호환 ("구매" 또는 "전환")."""
    if df.empty:
        return {
            "total_entities": 0, "active_entities": 0,
            "total_spend": 0, "total_revenue": 0, "total_conversions": 0,
            "roas": 0,
        }

    conv_col = "구매" if "구매" in df.columns else ("전환" if "전환" in df.columns else None)

    active = (df["클릭"] > 0) | (df["비용"] > 0)
    total_spend = df["비용"].sum()
    total_revenue = df["매출"].sum()
    total_conv = int(df[conv_col].sum()) if conv_col else 0

    return {
        "total_entities": len(df),
        "active_entities": int(active.sum()),
        "total_spend": int(total_spend),
        "total_revenue": int(total_revenue),
        "total_conversions": total_conv,
        "roas": round(total_revenue / total_spend * 100) if total_spend else 0,
    }
