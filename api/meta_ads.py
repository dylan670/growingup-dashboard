"""Meta (Facebook) Marketing API 클라이언트 — 광고 계정별 성과 조회.

공식 문서: https://developers.facebook.com/docs/marketing-api/

인증:
    Access Token 기반 (OAuth 2.0의 Implicit grant).
    - 단기 User Token: 1-2시간
    - 장기 User Token: 60일 (Extend 버튼으로 교환)
    - 시스템 사용자 Token: 영구 (Business Manager 필요)

필요 자격증명 (계정당):
    - Access Token
    - Ad Account ID (format: 'act_1234567890')

주요 API:
    GET /act_{ad_account_id}/insights
      params:
        fields: impressions, clicks, spend, ctr, cpm, actions, purchase_roas, ...
        time_range: {"since":"2026-01-22","until":"2026-04-21"}
        level: account / campaign / adset / ad
        time_increment: 1 (일별 breakdown)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests


API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# 기본 조회 필드
DEFAULT_INSIGHT_FIELDS = [
    "impressions",
    "clicks",
    "spend",
    "ctr",
    "cpc",
    "cpm",
    "reach",
    "actions",
    "action_values",
    "purchase_roas",
]


class MetaAdsClient:
    def __init__(self, access_token: str, ad_account_id: str,
                 store_label: str = ""):
        if not access_token or not ad_account_id:
            raise ValueError("Access Token과 Ad Account ID 모두 필요합니다.")
        self.access_token = access_token.strip()
        # act_ prefix 자동 추가
        aid = ad_account_id.strip()
        self.ad_account_id = aid if aid.startswith("act_") else f"act_{aid}"
        self.store_label = store_label

    # ---------- 공통 요청 ----------
    def _request(self, path: str, params: dict | None = None) -> Any:
        url = f"{BASE_URL}/{path.lstrip('/')}"
        p = {"access_token": self.access_token}
        if params:
            p.update(params)
        resp = requests.get(url, params=p, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ---------- 공개 메서드 ----------
    def test_connection(self) -> tuple[bool, str]:
        try:
            res = self._request(self.ad_account_id, {
                "fields": "id,name,account_status,currency,timezone_name",
            })
            name = res.get("name", "?")
            status = res.get("account_status")
            currency = res.get("currency", "?")
            return True, (
                f"인증 성공 - {name} (status={status}, currency={currency})"
            )
        except requests.HTTPError as e:
            code = e.response.status_code if e.response else 0
            body = e.response.text[:300] if e.response else ""
            if code in (400, 401, 403):
                return False, f"인증 실패 HTTP {code}. Token/Account ID 확인 필요.\n{body}"
            return False, f"HTTP {code}: {body}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def get_insights(self, since: date, until: date,
                     level: str = "account",
                     time_increment: int | str = 1,
                     fields: list[str] | None = None) -> list[dict]:
        """광고 인사이트 조회 (일별 breakdown).

        Args:
            since, until: date
            level: account / campaign / adset / ad
            time_increment: 1 (일별) / 7 (주별) / 'monthly' / 'all_days'
            fields: 필드 리스트. None이면 DEFAULT_INSIGHT_FIELDS
        """
        import json as _json

        params = {
            "fields": ",".join(fields or DEFAULT_INSIGHT_FIELDS),
            "time_range": _json.dumps({
                "since": since.isoformat(),
                "until": until.isoformat(),
            }),
            "level": level,
            "time_increment": str(time_increment),
            "limit": 500,
        }

        all_rows: list[dict] = []
        next_url: str | None = None

        # 첫 호출
        res = self._request(f"{self.ad_account_id}/insights", params)
        all_rows.extend(res.get("data", []))

        # 페이징
        paging = res.get("paging", {}) or {}
        next_url = paging.get("next")
        while next_url:
            # next URL은 절대 경로, access_token 이미 포함
            resp = requests.get(next_url, timeout=60)
            resp.raise_for_status()
            res = resp.json()
            all_rows.extend(res.get("data", []))
            paging = res.get("paging", {}) or {}
            next_url = paging.get("next")

        return all_rows

    def fetch_ads_df(self, since: date, until: date,
                     store: str) -> pd.DataFrame:
        """광고 성과 → 대시보드 ads.csv 스키마 (store 컬럼 추가).

        반환 컬럼: date, channel, store, spend, impressions, clicks,
                   conversions, revenue
        """
        raw = self.get_insights(since, until, level="account", time_increment=1)

        rows: list[dict] = []
        for entry in raw:
            date_str = entry.get("date_start", "") or entry.get("date_stop", "")
            if not date_str:
                continue

            spend = float(entry.get("spend") or 0)
            impressions = int(float(entry.get("impressions") or 0))
            clicks = int(float(entry.get("clicks") or 0))

            # 구매 전환 수 (actions 배열에서 purchase 추출)
            conversions = 0
            for a in (entry.get("actions") or []):
                if a.get("action_type") in ("purchase", "offsite_conversion.fb_pixel_purchase"):
                    conversions += int(float(a.get("value") or 0))

            # 구매 매출 (action_values)
            revenue = 0.0
            for v in (entry.get("action_values") or []):
                if v.get("action_type") in ("purchase", "offsite_conversion.fb_pixel_purchase"):
                    revenue += float(v.get("value") or 0)

            rows.append({
                "date": date_str,
                "channel": "자사몰",
                "store": store,
                "spend": int(round(spend)),
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": int(round(revenue)),
            })

        return pd.DataFrame(rows, columns=[
            "date", "channel", "store", "spend",
            "impressions", "clicks", "conversions", "revenue",
        ])

    def fetch_campaigns_df(self, since: date, until: date) -> pd.DataFrame:
        """캠페인 단위 광고 성과 (기간 합계).

        반환 컬럼: campaign_id, campaign_name, spend, impressions, clicks,
                  ctr_pct, cpc, conversions, revenue, roas_pct
        """
        raw = self.get_insights(
            since, until, level="campaign", time_increment="all_days",
            fields=[
                "campaign_id", "campaign_name",
                "impressions", "clicks", "spend", "ctr", "cpc", "cpm",
                "actions", "action_values", "purchase_roas",
            ],
        )

        rows: list[dict] = []
        for entry in raw:
            spend = float(entry.get("spend") or 0)
            impressions = int(float(entry.get("impressions") or 0))
            clicks = int(float(entry.get("clicks") or 0))

            # 구매 전환 + 매출
            conversions = 0
            revenue = 0.0
            for a in (entry.get("actions") or []):
                if a.get("action_type") in (
                    "purchase", "offsite_conversion.fb_pixel_purchase",
                ):
                    conversions += int(float(a.get("value") or 0))
            for v in (entry.get("action_values") or []):
                if v.get("action_type") in (
                    "purchase", "offsite_conversion.fb_pixel_purchase",
                ):
                    revenue += float(v.get("value") or 0)

            ctr = float(entry.get("ctr") or 0)
            cpc = float(entry.get("cpc") or 0)
            roas_pct = (revenue / spend * 100) if spend > 0 else 0

            rows.append({
                "campaign_id": entry.get("campaign_id", ""),
                "campaign_name": entry.get("campaign_name", "?"),
                "spend": int(round(spend)),
                "impressions": impressions,
                "clicks": clicks,
                "ctr_pct": round(ctr, 2),
                "cpc": int(round(cpc)),
                "conversions": conversions,
                "revenue": int(round(revenue)),
                "roas_pct": int(round(roas_pct)),
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("spend", ascending=False).reset_index(drop=True)
        return df

    def fetch_campaigns_daily_df(
        self, since: date, until: date,
    ) -> pd.DataFrame:
        """캠페인 × 일자 단위 광고 성과 (프리컴퓨트 저장 용).

        loader 에서 선택 기간으로 슬라이스 후 합산하므로 `ctr_pct`/`cpc`/
        `roas_pct` 같은 파생 지표는 저장하지 않음 (합산 시 부정확).

        반환 컬럼: date, campaign_id, campaign_name, spend, impressions,
                  clicks, conversions, revenue
        """
        raw = self.get_insights(
            since, until, level="campaign", time_increment=1,
            fields=[
                "campaign_id", "campaign_name",
                "impressions", "clicks", "spend",
                "actions", "action_values",
            ],
        )

        rows: list[dict] = []
        for entry in raw:
            date_str = entry.get("date_start", "") or entry.get("date_stop", "")
            if not date_str:
                continue

            spend = float(entry.get("spend") or 0)
            impressions = int(float(entry.get("impressions") or 0))
            clicks = int(float(entry.get("clicks") or 0))

            conversions = 0
            revenue = 0.0
            for a in (entry.get("actions") or []):
                if a.get("action_type") in (
                    "purchase", "offsite_conversion.fb_pixel_purchase",
                ):
                    conversions += int(float(a.get("value") or 0))
            for v in (entry.get("action_values") or []):
                if v.get("action_type") in (
                    "purchase", "offsite_conversion.fb_pixel_purchase",
                ):
                    revenue += float(v.get("value") or 0)

            rows.append({
                "date": date_str,
                "campaign_id": entry.get("campaign_id", ""),
                "campaign_name": entry.get("campaign_name", "?"),
                "spend": int(round(spend)),
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": int(round(revenue)),
            })

        return pd.DataFrame(rows, columns=[
            "date", "campaign_id", "campaign_name", "spend",
            "impressions", "clicks", "conversions", "revenue",
        ])


# ==========================================================
# 멀티 계정 로더
# ==========================================================

def _env_suffix_for_store(store: str) -> str:
    return {"똑똑연구소": "DDOK", "롤라루": "ROLLA", "루티니스트": "RUTI"}.get(
        store, store.upper(),
    )


def load_meta_client(store_brand: str) -> MetaAdsClient | None:
    """브랜드별 Meta 클라이언트 로드."""
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

    suffix = _env_suffix_for_store(store_brand)
    token = os.getenv(f"META_ACCESS_TOKEN_{suffix}", "").strip()
    acct = os.getenv(f"META_AD_ACCOUNT_ID_{suffix}", "").strip()

    if not all([token, acct]):
        return None

    store_label = f"자사몰_{store_brand}"
    return MetaAdsClient(token, acct, store_label=store_label)


def load_all_meta_clients() -> dict[str, MetaAdsClient]:
    """모든 Meta 광고 계정 클라이언트. key = '자사몰_{brand}'."""
    clients: dict[str, MetaAdsClient] = {}
    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        c = load_meta_client(brand)
        if c is not None:
            clients[f"자사몰_{brand}"] = c
    return clients


def save_meta_credentials(store_brand: str, access_token: str,
                          ad_account_id: str) -> None:
    from pathlib import Path

    env_path = Path(__file__).parent.parent / ".env"
    suffix = _env_suffix_for_store(store_brand)

    existing: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()

    existing[f"META_ACCESS_TOKEN_{suffix}"] = access_token.strip()
    existing[f"META_AD_ACCOUNT_ID_{suffix}"] = ad_account_id.strip()

    lines = [f"{k}={v}" for k, v in existing.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
