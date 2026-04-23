"""네이버 검색광고 API 클라이언트.

공식 문서: https://naver.github.io/searchad-apidoc/

인증 방식:
    HMAC-SHA256 서명. 요청마다 timestamp + method + uri 문자열을 secret_key로 서명.

필요한 자격증명:
    1. API Key        (X-API-KEY 헤더)
    2. Secret Key     (HMAC 서명용)
    3. Customer ID    (X-Customer 헤더, 광고주 계정 번호)

발급 경로:
    네이버 검색광고 시스템 (searchad.naver.com)
    → 도구 → API 사용 관리 → 'API 사용 신청' → 승인 후 Key/Secret 발급

실시간성:
    - 통계 데이터는 15분 ~ 1시간 지연으로 제공
    - 당일 데이터는 실시간 가까우나 최종값 아님
    - 전일자 이전 데이터는 확정 수치
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests


BASE_URL = "https://api.searchad.naver.com"


class NaverSearchAdClient:
    """네이버 검색광고 API 호출 클라이언트."""

    def __init__(self, api_key: str, secret_key: str, customer_id: str | int):
        if not all([api_key, secret_key, customer_id]):
            raise ValueError("API Key, Secret Key, Customer ID 세 가지 모두 필요합니다.")
        self.api_key = api_key.strip()
        self.secret_key = secret_key.strip()
        self.customer_id = str(customer_id).strip()

    # ---------- 서명 ----------
    def _sign(self, method: str, uri: str, timestamp: str) -> str:
        """HMAC-SHA256 서명 생성.

        message = "{timestamp}.{method}.{uri}"  (uri는 query string 제외)
        """
        message = f"{timestamp}.{method.upper()}.{uri}"
        digest = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _headers(self, method: str, uri: str) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        return {
            "X-Timestamp": ts,
            "X-API-KEY": self.api_key,
            "X-Customer": self.customer_id,
            "X-Signature": self._sign(method, uri, ts),
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json",
        }

    def _request(self, method: str, uri: str, **kwargs) -> Any:
        url = BASE_URL + uri
        headers = self._headers(method, uri)
        headers.update(kwargs.pop("headers", {}))
        resp = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        resp.raise_for_status()
        if resp.text:
            return resp.json()
        return None

    # ---------- 공개 메서드 ----------
    def test_connection(self) -> tuple[bool, str]:
        """연결 테스트. 성공 시 (True, 메시지), 실패 시 (False, 에러)."""
        try:
            campaigns = self.get_campaigns()
            total = len(campaigns)
            active = sum(1 for c in campaigns if c.get("status") == "ELIGIBLE")
            return True, f"연결 성공. 전체 캠페인 {total}개 (활성 {active}개)."
        except requests.HTTPError as e:
            code = e.response.status_code
            body = e.response.text[:200] if e.response else ""
            if code in (401, 403):
                return False, (
                    f"인증 실패 (HTTP {code}). API Key / Secret / Customer ID 확인 필요.\n"
                    f"응답: {body}"
                )
            return False, f"API 오류 (HTTP {code}): {body}"
        except requests.RequestException as e:
            return False, f"네트워크 오류: {e}"
        except Exception as e:
            return False, f"알 수 없는 오류: {e}"

    def get_campaigns(self) -> list[dict]:
        """내 계정의 모든 캠페인 조회 (연결 테스트용)."""
        return self._request("GET", "/ncc/campaigns") or []

    def get_adgroups(self) -> list[dict]:
        """내 계정의 모든 광고그룹 조회. /stats는 adgroup 레벨 ID만 허용."""
        return self._request("GET", "/ncc/adgroups") or []

    def get_keywords(self, adgroup_id: str | None = None) -> list[dict]:
        """키워드 조회. adgroup_id 미지정 시 모든 광고그룹 순회.

        Naver API 제약: /ncc/keywords는 nccAdgroupId/ids/nccLabelId 중 하나 필수.
        """
        if adgroup_id:
            return self._request("GET", "/ncc/keywords",
                                 params={"nccAdgroupId": adgroup_id}) or []
        # 전체 키워드: 광고그룹마다 순회
        adgroups = self.get_adgroups()
        all_keywords: list[dict] = []
        for ag in adgroups:
            if ag.get("deleted"):
                continue
            kws = self._request("GET", "/ncc/keywords",
                                params={"nccAdgroupId": ag["nccAdgroupId"]}) or []
            all_keywords.extend(kws)
        return all_keywords

    def get_stats_batch(self, ids: list[str], since: str, until: str,
                        fields: list[str] | None = None,
                        batch_size: int = 100) -> dict[str, dict]:
        """여러 ID에 대해 기간 집계 통계를 배치 조회.

        ⚠ 여기서 가져오는 ccnt/convAmt는 네이버의 "전체 전환"이며 구매완료만 원하면
        `fetch_purchase_by_date` 결과를 merge 하는 방식을 써야 함.

        Args:
            ids: 광고그룹/키워드/광고 ID 리스트
            since, until: 'YYYY-MM-DD' (기간 집계 — /stats는 범위 총합만 반환)
            fields: 조회할 지표 필드 (기본: 노출·클릭·비용)
            batch_size: 한 번에 /stats에 넣을 ID 수

        Returns:
            dict {id: stats_entry}
        """
        import json as _json

        # 기본값은 노출/클릭/비용만 (전환은 stat-reports 쪽에서 구매완료 기준으로 가져옴)
        default_fields = ["impCnt", "clkCnt", "salesAmt"]
        f = _json.dumps(fields or default_fields)
        tr = _json.dumps({"since": since, "until": until})

        out: dict[str, dict] = {}
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            params = {"ids": ",".join(batch), "fields": f, "timeRange": tr}
            res = self._request("GET", "/stats", params=params) or {}
            for entry in res.get("data", []):
                entry_id = entry.get("id")
                if entry_id:
                    out[entry_id] = entry
        return out

    # ---------- stat-reports: 구매완료 전환 데이터 전용 ----------

    # AD_CONVERSION_DETAIL TSV 15-컬럼 스키마 (2024 기준)
    _CONV_TSV_COLUMNS = [
        "date", "customer_id", "campaign_id", "adgroup_id", "keyword_id",
        "ad_id", "channel_id", "hour", "col9", "col10",
        "device", "conv_method", "conv_action", "conv_count", "conv_amount",
    ]

    def request_stat_report(self, report_tp: str, stat_dt: str) -> int:
        """POST /stat-reports. 리포트 생성 요청 후 reportJobId 반환.

        Args:
            report_tp: e.g. "AD_CONVERSION_DETAIL", "CAMPAIGN_CONVERSION_DETAIL", "AD"
            stat_dt: 'YYYY-MM-DD'
        """
        res = self._request("POST", "/stat-reports", json={
            "reportTp": report_tp,
            "statDt": stat_dt,
        }) or {}
        job_id = res.get("reportJobId")
        if job_id is None:
            raise RuntimeError(f"reportJobId 누락: {res}")
        return int(job_id)

    def wait_for_report(self, job_id: int, timeout: int = 60, poll: int = 2) -> dict:
        """리포트 생성 완료까지 폴링. 과거 날짜는 거의 즉시 BUILT."""
        import time
        uri = f"/stat-reports/{job_id}"
        elapsed = 0
        while elapsed <= timeout:
            info = self._request("GET", uri) or {}
            status = info.get("status")
            if status in ("BUILT", "DONE"):
                return info
            if status in ("FAILED", "NONE"):
                raise RuntimeError(f"리포트 실패 ({status}): {info}")
            time.sleep(poll)
            elapsed += poll
        raise TimeoutError(f"리포트 대기 초과 ({timeout}s): job_id={job_id}")

    def download_report_tsv(self, download_url: str) -> bytes:
        """서명된 헤더로 stat-reports downloadUrl 다운로드. TSV bytes 반환."""
        from urllib.parse import urlparse

        parsed = urlparse(download_url)
        headers = self._headers("GET", parsed.path)
        resp = requests.get(download_url, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.content

    def fetch_conversion_detail(self, stat_dt: str) -> pd.DataFrame:
        """하루치 AD_CONVERSION_DETAIL 조회 → 15-컬럼 DataFrame 반환.

        데이터 없으면 빈 DataFrame.
        """
        from io import BytesIO

        job_id = self.request_stat_report("AD_CONVERSION_DETAIL", stat_dt)
        info = self.wait_for_report(job_id)
        url = info.get("downloadUrl", "")
        if not url:
            return pd.DataFrame(columns=self._CONV_TSV_COLUMNS)

        raw = self.download_report_tsv(url)
        if not raw:
            return pd.DataFrame(columns=self._CONV_TSV_COLUMNS)

        df = pd.read_csv(
            BytesIO(raw), sep="\t", header=None, dtype=str,
            names=self._CONV_TSV_COLUMNS, keep_default_na=False,
        )
        df["conv_count"] = pd.to_numeric(df["conv_count"], errors="coerce").fillna(0).astype(int)
        df["conv_amount"] = pd.to_numeric(df["conv_amount"], errors="coerce").fillna(0).astype(int)
        return df

    def fetch_purchase_range(self, since: date, until: date,
                             progress_cb=None) -> pd.DataFrame:
        """기간 내 모든 날짜의 구매완료(purchase) 전환 데이터 수집.

        Returns:
            DataFrame with columns from _CONV_TSV_COLUMNS, filtered to
            conv_action == 'purchase', for all dates in range.
        """
        total_days = (until - since).days + 1
        frames: list[pd.DataFrame] = []

        current = since
        day_idx = 0
        while current <= until:
            day_idx += 1
            date_str = current.isoformat()
            if progress_cb:
                progress_cb(day_idx, total_days, date_str)

            try:
                df = self.fetch_conversion_detail(date_str)
            except Exception:
                df = pd.DataFrame(columns=self._CONV_TSV_COLUMNS)

            # 구매완료만 필터
            if not df.empty:
                df = df[df["conv_action"] == "purchase"].copy()
                if not df.empty:
                    frames.append(df)

            current += timedelta(days=1)

        if not frames:
            return pd.DataFrame(columns=self._CONV_TSV_COLUMNS)
        return pd.concat(frames, ignore_index=True)

    def fetch_campaigns_df(
        self, since: date, until: date, progress_cb=None,
    ) -> pd.DataFrame:
        """캠페인 단위 성과 (기간 합계).

        /stats 는 adgroup 레벨로 조회 → 캠페인별로 집계.
        fetch_purchase_range 로 구매완료 전환도 campaign_id 기준 합산.

        반환 컬럼: campaign_id, campaign_name, brand, spend, impressions,
                  clicks, ctr_pct, cpc, conversions, revenue, roas_pct
        """
        from utils.products import classify_naver_to_brand

        # 캠페인·광고그룹 매핑
        campaigns = self.get_campaigns()
        campaign_name_map = {
            c.get("nccCampaignId"): c.get("name", "") for c in campaigns
        }
        adgroups = self.get_adgroups()
        ag_to_campaign: dict[str, str] = {}
        for ag in adgroups:
            if not ag.get("deleted"):
                ag_to_campaign[ag.get("nccAdgroupId")] = ag.get("nccCampaignId")

        # 1) /stats 기간 루프 — adgroup 레벨 집계
        adgroup_agg: dict[str, dict] = {}  # agid → {spend, imp, clk}
        current = since
        while current <= until:
            date_str = current.isoformat()
            stats_list = self.get_stats(list(ag_to_campaign.keys()), date_str) if ag_to_campaign else []
            for entry in stats_list:
                agid = entry.get("id") or entry.get("adgroupId") or entry.get("keywordId")
                # /stats 응답 구조상 ID 가 여러 필드에 올 수 있어 fallback
                if not agid:
                    # 키워드 기반 매칭: 각 스토어별 ads 의 adgroup 정리되어 있다 가정
                    continue
                bucket = adgroup_agg.setdefault(
                    str(agid), {"spend": 0, "imp": 0, "clk": 0},
                )
                bucket["spend"] += float(entry.get("salesAmt", 0) or 0)
                bucket["imp"] += float(entry.get("impCnt", 0) or 0)
                bucket["clk"] += float(entry.get("clkCnt", 0) or 0)
            current += timedelta(days=1)

        # stats 응답 id 가 없으면 fallback — 일자×전체 stats 데이터로 광고그룹별 없이 집계 불가
        # 안전하게: 각 날짜별 요청을 adgroup_id 개별로 분리 호출
        if not adgroup_agg and ag_to_campaign:
            # 대체 경로: adgroup 별로 개별 쿼리 (비용 높지만 안전)
            for agid in list(ag_to_campaign.keys())[:200]:  # 상한
                current = since
                while current <= until:
                    date_str = current.isoformat()
                    entries = self.get_stats([agid], date_str)
                    for entry in entries:
                        bucket = adgroup_agg.setdefault(
                            str(agid), {"spend": 0, "imp": 0, "clk": 0},
                        )
                        bucket["spend"] += float(entry.get("salesAmt", 0) or 0)
                        bucket["imp"] += float(entry.get("impCnt", 0) or 0)
                        bucket["clk"] += float(entry.get("clkCnt", 0) or 0)
                    current += timedelta(days=1)

        # 2) 캠페인 레벨 집계
        campaign_agg: dict[str, dict] = {}
        for agid, vals in adgroup_agg.items():
            cmp_id = ag_to_campaign.get(agid, "")
            if not cmp_id:
                continue
            bucket = campaign_agg.setdefault(
                cmp_id, {"spend": 0, "imp": 0, "clk": 0, "conv": 0, "rev": 0},
            )
            bucket["spend"] += vals["spend"]
            bucket["imp"] += vals["imp"]
            bucket["clk"] += vals["clk"]

        # 3) 구매완료 전환 — campaign_id 기준
        try:
            purchase_df = self.fetch_purchase_range(since, until, progress_cb=progress_cb)
        except Exception:
            purchase_df = pd.DataFrame()

        if not purchase_df.empty and "campaign_id" in purchase_df.columns:
            conv_by_cmp = purchase_df.groupby("campaign_id").agg(
                conv=("conv_count", "sum"),
                rev=("conv_amount", "sum"),
            )
            for cmp_id, row in conv_by_cmp.iterrows():
                bucket = campaign_agg.setdefault(
                    str(cmp_id),
                    {"spend": 0, "imp": 0, "clk": 0, "conv": 0, "rev": 0},
                )
                bucket["conv"] += int(row["conv"])
                bucket["rev"] += float(row["rev"])

        # 4) DataFrame 생성
        rows: list[dict] = []
        for cmp_id, bucket in campaign_agg.items():
            name = campaign_name_map.get(cmp_id, "(이름 없음)")
            brand = classify_naver_to_brand(name)
            spend = int(round(bucket["spend"]))
            rev = int(round(bucket["rev"]))
            imp = int(bucket["imp"])
            clk = int(bucket["clk"])
            conv = int(bucket["conv"])
            ctr = round(clk / imp * 100, 2) if imp else 0
            cpc = int(spend / clk) if clk else 0
            roas = int(round(rev / spend * 100)) if spend else 0

            rows.append({
                "campaign_id": cmp_id,
                "campaign_name": name,
                "brand": brand,
                "spend": spend,
                "impressions": imp,
                "clicks": clk,
                "ctr_pct": ctr,
                "cpc": cpc,
                "conversions": conv,
                "revenue": rev,
                "roas_pct": roas,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("spend", ascending=False).reset_index(drop=True)
        return df

    def fetch_campaigns_daily_df(
        self, since: date, until: date, progress_cb=None,
    ) -> pd.DataFrame:
        """캠페인 × 일자 단위 성과 (프리컴퓨트 저장 용).

        loader 가 선택 기간으로 슬라이스 후 합산하므로 파생 지표
        (ctr_pct/cpc/roas_pct) 는 저장하지 않음 — 합산 시 부정확.

        반환 컬럼: date, campaign_id, campaign_name, brand, spend,
                  impressions, clicks, conversions, revenue
        """
        from utils.products import classify_naver_to_brand

        campaigns = self.get_campaigns()
        campaign_name_map = {
            c.get("nccCampaignId"): c.get("name", "") for c in campaigns
        }
        adgroups = self.get_adgroups()
        ag_to_campaign: dict[str, str] = {}
        for ag in adgroups:
            if not ag.get("deleted"):
                ag_to_campaign[ag.get("nccAdgroupId")] = ag.get("nccCampaignId")

        # 1) /stats 기간 루프 — (adgroup, date) 단위 보존
        daily_agg: dict[tuple[str, str], dict] = {}  # (agid, date) → {spend,imp,clk}
        current = since
        while current <= until:
            date_str = current.isoformat()
            stats_list = self.get_stats(list(ag_to_campaign.keys()), date_str) \
                if ag_to_campaign else []
            for entry in stats_list:
                agid = entry.get("id") or entry.get("adgroupId") or entry.get("keywordId")
                if not agid:
                    continue
                key = (str(agid), date_str)
                bucket = daily_agg.setdefault(
                    key, {"spend": 0.0, "imp": 0.0, "clk": 0.0},
                )
                bucket["spend"] += float(entry.get("salesAmt", 0) or 0)
                bucket["imp"] += float(entry.get("impCnt", 0) or 0)
                bucket["clk"] += float(entry.get("clkCnt", 0) or 0)
            current += timedelta(days=1)

        # fallback — agid 누락 시 adgroup × 일자 개별 호출
        if not daily_agg and ag_to_campaign:
            for agid in list(ag_to_campaign.keys())[:200]:
                current = since
                while current <= until:
                    date_str = current.isoformat()
                    entries = self.get_stats([agid], date_str)
                    for entry in entries:
                        key = (str(agid), date_str)
                        bucket = daily_agg.setdefault(
                            key, {"spend": 0.0, "imp": 0.0, "clk": 0.0},
                        )
                        bucket["spend"] += float(entry.get("salesAmt", 0) or 0)
                        bucket["imp"] += float(entry.get("impCnt", 0) or 0)
                        bucket["clk"] += float(entry.get("clkCnt", 0) or 0)
                    current += timedelta(days=1)

        # 2) 캠페인 × 일자 집계
        campaign_daily: dict[tuple[str, str], dict] = {}
        for (agid, date_str), vals in daily_agg.items():
            cmp_id = ag_to_campaign.get(agid, "")
            if not cmp_id:
                continue
            key = (cmp_id, date_str)
            bucket = campaign_daily.setdefault(
                key, {"spend": 0.0, "imp": 0.0, "clk": 0.0, "conv": 0, "rev": 0.0},
            )
            bucket["spend"] += vals["spend"]
            bucket["imp"] += vals["imp"]
            bucket["clk"] += vals["clk"]

        # 3) 구매완료 전환 — campaign × 일자 기준
        try:
            purchase_df = self.fetch_purchase_range(since, until, progress_cb=progress_cb)
        except Exception:
            purchase_df = pd.DataFrame()

        if (
            not purchase_df.empty
            and "campaign_id" in purchase_df.columns
            and "date" in purchase_df.columns
        ):
            # date 컬럼은 YYYY-MM-DD 또는 YYYYMMDD 문자열일 수 있음
            purchase_df = purchase_df.copy()
            purchase_df["date_norm"] = (
                purchase_df["date"].astype(str).str.slice(0, 10)
                .str.replace(r"(\d{4})(\d{2})(\d{2})", r"\1-\2-\3", regex=True)
            )
            conv_by = purchase_df.groupby(["campaign_id", "date_norm"]).agg(
                conv=("conv_count", "sum"),
                rev=("conv_amount", "sum"),
            )
            for (cmp_id, date_str), row in conv_by.iterrows():
                key = (str(cmp_id), str(date_str))
                bucket = campaign_daily.setdefault(
                    key,
                    {"spend": 0.0, "imp": 0.0, "clk": 0.0, "conv": 0, "rev": 0.0},
                )
                bucket["conv"] += int(row["conv"])
                bucket["rev"] += float(row["rev"])

        # 4) DataFrame 생성
        rows: list[dict] = []
        for (cmp_id, date_str), bucket in campaign_daily.items():
            name = campaign_name_map.get(cmp_id, "(이름 없음)")
            brand = classify_naver_to_brand(name)
            rows.append({
                "date": date_str,
                "campaign_id": cmp_id,
                "campaign_name": name,
                "brand": brand,
                "spend": int(round(bucket["spend"])),
                "impressions": int(bucket["imp"]),
                "clicks": int(bucket["clk"]),
                "conversions": int(bucket["conv"]),
                "revenue": int(round(bucket["rev"])),
            })

        return pd.DataFrame(rows, columns=[
            "date", "campaign_id", "campaign_name", "brand",
            "spend", "impressions", "clicks", "conversions", "revenue",
        ])

    def get_daily_stats_by_brand_df(
        self, since: date, until: date, progress_cb=None,
    ) -> pd.DataFrame:
        """기간 내 일자 × 브랜드 통계 DataFrame.

        캠페인·광고그룹 이름의 키워드 매칭으로 브랜드 자동 분류 후 집계.
        '공통' (매칭 실패) 브랜드는 결과에서 제외 (광고비가 0 이 아니면 별도 경고).

        반환 컬럼:
            date, channel='네이버', store='네이버_{브랜드}',
            spend, impressions, clicks, conversions, revenue
        """
        # 로컬 import (순환 참조 회피)
        from utils.products import classify_naver_to_brand

        # 1) 캠페인 map (id → name)
        campaigns = self.get_campaigns()
        campaign_name_map = {
            c.get("nccCampaignId"): c.get("name", "")
            for c in campaigns
        }

        # 2) 광고그룹 map + 브랜드 분류
        adgroups = self.get_adgroups()
        adgroup_brand: dict[str, str] = {}  # adgroup_id → brand
        adgroup_classify_debug: list[dict] = []
        for ag in adgroups:
            if ag.get("deleted"):
                continue
            ag_id = ag.get("nccAdgroupId")
            ag_name = ag.get("name", "")
            cmp_id = ag.get("nccCampaignId")
            cmp_name = campaign_name_map.get(cmp_id, "")
            combined = f"{cmp_name} / {ag_name}"
            brand = classify_naver_to_brand(combined)
            adgroup_brand[ag_id] = brand
            adgroup_classify_debug.append({
                "adgroup_id": ag_id, "campaign": cmp_name,
                "adgroup": ag_name, "brand": brand,
            })

        # 브랜드별 adgroup_id 묶음
        brand_groups: dict[str, list[str]] = {"똑똑연구소": [], "롤라루": [], "공통": []}
        for aid, b in adgroup_brand.items():
            brand_groups.setdefault(b, []).append(aid)

        # 3) 구매 전환 데이터 (adgroup_id 포함)
        purchase_df = self.fetch_purchase_range(since, until, progress_cb=progress_cb)

        # 4) 브랜드별 × 일자별 집계
        rows: list[dict] = []
        for brand in ("똑똑연구소", "롤라루"):
            ids = brand_groups.get(brand, [])
            if not ids:
                continue

            # 전환 데이터 (이 브랜드의 adgroup_id 만)
            if not purchase_df.empty:
                b_purchase = purchase_df[purchase_df["adgroup_id"].astype(str).isin([str(i) for i in ids])]
            else:
                b_purchase = pd.DataFrame()

            if not b_purchase.empty:
                b_purchase_daily = (
                    b_purchase.groupby("date")
                    .agg(conversions=("conv_count", "sum"),
                         revenue=("conv_amount", "sum"))
                    .reset_index()
                )
                b_purchase_daily["date"] = pd.to_datetime(
                    b_purchase_daily["date"], format="%Y%m%d"
                ).dt.strftime("%Y-%m-%d")
            else:
                b_purchase_daily = pd.DataFrame(columns=["date", "conversions", "revenue"])

            # /stats 일자별 루프 (이 브랜드의 adgroup_id 만)
            current = since
            while current <= until:
                date_str = current.isoformat()
                stats_list = self.get_stats(ids, date_str)
                totals = {"impCnt": 0, "clkCnt": 0, "salesAmt": 0}
                for entry in stats_list:
                    for k in totals:
                        totals[k] += float(entry.get(k, 0) or 0)

                # 전환 merge
                conv_row = b_purchase_daily[b_purchase_daily["date"] == date_str]
                conversions = int(conv_row["conversions"].sum()) if not conv_row.empty else 0
                revenue = int(conv_row["revenue"].sum()) if not conv_row.empty else 0

                rows.append({
                    "date": date_str,
                    "channel": "네이버",
                    "store": f"네이버_{brand}",
                    "spend": int(round(totals["salesAmt"])),
                    "impressions": int(totals["impCnt"]),
                    "clicks": int(totals["clkCnt"]),
                    "conversions": conversions,
                    "revenue": revenue,
                })
                current += timedelta(days=1)

        df = pd.DataFrame(rows, columns=[
            "date", "channel", "store", "spend", "impressions",
            "clicks", "conversions", "revenue",
        ])

        # '공통' 버킷에 광고비가 실제로 있다면 stderr 로 경고
        unmatched_ids = brand_groups.get("공통", [])
        if unmatched_ids:
            import sys as _sys
            unmatched_names = [
                f"{d['campaign']} / {d['adgroup']}"
                for d in adgroup_classify_debug
                if d["brand"] == "공통"
            ][:5]
            _sys.stderr.write(
                f"[네이버] 브랜드 미분류 광고그룹 {len(unmatched_ids)}개 "
                f"(매출 미포함). 샘플: {unmatched_names}\n"
            )

        return df

    def get_daily_stats_df(self, since: date, until: date,
                           progress_cb=None) -> pd.DataFrame:
        """기간 내 일자별 통계 DataFrame.

        - 노출·클릭·광고비: /stats (generic, 당일 15분~1시간 지연)
        - 전환·전환매출액: /stat-reports AD_CONVERSION_DETAIL → purchase 필터만 집계
          (= 네이버 UI의 "구매완료 전환수" / "구매완료 전환매출액"과 동일 기준)
        """
        adgroups = self.get_adgroups()
        group_ids = [g["nccAdgroupId"] for g in adgroups if not g.get("deleted")]
        total_days = (until - since).days + 1

        # 1. 구매완료 전환 데이터 (일자별 집계용)
        purchase_df = self.fetch_purchase_range(since, until, progress_cb=progress_cb)
        purchase_daily = (
            purchase_df.groupby("date")
            .agg(conversions=("conv_count", "sum"), revenue=("conv_amount", "sum"))
            .reset_index()
            if not purchase_df.empty
            else pd.DataFrame(columns=["date", "conversions", "revenue"])
        )
        # 20260420 → 2026-04-20
        if not purchase_daily.empty:
            purchase_daily["date"] = pd.to_datetime(
                purchase_daily["date"], format="%Y%m%d"
            ).dt.strftime("%Y-%m-%d")

        # 2. 노출·클릭·비용 (/stats 일자별 루프)
        rows = []
        current = since
        while current <= until:
            date_str = current.isoformat()
            stats_list = self.get_stats(group_ids, date_str) if group_ids else []

            totals = {"impCnt": 0, "clkCnt": 0, "salesAmt": 0}
            for entry in stats_list:
                for k in totals:
                    totals[k] += float(entry.get(k, 0) or 0)

            rows.append({
                "date": date_str,
                "channel": "네이버",
                "spend": int(round(totals["salesAmt"])),
                "impressions": int(totals["impCnt"]),
                "clicks": int(totals["clkCnt"]),
            })
            current += timedelta(days=1)

        stats_df = pd.DataFrame(rows)

        # 3. Merge
        merged = stats_df.merge(purchase_daily, on="date", how="left")
        merged["conversions"] = merged["conversions"].fillna(0).astype(int)
        merged["revenue"] = merged["revenue"].fillna(0).astype(int)

        return merged[[
            "date", "channel", "spend", "impressions", "clicks",
            "conversions", "revenue",
        ]]

    def get_stats(self, ids: list[str], date_str: str,
                  fields: list[str] | None = None) -> list[dict]:
        """특정 날짜의 통계 조회 (adgroup 레벨).

        Args:
            ids: 광고그룹 ID 리스트 ['grp-a001-...']
            date_str: 'YYYY-MM-DD'
            fields: 조회 지표. 기본: 노출·클릭·비용·전환·전환매출액

        Returns:
            응답 dict의 data 배열 (엔트리별 adgroup 집계값).
        """
        if not ids:
            return []

        import json as _json
        default_fields = ["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"]
        params = {
            # 네이버 /stats는 ids를 '쉼표 구분' 으로 받음 (JSON array 불가)
            "ids": ",".join(ids),
            "fields": _json.dumps(fields or default_fields),
            "timeRange": _json.dumps({"since": date_str, "until": date_str}),
        }
        try:
            res = self._request("GET", "/stats", params=params) or {}
            return res.get("data", []) if isinstance(res, dict) else []
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return []
            raise



def load_client_from_env() -> NaverSearchAdClient | None:
    """환경변수 (.env 포함) 에서 클라이언트 생성. 없으면 None."""
    import os
    from dotenv import load_dotenv
    from pathlib import Path

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    api_key = os.getenv("NAVER_SEARCHAD_API_KEY", "").strip()
    secret_key = os.getenv("NAVER_SEARCHAD_SECRET_KEY", "").strip()
    customer_id = os.getenv("NAVER_SEARCHAD_CUSTOMER_ID", "").strip()

    if not all([api_key, secret_key, customer_id]):
        return None
    return NaverSearchAdClient(api_key, secret_key, customer_id)


def save_credentials_to_env(api_key: str, secret_key: str, customer_id: str) -> None:
    """API 자격증명을 .env 파일에 저장."""
    from pathlib import Path

    env_path = Path(__file__).parent.parent / ".env"

    # 기존 .env 읽고 네이버 관련 라인만 교체
    existing = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()

    existing["NAVER_SEARCHAD_API_KEY"] = api_key.strip()
    existing["NAVER_SEARCHAD_SECRET_KEY"] = secret_key.strip()
    existing["NAVER_SEARCHAD_CUSTOMER_ID"] = customer_id.strip()

    lines = [f"{k}={v}" for k, v in existing.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
