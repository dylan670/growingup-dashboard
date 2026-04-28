"""Cafe24 자사몰 API 클라이언트 (OAuth 2.0).

공식 문서: https://developers.cafe24.com/docs/api/admin/

인증:
    - Private App OAuth 2.0 (authorization_code grant)
    - 1회 수동 인증 → access_token + refresh_token 발급
    - access_token 2시간 후 만료 → refresh_token으로 자동 갱신
    - refresh_token도 만료 시 재인증 필요 (약 2주)

필요 자격증명 (스토어당):
    - mall_id (쇼핑몰 ID, 관리자 URL의 서브도메인)
    - client_id, client_secret (developers.cafe24.com에서 발급)
    - (OAuth 후) access_token, refresh_token, expires_at

토큰은 `data/cafe24_tokens.json`에 저장 (gitignore).
"""
from __future__ import annotations

import base64
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests


TOKEN_FILE = Path(__file__).parent.parent / "data" / "cafe24_tokens.json"

# OAuth 스코프
DEFAULT_SCOPES = [
    "mall.read_application",
    "mall.write_application",
    "mall.read_order",
    "mall.read_product",
    "mall.read_customer",
]


def mall_redirect_uri(mall_id: str, path: str = "/order/basket.html") -> str:
    """각 mall의 자체 도메인 기반 Redirect URI.

    path는 dev center에 등록한 값과 정확히 일치해야 함.
    """
    return f"https://{mall_id}.cafe24.com{path}"


class Cafe24Client:
    def __init__(self, mall_id: str, client_id: str, client_secret: str,
                 store_label: str = ""):
        self.mall_id = mall_id.strip()
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.store_label = store_label

        # 토큰 캐시 (파일에서 로드)
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at: datetime | None = None
        self._load_tokens()

    # ---------- OAuth URL 생성 ----------
    @property
    def base_url(self) -> str:
        return f"https://{self.mall_id}.cafe24api.com"

    def authorize_url(self, redirect_uri: str,
                      scopes: list[str] | None = None,
                      state: str = "grow_up_dashboard") -> str:
        """사용자가 브라우저에서 클릭할 OAuth 인증 URL."""
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "state": state,
            "redirect_uri": redirect_uri,
            "scope": ",".join(scopes or DEFAULT_SCOPES),
        }
        return f"{self.base_url}/api/v2/oauth/authorize?{urlencode(params)}"

    def exchange_code_for_token(self, code: str, redirect_uri: str) -> dict:
        """authorization_code → access_token + refresh_token 교환."""
        basic = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("ascii")

        resp = requests.post(
            f"{self.base_url}/api/v2/oauth/token",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._store_token_response(data)
        return data

    def refresh(self) -> dict:
        """refresh_token을 이용해 access_token 갱신."""
        if not self._refresh_token:
            raise RuntimeError("refresh_token이 없습니다. 초기 OAuth 인증 필요.")

        basic = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("ascii")

        resp = requests.post(
            f"{self.base_url}/api/v2/oauth/token",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._store_token_response(data)
        return data

    def _store_token_response(self, data: dict) -> None:
        self._access_token = data.get("access_token")
        self._refresh_token = data.get("refresh_token") or self._refresh_token
        exp_str = data.get("expires_at")  # Cafe24는 "2026-04-21T15:30:00.000"
        if exp_str:
            try:
                # 다양한 포맷 대응
                self._expires_at = datetime.fromisoformat(exp_str.replace("Z", ""))
            except Exception:
                self._expires_at = datetime.now() + timedelta(hours=2)
        else:
            expires_in = int(data.get("expires_in", 7200))
            self._expires_at = datetime.now() + timedelta(seconds=expires_in)
        self._save_tokens()

    # ---------- 토큰 파일 영속화 ----------
    def _load_tokens(self) -> None:
        if not TOKEN_FILE.exists():
            return
        try:
            with open(TOKEN_FILE, encoding="utf-8") as f:
                all_tokens = json.load(f)
            my = all_tokens.get(self.mall_id) or {}
            self._access_token = my.get("access_token")
            self._refresh_token = my.get("refresh_token")
            exp_str = my.get("expires_at")
            if exp_str:
                self._expires_at = datetime.fromisoformat(exp_str)
        except Exception:
            pass

    def _save_tokens(self) -> None:
        TOKEN_FILE.parent.mkdir(exist_ok=True)
        all_tokens: dict = {}
        if TOKEN_FILE.exists():
            try:
                with open(TOKEN_FILE, encoding="utf-8") as f:
                    all_tokens = json.load(f)
            except Exception:
                pass
        all_tokens[self.mall_id] = {
            "access_token": self._access_token,
            "refresh_token": self._refresh_token,
            "expires_at": self._expires_at.isoformat() if self._expires_at else None,
        }
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(all_tokens, f, indent=2, ensure_ascii=False)

    # ---------- API 호출 공통 ----------
    def _ensure_valid_token(self) -> str:
        """만료 임박 시 자동 갱신."""
        if not self._access_token:
            raise RuntimeError("access_token 없음. OAuth 인증 먼저 완료하세요.")
        if self._expires_at and datetime.now() >= self._expires_at - timedelta(minutes=5):
            self.refresh()
        return self._access_token

    def _auth_headers(self) -> dict:
        token = self._ensure_valid_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            # 2026-03-01 현재 Cafe24 API 최신 안정 버전 (2024-06-01 은 폐기됨)
            "X-Cafe24-Api-Version": "2026-03-01",
        }

    def _request(self, method: str, path: str, params: dict | None = None,
                 json_body: dict | None = None) -> Any:
        url = f"{self.base_url}/api/v2/admin{path}"
        resp = requests.request(
            method, url,
            headers=self._auth_headers(),
            params=params, json=json_body, timeout=30,
        )
        resp.raise_for_status()
        return resp.json() if resp.text else None

    # ---------- 공개 메서드 ----------
    def test_connection(self) -> tuple[bool, str]:
        try:
            self._ensure_valid_token()
            # 어제 하루 주문 조회 (날짜 range 필수)
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            res = self._request("GET", "/orders", params={
                "start_date": yesterday,
                "end_date": yesterday,
                "limit": 1,
            })
            count = len(res.get("orders", [])) if res else 0
            return True, f"인증 성공 (mall: {self.mall_id}, 어제 주문 {count}건)"
        except requests.HTTPError as e:
            code = e.response.status_code if e.response else 0
            body = e.response.text[:300] if e.response else ""
            if code in (401, 403):
                return False, f"인증 실패 HTTP {code}. 재인증 필요. {body}"
            return False, f"HTTP {code}: {body}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def get_orders(self, since: date, until: date,
                   limit: int = 100) -> list[dict]:
        """기간 내 주문 조회 (페이징)."""
        all_orders: list[dict] = []
        offset = 0
        # Cafe24 API v2026-03-01 기준 — order_status 필드 제거됨.
        # 서버 필터 없이 전체 수집 후 취소/반품은 amount 로 구분.
        while True:
            params = {
                "start_date": since.isoformat(),
                "end_date": until.isoformat(),
                "limit": limit,
                "offset": offset,
                "embed": "items",
            }
            res = self._request("GET", "/orders", params=params) or {}
            batch = res.get("orders", []) or []
            all_orders.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
            if offset > 10000:
                break
        return all_orders

    def get_products(self, limit: int = 100) -> list[dict]:
        """판매 중인 전체 상품 조회 (페이징)."""
        all_products: list[dict] = []
        offset = 0
        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "display": "T",  # 진열 중만
                "selling": "T",  # 판매 중만
            }
            res = self._request("GET", "/products", params=params) or {}
            batch = res.get("products", []) or []
            all_products.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
            if offset > 10000:
                break
        return all_products

    def fetch_orders_df(self, since: date, until: date, store: str,
                        progress_cb=None) -> pd.DataFrame:
        """주문 → 대시보드 orders.csv 스키마."""
        import hashlib

        def _hash(raw: Any) -> str:
            if not raw:
                return "CF-UNKNOWN"
            return "CF-" + hashlib.md5(str(raw).encode("utf-8")).hexdigest()[:8].upper()

        if progress_cb:
            progress_cb("fetching orders", 0, 1)
        orders = self.get_orders(since, until)
        if progress_cb:
            progress_cb("fetching orders", 1, 1)

        rows: list[dict] = []
        for o in orders:
            paid_date = (o.get("payment_date") or o.get("order_date") or "")[:10]
            if not paid_date:
                continue
            if paid_date < since.isoformat() or paid_date > until.isoformat():
                continue

            member_id = o.get("member_id") or o.get("billing_name") or ""
            order_id_base = str(o.get("order_id") or "")

            items = o.get("items", []) or []
            if not items:
                # item 없으면 주문 전체 값으로 한 행
                rows.append({
                    "date": paid_date,
                    "order_id": order_id_base,
                    "customer_id": _hash(member_id),
                    "channel": "자사몰",
                    "store": store,
                    "product": o.get("product_name", "") or "미지정",
                    "quantity": 1,
                    "revenue": int(float(o.get("actual_payment_amount", 0) or 0)),
                })
                continue

            for item in items:
                qty = int(float(item.get("quantity", 1) or 1))
                unit = float(item.get("product_price") or item.get("price") or 0)
                amount = int(round(qty * unit))

                # 옵션 추출 — Cafe24 응답에 다양한 키로 들어옴
                option_value = (
                    item.get("option_value")
                    or item.get("option_text")
                    or item.get("option_name")
                    or ""
                )
                # 가끔 list 형태로 옴 — 문자열로 직렬화
                if isinstance(option_value, list):
                    option_value = " / ".join(
                        str(o.get("name", "") if isinstance(o, dict) else o)
                        for o in option_value
                    )
                option_str = str(option_value).strip()

                rows.append({
                    "date": paid_date,
                    "order_id": f"{order_id_base}-{item.get('shipping_code', '')}",
                    "customer_id": _hash(member_id),
                    "channel": "자사몰",
                    "store": store,
                    "product": str(item.get("product_name") or ""),
                    "option": option_str,
                    "quantity": qty,
                    "revenue": amount,
                })

        return pd.DataFrame(rows, columns=[
            "date", "order_id", "customer_id", "channel",
            "store", "product", "option", "quantity", "revenue",
        ])

    def get_product_images(self) -> list[dict]:
        """상품 + 대표 이미지 URL."""
        products = self.get_products()
        results: list[dict] = []
        for p in products:
            name = (p.get("product_name") or "").strip()
            img = (p.get("detail_image")
                   or p.get("list_image")
                   or p.get("tiny_image") or "")
            if name and img:
                results.append({
                    "name": name,
                    "image_url": img,
                    "product_no": p.get("product_no"),
                })
        return results


# ==========================================================
# 멀티 스토어 로더
# ==========================================================

def _env_suffix_for_store(store: str) -> str:
    """스토어 이름 → 환경변수 suffix."""
    mapping = {"똑똑연구소": "DDOK", "롤라루": "ROLLA", "루티니스트": "RUTI"}
    return mapping.get(store, store.upper())


def load_cafe24_client(store_brand: str) -> Cafe24Client | None:
    """스토어 브랜드 (똑똑연구소 / 롤라루 / 루티니스트) Cafe24 클라이언트 로드.

    환경변수 (2가지 모드 지원):
      - 공유 (추천): CAFE24_CLIENT_ID, CAFE24_CLIENT_SECRET + CAFE24_MALL_ID_{SUFFIX}
      - 분리:       CAFE24_CLIENT_ID_{SUFFIX}, CAFE24_CLIENT_SECRET_{SUFFIX}, CAFE24_MALL_ID_{SUFFIX}
    """
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

    suffix = _env_suffix_for_store(store_brand)
    mall_id = os.getenv(f"CAFE24_MALL_ID_{suffix}", "").strip()

    # per-mall credential 우선, 없으면 shared fallback
    cid = (os.getenv(f"CAFE24_CLIENT_ID_{suffix}", "").strip()
           or os.getenv("CAFE24_CLIENT_ID", "").strip())
    cs = (os.getenv(f"CAFE24_CLIENT_SECRET_{suffix}", "").strip()
          or os.getenv("CAFE24_CLIENT_SECRET", "").strip())

    if not all([mall_id, cid, cs]):
        return None
    return Cafe24Client(mall_id, cid, cs, store_label=store_brand)


def load_all_cafe24_clients() -> dict[str, Cafe24Client]:
    """등록된 모든 Cafe24 클라이언트 반환. key = 자사몰 store 이름."""
    clients: dict[str, Cafe24Client] = {}
    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        c = load_cafe24_client(brand)
        if c is not None:
            # store 레이블은 '자사몰_{브랜드}'
            store_name = f"자사몰_{brand}"
            c.store_label = store_name
            clients[store_name] = c
    return clients


def save_cafe24_credentials(store_brand: str, mall_id: str,
                            client_id: str, client_secret: str) -> None:
    """Cafe24 자격증명 .env 저장."""
    from pathlib import Path

    env_path = Path(__file__).parent.parent / ".env"
    suffix = _env_suffix_for_store(store_brand)

    existing: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()

    existing[f"CAFE24_MALL_ID_{suffix}"] = mall_id.strip()
    existing[f"CAFE24_CLIENT_ID_{suffix}"] = client_id.strip()
    existing[f"CAFE24_CLIENT_SECRET_{suffix}"] = client_secret.strip()

    lines = [f"{k}={v}" for k, v in existing.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
