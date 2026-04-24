"""네이버 커머스 API 클라이언트 (스마트스토어 주문·정산 자동 수집).

공식 문서: https://apicenter.commerce.naver.com/

인증:
    OAuth 2.0 client_credentials + bcrypt 서명.
    서명: base64( bcrypt.hashpw(f"{client_id}_{timestamp}", client_secret) )

필요한 자격증명 (스토어당):
    1. Client ID     (애플리케이션 ID)
    2. Client Secret (bcrypt salt 형식 — $2a$... 로 시작)

토큰 만료: 약 1시간 (3600초)

스토어 2개 운영 시 각각 별도 애플리케이션 등록 후 4개 키 보관.
"""
from __future__ import annotations

import base64
import time
from datetime import date, datetime, timedelta
from typing import Any

import bcrypt
import pandas as pd
import requests


BASE_URL = "https://api.commerce.naver.com"


class NaverCommerceClient:
    """단일 스토어의 커머스 API 클라이언트."""

    def __init__(self, client_id: str, client_secret: str, store_label: str = ""):
        if not client_id or not client_secret:
            raise ValueError("Client ID와 Secret 모두 필요합니다.")
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.store_label = store_label
        self._token: str | None = None
        self._token_expires: float = 0.0

    # ---------- 인증 ----------
    def _generate_signature(self, timestamp: int) -> str:
        """bcrypt 서명 생성. client_secret이 bcrypt salt 역할."""
        password = f"{self.client_id}_{timestamp}"
        hashed = bcrypt.hashpw(
            password.encode("utf-8"),
            self.client_secret.encode("utf-8"),
        )
        return base64.standard_b64encode(hashed).decode("utf-8")

    def authenticate(self) -> str:
        """OAuth access_token 발급. 캐시 만료 시 자동 갱신."""
        if self._token and time.time() < self._token_expires - 60:
            return self._token

        timestamp = int(time.time() * 1000)
        sign = self._generate_signature(timestamp)

        resp = requests.post(
            f"{BASE_URL}/external/v1/oauth2/token",
            headers={"content-type": "application/x-www-form-urlencoded"},
            data={
                "client_id": self.client_id,
                "timestamp": timestamp,
                "grant_type": "client_credentials",
                "client_secret_sign": sign,
                "type": "SELF",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        # expires_in은 초 단위 (보통 3600)
        self._token_expires = time.time() + int(data.get("expires_in", 3600))
        return self._token

    def _auth_headers(self, extra: dict | None = None) -> dict:
        token = self.authenticate()
        headers = {
            "Authorization": f"Bearer {token}",
            "content-type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def get_products(self, size: int = 100) -> list[dict]:
        """스토어의 모든 채널상품 페이징 조회."""
        all_products: list[dict] = []
        page = 1
        while True:
            resp = requests.post(
                f"{BASE_URL}/external/v1/products/search",
                headers=self._auth_headers(),
                json={"size": size, "page": page, "orderType": "NO"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json() or {}
            contents = data.get("contents", [])
            if not contents:
                break
            all_products.extend(contents)
            total = data.get("totalElements", 0)
            if len(all_products) >= total:
                break
            page += 1
        return all_products

    # ---------- 공개 메서드 ----------
    def test_connection(self) -> tuple[bool, str]:
        try:
            self.authenticate()
            ttl = int(self._token_expires - time.time())
            return True, f"OAuth 인증 성공 (토큰 {ttl}초 유효)"
        except requests.HTTPError as e:
            code = e.response.status_code
            body = e.response.text[:300] if e.response else ""
            if code in (400, 401, 403):
                return False, (
                    f"인증 실패 HTTP {code}. Client ID / Secret 확인 필요.\n응답: {body}"
                )
            return False, f"HTTP {code}: {body}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def get_changed_orders(self, last_changed_from: str,
                           last_changed_to: str | None = None,
                           last_changed_type: str | None = None) -> list[dict]:
        """기간 내 상태 변경된 주문 조회.

        Args:
            last_changed_from: ISO8601 + 타임존 (예: '2026-04-18T00:00:00.000+09:00')
            last_changed_to: 동일 형식. 생략 시 지금
            last_changed_type: 'PAYED', 'DISPATCHED' 등 필터 (생략 시 전체)

        Returns:
            [{'productOrderId': ..., 'productOrderStatus': ..., ...}, ...]
        """
        params: dict[str, Any] = {"lastChangedFrom": last_changed_from}
        if last_changed_to:
            params["lastChangedTo"] = last_changed_to
        if last_changed_type:
            params["lastChangedType"] = last_changed_type

        resp = requests.get(
            f"{BASE_URL}/external/v1/pay-order/seller/product-orders/last-changed-statuses",
            headers=self._auth_headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json() or {}
        # 응답 형태 대응 (data.lastChangeStatuses 또는 data 직접)
        if isinstance(data, dict):
            content = data.get("data") or {}
            if isinstance(content, dict):
                return content.get("lastChangeStatuses", []) or []
            if isinstance(content, list):
                return content
        return []

    def get_order_details(self, product_order_ids: list[str]) -> list[dict]:
        """주문 상세 일괄 조회 (한 번에 최대 300건)."""
        if not product_order_ids:
            return []

        all_details: list[dict] = []
        batch_size = 300
        for i in range(0, len(product_order_ids), batch_size):
            batch = product_order_ids[i:i + batch_size]
            resp = requests.post(
                f"{BASE_URL}/external/v1/pay-order/seller/product-orders/query",
                headers=self._auth_headers(),
                json={"productOrderIds": batch},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json() or {}
            content = data.get("data") if isinstance(data, dict) else data
            if isinstance(content, list):
                all_details.extend(content)
            elif isinstance(content, dict):
                # 가능한 래핑 키들
                for key in ("productOrders", "orders", "items"):
                    if key in content:
                        all_details.extend(content[key])
                        break
        return all_details

    def fetch_orders_df(self, since: date, until: date,
                        store: str,
                        progress_cb=None) -> pd.DataFrame:
        """기간 내 주문을 대시보드 orders.csv 스키마로 반환.

        네이버 API 제약: last-changed-statuses는 한 번에 24시간까지만 조회 가능.
        → 일별 루프로 기간 커버.

        반환 컬럼: date, order_id, customer_id, channel, store,
                   product, quantity, revenue
        """
        import hashlib

        def _hash_id(raw: Any) -> str:
            if raw is None or str(raw) == "":
                return "NS-UNKNOWN"
            return "NS-" + hashlib.md5(str(raw).encode("utf-8")).hexdigest()[:8].upper()

        # 1. 일별 24h 청크로 변경 이력 수집
        total_days = (until - since).days + 1
        all_changed: list[dict] = []
        current = since
        day_idx = 0

        while current <= until:
            day_idx += 1
            if progress_cb:
                progress_cb(f"changes-{current}", day_idx, total_days)

            from_str = f"{current.isoformat()}T00:00:00.000+09:00"
            next_day = current + timedelta(days=1)
            to_str = f"{next_day.isoformat()}T00:00:00.000+09:00"

            try:
                changed = self.get_changed_orders(from_str, to_str)
                all_changed.extend(changed)
            except requests.HTTPError:
                # 하루 실패해도 진행 (로그는 상위에서)
                pass

            current += timedelta(days=1)

        # 2. productOrderId 중복 제거 + paymentDate 있는 것만
        seen: set[str] = set()
        order_ids: list[str] = []
        for c in all_changed:
            oid = c.get("productOrderId")
            if not oid or oid in seen:
                continue
            if c.get("paymentDate"):  # 결제된 주문만
                order_ids.append(str(oid))
                seen.add(oid)

        if progress_cb:
            progress_cb("details-fetch", 0, len(order_ids))

        # 3. 상세 일괄 조회 (300건 배치)
        details = self.get_order_details(order_ids) if order_ids else []

        if progress_cb:
            progress_cb("details-fetch", len(order_ids), len(order_ids))

        # 4. 스키마 변환
        rows: list[dict] = []
        for d in details:
            # 응답 래핑 키 다양하게 대응
            po = d.get("productOrder") if isinstance(d, dict) else None
            order = d.get("order") if isinstance(d, dict) else None
            src = d if not (po or order) else {**d, **(po or {}), **(order or {})}

            pay_date_raw = src.get("paymentDate") or src.get("orderDate") or ""
            pay_date = pay_date_raw[:10] if pay_date_raw else ""
            if not pay_date:
                continue

            # 기간 필터
            if pay_date < since.isoformat() or pay_date > until.isoformat():
                continue

            buyer_key = (
                src.get("ordererId") or src.get("ordererNo") or
                src.get("buyerId") or src.get("buyerNo") or
                src.get("ordererName") or src.get("buyerName") or ""
            )

            total_amt = (
                src.get("totalPaymentAmount") or
                src.get("totalProductAmount") or
                src.get("paymentAmount") or 0
            )

            rows.append({
                "date": pay_date,
                "order_id": str(src.get("productOrderId") or src.get("orderNo") or ""),
                "customer_id": _hash_id(buyer_key),
                "channel": "네이버",
                "store": store,
                "product": str(src.get("productName") or ""),
                "quantity": int(src.get("quantity") or 1),
                "revenue": int(total_amt or 0),
            })

        return pd.DataFrame(rows, columns=[
            "date", "order_id", "customer_id", "channel",
            "store", "product", "quantity", "revenue",
        ])


# ---------- 멀티 스토어 로더 ----------

def load_commerce_clients_from_env() -> dict[str, NaverCommerceClient]:
    """환경변수 (.env 포함)에서 스토어별 클라이언트 dict 생성.

    읽는 변수 (각 스토어 쌍):
        NAVER_COMMERCE_CLIENT_ID_DDOK / _SECRET_DDOK       (똑똑연구소)
        NAVER_COMMERCE_CLIENT_ID_ROLLA / _SECRET_ROLLA     (롤라루)
        NAVER_COMMERCE_CLIENT_ID_RUTI / _SECRET_RUTI       (루티니스트)

    Returns:
        {store_name: client} — 등록된 스토어만 포함. 비어있을 수 있음.
    """
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

    config = [
        ("똑똑연구소", "NAVER_COMMERCE_CLIENT_ID_DDOK", "NAVER_COMMERCE_CLIENT_SECRET_DDOK"),
        ("롤라루", "NAVER_COMMERCE_CLIENT_ID_ROLLA", "NAVER_COMMERCE_CLIENT_SECRET_ROLLA"),
        ("루티니스트", "NAVER_COMMERCE_CLIENT_ID_RUTI", "NAVER_COMMERCE_CLIENT_SECRET_RUTI"),
    ]

    clients: dict[str, NaverCommerceClient] = {}
    for store, id_key, secret_key in config:
        cid = os.getenv(id_key, "").strip()
        cs = os.getenv(secret_key, "").strip()
        if cid and cs:
            clients[store] = NaverCommerceClient(cid, cs, store_label=store)
    return clients


def save_commerce_credentials(store: str, client_id: str, client_secret: str) -> None:
    """한 스토어 자격증명을 .env에 저장."""
    from pathlib import Path

    env_path = Path(__file__).parent.parent / ".env"

    suffix_map = {"똑똑연구소": "DDOK", "롤라루": "ROLLA", "루티니스트": "RUTI"}
    suffix = suffix_map.get(store)
    if suffix is None:
        raise ValueError(f"지원하지 않는 스토어: {store}")

    existing: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()

    existing[f"NAVER_COMMERCE_CLIENT_ID_{suffix}"] = client_id.strip()
    existing[f"NAVER_COMMERCE_CLIENT_SECRET_{suffix}"] = client_secret.strip()

    lines = [f"{k}={v}" for k, v in existing.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
