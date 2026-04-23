"""쿠팡 Wing Open API 클라이언트 (판매자 주문/상품 조회).

공식 문서: https://developers.coupangcorp.com/hc/ko

인증:
    HMAC-SHA256 서명 + Authorization 헤더
    message = f"{yymmdd}T{hhmmss}Z{method}{path_with_query}"
    signature = hex(HMAC-SHA256(secret_key, message))
    Authorization: "CEA algorithm=HmacSHA256, access-key={access_key}, "
                   "signed-date={yymmdd}T{hhmmss}Z, signature={signature}"

필요 자격증명:
    1. Access Key
    2. Secret Key
    3. Vendor ID (판매자 ID, 예: A00xxxxxx)

주요 엔드포인트:
    - /v2/providers/openapi/apis/api/v4/vendors/{vendorId}/ordersheets
      주문서 조회 (최대 7일 범위, 페이징)
"""
from __future__ import annotations

import hashlib
import hmac
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, quote

import pandas as pd
import requests


BASE_URL = "https://api-gateway.coupang.com"


class CoupangWingClient:
    def __init__(self, access_key: str, secret_key: str, vendor_id: str):
        if not all([access_key, secret_key, vendor_id]):
            raise ValueError("Access Key, Secret Key, Vendor ID 모두 필요합니다.")
        self.access_key = access_key.strip()
        self.secret_key = secret_key.strip()
        self.vendor_id = vendor_id.strip()

    # ---------- 서명 ----------
    def _generate_authorization(self, method: str, path: str,
                                query_str: str = "") -> str:
        """HMAC-SHA256 서명 + Authorization 헤더.

        signed-date: YYMMDDTHHMMSSZ (UTC)
        message = signed_date + method + path + query_str  (⚠ '?' 없이 concat)
        """
        now = datetime.now(timezone.utc)
        signed_date = now.strftime("%y%m%dT%H%M%SZ")
        # Coupang 공식 패턴: path와 query를 "?" 없이 직접 붙임
        message = signed_date + method + path + query_str

        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return (
            f"CEA algorithm=HmacSHA256, access-key={self.access_key}, "
            f"signed-date={signed_date}, signature={signature}"
        )

    def _request(self, method: str, path: str, params: dict | None = None,
                 **kwargs) -> Any:
        """HTTP 요청. 서명용 message는 '?' 없이, URL은 '?' 포함."""
        query_str = ""
        if params:
            query_str = urlencode(params, doseq=True, quote_via=quote)

        auth = self._generate_authorization(method, path, query_str)
        headers = {
            "Authorization": auth,
            "X-EXTENDED-TIMEOUT": "90000",
            "Content-Type": "application/json;charset=UTF-8",
        }
        headers.update(kwargs.pop("headers", {}))

        # 실제 HTTP URL은 path + ? + query
        full_url = BASE_URL + path + (f"?{query_str}" if query_str else "")
        resp = requests.request(
            method, full_url,
            headers=headers,
            timeout=60,
            **kwargs,
        )
        resp.raise_for_status()
        return resp.json() if resp.text else None

    # ---------- 공개 메서드 ----------
    def test_connection(self) -> tuple[bool, str]:
        """연결 테스트: 최근 1일 주문 조회 시도."""
        try:
            yesterday = date.today() - timedelta(days=1)
            path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets"
            params = {
                "createdAtFrom": yesterday.strftime("%Y-%m-%d"),
                "createdAtTo": yesterday.strftime("%Y-%m-%d"),
                "status": "FINAL_DELIVERY",  # 배송완료 — 기존 주문 조회 가능
                "maxPerPage": "1",
            }
            res = self._request("GET", path, params=params) or {}
            return True, f"인증 성공 (Vendor {self.vendor_id}, code {res.get('code')})"
        except requests.HTTPError as e:
            code = e.response.status_code
            body = e.response.text[:300] if e.response else ""
            return False, f"HTTP {code}: {body}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    # 쿠팡 주문 상태 (paid 이후 모든 단계 — 최소 하나에 속하게 되므로 모두 순회)
    ORDER_STATUSES = [
        "ACCEPT",         # 주문 접수 (결제 완료)
        "INSTRUCT",       # 상품 준비중
        "DEPARTURE",      # 배송지시
        "DELIVERING",     # 배송중
        "FINAL_DELIVERY", # 배송완료
        "NONE_TRACKING",  # 추적 정보 없음
    ]

    def get_ordersheets(self, since: date, until: date,
                        max_per_page: int = 50,
                        progress_cb=None) -> list[dict]:
        """기간 내 모든 상태의 주문서 조회 + 중복 제거.

        쿠팡 API 제약:
            - status 파라미터 필수 → 모든 상태 순회
            - 7일 범위 제한 → 7일 청크로 루프
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets"

        all_orders: list[dict] = []
        current_since = since

        # 청크 수 계산 (진행률 표시용)
        total_days = (until - since).days + 1
        total_chunks = (total_days + 6) // 7 * len(self.ORDER_STATUSES)
        chunk_idx = 0

        while current_since <= until:
            chunk_until = min(current_since + timedelta(days=6), until)

            for status in self.ORDER_STATUSES:
                chunk_idx += 1
                if progress_cb:
                    progress_cb(f"{current_since}~{chunk_until} {status}",
                                chunk_idx, total_chunks)

                next_token: str | None = None
                while True:
                    params: dict[str, Any] = {
                        "createdAtFrom": current_since.strftime("%Y-%m-%d"),
                        "createdAtTo": chunk_until.strftime("%Y-%m-%d"),
                        "status": status,
                        "maxPerPage": str(max_per_page),
                    }
                    if next_token:
                        params["nextToken"] = next_token

                    try:
                        res = self._request("GET", path, params=params) or {}
                    except requests.HTTPError:
                        break
                    data = res.get("data") or []
                    all_orders.extend(data if isinstance(data, list) else [])

                    next_token = res.get("nextToken") or None
                    if not next_token:
                        break

            current_since = chunk_until + timedelta(days=1)

        # orderId 기준 dedupe (같은 주문이 여러 상태 결과에 걸쳐 나타나면 마지막 상태만)
        seen: set[str] = set()
        unique: list[dict] = []
        for o in all_orders:
            oid = str(o.get("orderId") or "")
            if oid and oid not in seen:
                seen.add(oid)
                unique.append(o)
        return unique

    def get_rocketgrowth_orders(self, since: date, until: date,
                                max_per_page: int = 50) -> list[dict]:
        """로켓그로스 주문 조회 (풀필먼트 주문, 30일 청크로 페이징).

        경로: /rg_open_api/apis/api/v1/vendors/{vendorId}/rg/orders
        범위 제한: 한 번 호출당 약 30일
        """
        path = (f"/v2/providers/rg_open_api/apis/api/v1/vendors/"
                f"{self.vendor_id}/rg/orders")

        all_orders: list[dict] = []
        current_since = since

        while current_since <= until:
            # 30일 청크 (safe margin)
            chunk_until = min(current_since + timedelta(days=29), until)
            next_token: str | None = None

            while True:
                params: dict[str, Any] = {
                    "paidDateFrom": current_since.strftime("%Y%m%d"),
                    "paidDateTo": chunk_until.strftime("%Y%m%d"),
                    "maxPerPage": str(max_per_page),
                }
                if next_token:
                    params["nextToken"] = next_token

                try:
                    res = self._request("GET", path, params=params) or {}
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response else 0
                    if code in (401, 403):
                        raise RuntimeError(
                            "로켓그로스 API 접근 권한 없음. Wing → 판매자 정보 → "
                            "'로켓그로스 상품 API 이용 동의' 클릭 후 재시도하세요."
                        ) from e
                    # 400 등 범위 에러는 청크 건너뛰고 계속
                    break

                data = res.get("data") or []
                all_orders.extend(data if isinstance(data, list) else [])

                next_token = res.get("nextToken") or None
                if not next_token:
                    break

            current_since = chunk_until + timedelta(days=1)

        return all_orders

    def fetch_orders_df(self, since: date, until: date,
                        progress_cb=None) -> pd.DataFrame:
        """업체배송(Wholesale) + 로켓그로스(RG) 주문 통합.

        반환 컬럼: date, order_id, customer_id, channel, store,
                   product, quantity, revenue
        """
        import hashlib

        def _hash_id(raw: Any) -> str:
            if raw is None or str(raw) == "":
                return "CP-UNKNOWN"
            return "CP-" + hashlib.md5(str(raw).encode("utf-8")).hexdigest()[:8].upper()

        rows: list[dict] = []

        # ---------- 1. 업체배송 (ordersheets) ----------
        if progress_cb:
            progress_cb("wholesale", 0, 2)
        try:
            sheets = self.get_ordersheets(since, until)
        except Exception:
            sheets = []

        for s in sheets:
            paid_at = (s.get("paidAt") or s.get("orderedAt") or "")[:10]
            if not paid_at:
                continue
            if paid_at < since.isoformat() or paid_at > until.isoformat():
                continue

            orderer = s.get("orderer") or {}
            buyer_key = (
                orderer.get("ordererNumber") or
                orderer.get("safeNumber") or
                orderer.get("name") or ""
            )

            items = s.get("orderItems") or []
            order_id_base = str(s.get("orderId") or "")
            for item in items:
                product_name = item.get("productName") or item.get("sellerProductName") or ""
                qty = int(item.get("shippingCount") or item.get("ordersCount") or 1)
                amount = int(
                    item.get("salesPrice")
                    or item.get("orderPrice")
                    or item.get("discountPrice")
                    or 0
                ) * qty

                rows.append({
                    "date": paid_at,
                    "order_id": f"W-{order_id_base}-{item.get('vendorItemId', '')}",
                    "customer_id": _hash_id(buyer_key),
                    "channel": "쿠팡",
                    "store": "쿠팡",
                    "product": str(product_name),
                    "quantity": qty,
                    "revenue": amount,
                })

        # ---------- 2. 로켓그로스 ----------
        if progress_cb:
            progress_cb("rocketgrowth", 1, 2)
        rg_orders = self.get_rocketgrowth_orders(since, until)

        for r in rg_orders:
            # paidAt: epoch-ms
            paid_at_raw = r.get("paidAt")
            if not paid_at_raw:
                continue
            try:
                # 문자열 또는 숫자 — epoch-ms를 KST 날짜로
                paid_ms = int(paid_at_raw)
                paid_dt_utc = datetime.fromtimestamp(paid_ms / 1000, tz=timezone.utc)
                paid_dt_kst = paid_dt_utc + timedelta(hours=9)
                paid_date = paid_dt_kst.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue

            if paid_date < since.isoformat() or paid_date > until.isoformat():
                continue

            items = r.get("orderItems") or []
            order_id_base = str(r.get("orderId") or "")
            for item in items:
                qty = int(float(item.get("salesQuantity") or 1))
                unit_price = int(float(
                    item.get("unitSalesPrice")
                    or item.get("salesPrice")
                    or 0
                ))
                amount = qty * unit_price

                # RG는 고객 정보 노출 X → 주문 단위로 익명 ID (재구매 추적 불가)
                rg_cust = _hash_id(f"RG-{order_id_base}")

                rows.append({
                    "date": paid_date,
                    "order_id": f"RG-{order_id_base}-{item.get('vendorItemId', '')}",
                    "customer_id": rg_cust,
                    "channel": "쿠팡",
                    "store": "쿠팡",
                    "product": str(item.get("productName") or ""),
                    "quantity": qty,
                    "revenue": amount,
                })

        if progress_cb:
            progress_cb("done", 2, 2)

        return pd.DataFrame(rows, columns=[
            "date", "order_id", "customer_id", "channel",
            "store", "product", "quantity", "revenue",
        ])


    # ---------- 상품 조회 (이미지 캐시용) ----------
    def get_seller_products_summary(self, max_per_page: int = 50) -> list[dict]:
        """모든 판매자 상품 요약 페이징 조회."""
        path = "/v2/providers/seller_api/apis/api/v1/marketplace/seller-products"
        all_products: list[dict] = []
        next_token: str | None = "1"

        while next_token:
            params = {
                "vendorId": self.vendor_id,
                "nextToken": next_token,
                "maxPerPage": str(max_per_page),
            }
            try:
                res = self._request("GET", path, params=params) or {}
            except requests.HTTPError:
                break
            items = res.get("data") or []
            all_products.extend(items if isinstance(items, list) else [])
            nt = res.get("nextToken")
            if not nt or nt == next_token:
                break
            next_token = nt

        return all_products

    def get_product_detail(self, seller_product_id: int) -> dict | None:
        """단일 상품 상세 조회 (이미지 포함)."""
        path = f"/v2/providers/seller_api/apis/api/v1/marketplace/seller-products/{seller_product_id}"
        try:
            res = self._request("GET", path) or {}
        except requests.HTTPError:
            return None
        return res.get("data")

    def get_product_images(self, progress_cb=None) -> list[dict]:
        """쿠팡 상품 이미지 추출 → [(name, image_url, seller_product_id), ...]."""
        IMAGE_CDN = "https://image9.coupangcdn.com/image/"

        products = self.get_seller_products_summary()
        total = len(products)
        results: list[dict] = []

        for i, summary in enumerate(products, 1):
            if progress_cb:
                progress_cb(i, total)

            pid = summary.get("sellerProductId")
            if not pid:
                continue
            detail = self.get_product_detail(pid)
            if not detail:
                continue

            name = (detail.get("sellerProductName")
                    or summary.get("sellerProductName") or "").strip()
            if not name:
                continue

            # 첫 item의 REPRESENTATION 이미지
            image_url: str | None = None
            for item in detail.get("items", []):
                for img in item.get("images", []):
                    if img.get("imageType") == "REPRESENTATION" and img.get("cdnPath"):
                        image_url = IMAGE_CDN + img["cdnPath"]
                        break
                if image_url:
                    break

            if not image_url:
                continue

            results.append({
                "name": name,
                "image_url": image_url,
                "seller_product_id": pid,
            })

        return results


def load_coupang_client_from_env() -> CoupangWingClient | None:
    """환경변수 (.env)에서 클라이언트 생성."""
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

    ak = os.getenv("COUPANG_ACCESS_KEY", "").strip()
    sk = os.getenv("COUPANG_SECRET_KEY", "").strip()
    vid = os.getenv("COUPANG_VENDOR_ID", "").strip()

    if not all([ak, sk, vid]):
        return None
    return CoupangWingClient(ak, sk, vid)


def save_coupang_credentials(access_key: str, secret_key: str, vendor_id: str) -> None:
    from pathlib import Path
    env_path = Path(__file__).parent.parent / ".env"

    existing: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()

    existing["COUPANG_ACCESS_KEY"] = access_key.strip()
    existing["COUPANG_SECRET_KEY"] = secret_key.strip()
    existing["COUPANG_VENDOR_ID"] = vendor_id.strip()

    lines = [f"{k}={v}" for k, v in existing.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
