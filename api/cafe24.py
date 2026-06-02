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
    "mall.read_store",
    "mall.read_salesreport",
    "mall.read_community",
    "mall.write_community",   # 답글 작성용
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
        """파일 우선 → env (CAFE24_TOKENS_JSON) fallback.

        Cloud 에서는 파일이 ephemeral 하고 env_bootstrap 의 파일 복원이
        타이밍/경로 문제로 실패할 수 있어 env 도 직접 본다.
        """
        # 1. 파일에서 로드 시도
        if TOKEN_FILE.exists():
            try:
                with open(TOKEN_FILE, encoding="utf-8") as f:
                    all_tokens = json.load(f)
                my = all_tokens.get(self.mall_id) or {}
                self._access_token = my.get("access_token")
                self._refresh_token = my.get("refresh_token")
                exp_str = my.get("expires_at")
                if exp_str:
                    self._expires_at = datetime.fromisoformat(exp_str)
                if self._access_token:
                    return
            except Exception:
                pass

        # 2. env fallback — CAFE24_TOKENS_JSON (Streamlit Cloud Secrets)
        import os as _os
        tokens_env = _os.getenv("CAFE24_TOKENS_JSON", "").strip()
        if tokens_env:
            try:
                all_tokens = json.loads(tokens_env)
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

    # ============================================================
    # 리뷰 (상품후기) 수집
    # ============================================================
    def fetch_reviews_df(
        self, since: date, until: date, store: str,
        board_no: int = 4, limit: int = 100,
    ) -> pd.DataFrame:
        """기간 내 상품 후기 수집.

        시도 순서:
            1. /api/v2/admin/reviews        (review API 있는 몰)
            2. /api/v2/admin/boards/{board_no}/articles  (게시판 fallback)
               board_no=4 가 기본 '상품후기' (몰마다 다를 수 있음)

        반환 스키마 (reviews.csv 와 동일):
            date, channel, brand, product, rating, text
        """
        brand = self._brand_of_store(store)

        # 상품번호 → 이름 매핑 (리뷰에 상품번호만 와도 이름 추출용)
        product_name_map: dict[int, str] = {}
        try:
            for p in self.get_products():
                pno = p.get("product_no")
                pname = (p.get("product_name") or "").strip()
                if pno and pname:
                    product_name_map[int(pno)] = pname
        except Exception:
            pass

        rows: list[dict] = []

        # ---- 시도 1: /admin/reviews ----
        review_endpoint_ok = False
        used_board_no = 0   # /reviews 경로면 board_no=0 (답글 대상 아님)
        try:
            offset = 0
            while True:
                params = {
                    "limit": limit,
                    "offset": offset,
                    "start_date": since.isoformat(),
                    "end_date": until.isoformat(),
                }
                res = self._request("GET", "/reviews", params=params) or {}
                batch = res.get("reviews", []) or []
                review_endpoint_ok = True
                for r in batch:
                    rows.append(self._normalize_review(
                        r, brand, product_name_map,
                        board_no=0, mall_id=self.mall_id,
                    ))
                if len(batch) < limit:
                    break
                offset += limit
                if offset > 10000:
                    break
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code not in (404, 405):
                raise

        # ---- 시도 2: 게시판 articles (review endpoint 없을 때만) ----
        if not review_endpoint_ok:
            used_board_no = board_no
            offset = 0
            while True:
                params = {
                    "limit": limit,
                    "offset": offset,
                    "start_date": since.isoformat(),
                    "end_date": until.isoformat(),
                }
                try:
                    res = self._request(
                        "GET", f"/boards/{board_no}/articles", params=params,
                    ) or {}
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else 0
                    raise RuntimeError(
                        f"Cafe24 리뷰 API 접근 불가 (HTTP {code}). "
                        f"OAuth scope 'mall.read_community' 필요 또는 "
                        f"게시판 번호 (현재 board_no={board_no}) 확인."
                    )
                batch = res.get("articles", []) or []
                for r in batch:
                    rows.append(self._normalize_review(
                        r, brand, product_name_map,
                        board_no=used_board_no, mall_id=self.mall_id,
                    ))
                if len(batch) < limit:
                    break
                offset += limit
                if offset > 10000:
                    break

        # None 행 제거
        rows = [r for r in rows if r is not None]
        return pd.DataFrame(rows, columns=[
            "date", "channel", "brand", "product", "rating", "text",
            "mall_id", "board_no", "article_no", "image_urls",
        ])

    @staticmethod
    def _normalize_review(
        r: dict, brand: str, product_name_map: dict[int, str],
        board_no: int = 0, mall_id: str = "",
    ) -> dict | None:
        """카페24 리뷰 1건 → reviews.csv 스키마.

        board_no / mall_id / article_no 는 답글 작성 시 필요한 식별자.
        """
        # 날짜
        d_str = (
            r.get("created_date")
            or r.get("written_date")
            or r.get("write_date")
            or r.get("input_date")
            or ""
        )
        d_str = str(d_str)[:10]
        if not d_str:
            return None

        # 별점
        rating = (
            r.get("rating")
            or r.get("star_score")
            or r.get("score")
            or r.get("review_score")
            or 0
        )
        try:
            rating = int(float(rating))
        except (TypeError, ValueError):
            rating = 0
        if not (1 <= rating <= 5):
            return None

        # 본문
        text = (
            r.get("content")
            or r.get("review_content")
            or r.get("article_content")
            or ""
        )
        # HTML 태그 간단 제거
        import re as _re
        text = _re.sub(r"<[^>]+>", " ", str(text))
        text = _re.sub(r"\s+", " ", text).strip()
        if not text:
            return None

        # 상품명
        product = (
            r.get("product_name")
            or r.get("item_name")
            or ""
        )
        if not product:
            pno = r.get("product_no")
            if pno:
                try:
                    product = product_name_map.get(int(pno), "")
                except (TypeError, ValueError):
                    pass

        # 답글 작성용 식별자
        article_no = r.get("article_no") or r.get("review_no") or 0
        try:
            article_no = int(article_no)
        except (TypeError, ValueError):
            article_no = 0

        # 첨부 이미지 URL — list 로 옴, protocol-relative URL 도 정규화
        image_urls: list[str] = []
        for fld in ("attach_file_urls", "attached_file_urls",
                    "image_urls", "attached_images"):
            v = r.get(fld)
            if isinstance(v, list):
                for item in v:
                    u = ""
                    if isinstance(item, dict):
                        u = item.get("url") or item.get("image_url") or ""
                    elif isinstance(item, str):
                        u = item
                    u = str(u).strip()
                    if u.startswith("//"):
                        u = "https:" + u
                    elif u.startswith("/"):
                        u = "https://" + u.lstrip("/")
                    if u:
                        image_urls.append(u)
                break  # 첫 매칭 필드만
        image_urls_str = "|".join(image_urls)

        return {
            "date": d_str,
            "channel": "자사몰",
            "brand": brand,
            "product": str(product).strip(),
            "rating": rating,
            "text": text,
            "mall_id": mall_id,
            "board_no": board_no,
            "article_no": article_no,
            "image_urls": image_urls_str,
        }

    # ============================================================
    # 범용 게시판 글 수집 (문의하기 board_no=6, 1:1 상담 board_no=9 등)
    # rating 없는 게시판용 — 제목/본문/작성자 수집
    # ============================================================
    def fetch_board_articles_df(
        self, board_no: int, since: date, until: date, brand: str,
        limit: int = 100,
    ) -> pd.DataFrame:
        """게시판 글 목록 → DataFrame (문의/상담 등).

        반환 컬럼: date, brand, mall_id, board_no, article_no,
                   title, content, writer
        """
        import re as _re
        rows: list[dict] = []
        offset = 0
        while True:
            try:
                res = self._request(
                    "GET", f"/boards/{board_no}/articles",
                    params={
                        "limit": limit, "offset": offset,
                        "start_date": since.isoformat(),
                        "end_date": until.isoformat(),
                    },
                ) or {}
            except requests.HTTPError:
                break
            batch = res.get("articles", []) or []
            for a in batch:
                content = _re.sub(r"<[^>]+>", " ", str(a.get("content", "")))
                content = _re.sub(r"\s+", " ", content).strip()
                d = str(a.get("created_date") or a.get("written_date") or "")[:10]
                try:
                    a_no = int(a.get("article_no") or 0)
                except (TypeError, ValueError):
                    a_no = 0
                rows.append({
                    "date": d,
                    "brand": brand,
                    "mall_id": self.mall_id,
                    "board_no": board_no,
                    "article_no": a_no,
                    "title": str(a.get("title") or "").strip(),
                    "content": content,
                    "writer": str(
                        a.get("writer") or a.get("member_id") or ""
                    ).strip(),
                })
            if len(batch) < limit:
                break
            offset += limit
            if offset > 5000:
                break
        return pd.DataFrame(rows, columns=[
            "date", "brand", "mall_id", "board_no",
            "article_no", "title", "content", "writer",
        ])

    # ============================================================
    # 답글 (게시판 댓글) — 관리자가 리뷰에 답글 작성
    # 필요 scope: mall.write_community
    # ============================================================
    def get_board_comments(
        self, board_no: int, article_no: int,
    ) -> list[dict]:
        """게시글의 기존 댓글(답글) 목록."""
        try:
            res = self._request(
                "GET",
                f"/boards/{board_no}/articles/{article_no}/comments",
                params={"limit": 100},
            ) or {}
            return res.get("comments", []) or []
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code == 404:
                return []
            raise

    def post_board_comment(
        self, board_no: int, article_no: int, content: str,
        writer: str = "관리자",
    ) -> dict:
        """리뷰(게시글)에 관리자 답글 작성.

        반환: 생성된 comment dict (comment_no 포함).
        scope: mall.write_community 필요.

        다양한 body 형식 시도 — Cafe24 board API 가 게시판 설정에 따라
        password mandatory / writer 위치 등 다름.
        """
        content = (content or "").strip()
        if not content:
            raise ValueError("답글 본문이 비어있습니다.")

        # Cafe24 공식 spec (developers.cafe24.com 확인):
        # {"comment": {"content": "...", "password": "4-16 alphanumeric",
        #              "is_secret": "T" or "F"}}
        # writer 는 OAuth 토큰의 admin 계정으로 자동 설정됨.
        candidate_bodies = [
            {
                "shop_no": 1,
                "comment": {
                    "content": content,
                    "password": "growup24",  # 4-16 alphanumeric
                    "is_secret": "F",
                },
            },
            {
                "comment": {
                    "content": content,
                    "password": "growup24",
                    "is_secret": "F",
                },
            },
            # legacy / 다른 변형 — fallback
            {
                "shop_no": 1,
                "request": {
                    "writer": writer,
                    "content": content,
                    "password": "growup24",
                    "secret": "F",
                },
            },
        ]

        last_err: str = ""
        for i, body in enumerate(candidate_bodies, 1):
            try:
                res = self._request(
                    "POST",
                    f"/boards/{board_no}/articles/{article_no}/comments",
                    json_body=body,
                ) or {}
                return res.get("comment", {}) or res.get("request", {}) or res
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else 0
                body_text = e.response.text[:300] if e.response else ""
                last_err = f"try{i} HTTP {code}: {body_text}"
                # 422 는 body 형식 → 다음 candidate 시도
                # 401/403 은 인증/권한 → 즉시 중단
                if code in (401, 403):
                    raise RuntimeError(last_err)
                continue
        raise RuntimeError(
            f"답글 작성 실패 — 모든 body 형식 시도 실패. 마지막: {last_err}"
        )

    def delete_board_comment(
        self, board_no: int, article_no: int, comment_no: int,
    ) -> bool:
        """관리자 답글 삭제."""
        self._request(
            "DELETE",
            f"/boards/{board_no}/articles/{article_no}/comments/{comment_no}",
        )
        return True

    @staticmethod
    def _brand_of_store(store: str) -> str:
        s = str(store).replace(" ", "")
        if "똑똑" in s:
            return "똑똑연구소"
        if "롤라루" in s:
            return "롤라루"
        if "루티니" in s:
            return "루티니스트"
        return "기타"


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


def load_cafe24_client_by_mall_id(mall_id: str) -> Cafe24Client | None:
    """mall_id (toktoklab1 / routinist / rollaroo) → 클라이언트.

    페이지에서 reviews.csv 의 mall_id 컬럼 기반으로 답글 전송 시 사용.
    """
    mall_id = (mall_id or "").strip()
    if not mall_id:
        return None
    # 매장 → 브랜드 역매핑
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

    for brand in ["똑똑연구소", "롤라루", "루티니스트"]:
        suffix = _env_suffix_for_store(brand)
        mid = os.getenv(f"CAFE24_MALL_ID_{suffix}", "").strip()
        if mid == mall_id:
            return load_cafe24_client(brand)
    return None


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
