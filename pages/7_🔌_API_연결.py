"""API 연결 설정 및 수동 동기화."""
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

from api.naver_searchad import (
    NaverSearchAdClient,
    load_client_from_env,
    save_credentials_to_env,
)
from api.cafe24 import (
    Cafe24Client,
    load_cafe24_client,
    save_cafe24_credentials,
    DEFAULT_SCOPES,
)
from utils.data import merge_channel_ads
from utils.ui import setup_page

setup_page(
    page_title="API 연결",
    page_icon="🔌",
    header_title="🔌 API 연결",
    header_subtitle="각 플랫폼 인증 및 수동 동기화 관리",
)


st.caption("플랫폼 API 자격증명을 등록하고, 실시간에 가깝게 데이터를 자동 갱신합니다.")

# ---------- 현재 상태 ----------
client_env = load_client_from_env()

col1, col2 = st.columns([1, 3])
with col1:
    if client_env:
        st.success("네이버 검색광고 API 키 등록됨")
    else:
        st.warning("네이버 검색광고 API 키 미등록")
with col2:
    st.caption("키는 `.env` 파일 (프로젝트 루트)에 저장됩니다. `.gitignore`에 포함되어 버전 관리에서 제외됨.")

st.divider()

# ---------- 네이버 검색광고 API ----------
st.header("네이버 검색광고 API")

st.markdown("""
**발급 경로:**
1. [searchad.naver.com](https://searchad.naver.com) 로그인
2. 우측 상단 **「도구」** → **「API 사용 관리」**
3. **「API 사용 신청」** 버튼 → 약관 동의 → 보통 당일~1영업일 승인
4. 승인 후 **API Key**, **Secret Key** 확인
5. **Customer ID** (광고주 ID)는 상단 프로필 영역에서 확인 (예: `1234567`)

자세한 가이드: `docs/네이버_API_설정.md`
""")

with st.expander("키 입력 / 변경", expanded=(client_env is None)):
    with st.form("naver_creds"):
        c1, c2 = st.columns(2)
        with c1:
            in_api_key = st.text_input(
                "API Key",
                value="" if client_env is None else "●" * 12,
                type="password",
                help="API 사용 관리 페이지의 '액세스라이선스' 아래 첫 문자열",
            )
            in_customer_id = st.text_input(
                "Customer ID",
                value="" if client_env is None else client_env.customer_id,
                help="네이버 검색광고 시스템 상단의 광고주 ID (숫자 7~9자리)",
            )
        with c2:
            in_secret_key = st.text_input(
                "Secret Key",
                value="",
                type="password",
                help="API 사용 관리 페이지에서 '비밀키' (한 번 발급 시 전체 문자열 복사해두셔야 함)",
            )
            save_to_env = st.checkbox(
                "이 기기에 저장 (.env 파일)",
                value=True,
                help="체크 해제 시 현재 세션에만 유지되며 브라우저 새로고침 시 재입력 필요",
            )

        submit = st.form_submit_button("저장 및 연결 테스트", type="primary")

    if submit:
        # 기존 키에서 마스킹된 값을 기존 값으로 복원
        final_api_key = in_api_key if in_api_key != "●" * 12 else (client_env.api_key if client_env else "")
        final_secret = in_secret_key if in_secret_key else (client_env.secret_key if client_env else "")
        final_cust = in_customer_id

        if not all([final_api_key, final_secret, final_cust]):
            st.error("API Key / Secret Key / Customer ID 모두 입력해야 합니다.")
        else:
            try:
                client = NaverSearchAdClient(final_api_key, final_secret, final_cust)
                with st.spinner("연결 테스트 중..."):
                    ok, msg = client.test_connection()

                if ok:
                    st.success(f"{msg}")
                    if save_to_env:
                        save_credentials_to_env(final_api_key, final_secret, final_cust)
                        st.info("키가 `.env` 파일에 저장되었습니다. 재시작 후에도 자동 로드됩니다.")
                    st.session_state["naver_client"] = client
                else:
                    st.error(f"{msg}")
            except Exception as e:
                st.error(f"클라이언트 생성 실패: {e}")

st.divider()

# ---------- 수동 동기화 ----------
st.subheader("데이터 동기화")

active_client = st.session_state.get("naver_client") or client_env

if active_client is None:
    st.info("먼저 위에서 API 키를 등록하세요.")
else:
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        days_back = st.number_input("며칠 전부터", min_value=1, max_value=90, value=7, step=1)
    with c2:
        until = st.date_input("기준일", value=date.today() - timedelta(days=1),
                              help="네이버는 당일 데이터가 미확정이므로 전일까지 동기화 권장")
    with c3:
        st.caption(
            f"**조회 기간:** {until - timedelta(days=days_back-1)} ~ {until}  \n"
            f"**동기화 방식:** 해당 기간 네이버 데이터를 당겨와서 기존 네이버 행만 교체 (쿠팡·자사몰 유지)"
        )

    if st.button("지금 동기화", type="primary"):
        since = until - timedelta(days=days_back - 1)
        try:
            with st.spinner(f"{days_back}일치 데이터 조회 중... (일자당 약 2초)"):
                df = active_client.get_daily_stats_df(since, until)

            if df.empty or df["clicks"].sum() == 0:
                st.warning("조회된 데이터가 없습니다. 기간이나 캠페인 운영 상태를 확인하세요.")
            else:
                # 미리보기
                with st.expander(f"변환 미리보기 ({len(df)}일치)", expanded=True):
                    st.dataframe(df, width="stretch", hide_index=True)

                    total = df[["spend", "clicks", "conversions", "revenue"]].sum()
                    roas = total["revenue"] / total["spend"] * 100 if total["spend"] > 0 else 0
                    cc1, cc2, cc3, cc4 = st.columns(4)
                    cc1.metric("기간 광고비", f"{int(total['spend']):,}원")
                    cc2.metric("기간 매출", f"{int(total['revenue']):,}원")
                    cc3.metric("ROAS", f"{roas:.0f}%")
                    cc4.metric("총 전환", f"{int(total['conversions']):,}건")

                # 병합
                removed, added = merge_channel_ads(df, "네이버")
                st.success(f"동기화 완료. 기존 네이버 {removed}행 제거 → 신규 {added}행 추가됨.")
                st.info("홈 / 채널별 심화 페이지로 이동하면 실제 데이터로 대시보드가 그려집니다.")

        except Exception as e:
            st.error(f"동기화 실패: {e}")

st.divider()

# ---------- 자동 갱신 가이드 ----------
st.subheader("매일 자동 갱신 설정 (Windows 작업 스케줄러)")

st.markdown("""
`.env`에 키를 저장한 상태에서, 아래 스크립트를 Windows 작업 스케줄러로 매일 아침 실행하면
출근 시엔 이미 전일 데이터가 반영된 상태로 대시보드를 열 수 있습니다.

**1. 스크립트 위치:** `scripts/sync_naver_ads.py` (프로젝트에 포함됨)

**2. 작업 스케줄러 등록:**

1. `Win + R` → `taskschd.msc` 엔터
2. 우측 패널 **「기본 작업 만들기」** 클릭
3. 이름: `그로잉업팀_네이버동기화` / 매일 / 오전 6:30 / 프로그램 시작
4. 프로그램/스크립트:
   ```
   C:\\Users\\PC\\ddokddok-dashboard\\.venv\\Scripts\\python.exe
   ```
5. 인수 추가:
   ```
   C:\\Users\\PC\\ddokddok-dashboard\\scripts\\sync_naver_ads.py --days 3
   ```
6. 시작 위치:
   ```
   C:\\Users\\PC\\ddokddok-dashboard
   ```
7. 완료 → 작업 우클릭 → **속성 → 일반 → "사용자가 로그온할 때 실행"** + **"가장 높은 수준의 권한으로 실행"** 체크

설정 후 **작업 우클릭 → 실행**으로 즉시 테스트 가능. 정상 작동하면 매일 아침 자동 반영됩니다.

**3. 로그 확인:**
`data/sync_log.txt` 에 매 실행 결과가 누적됩니다. (성공/실패, 반영된 행수)
""")

st.divider()

# ==========================================================
# Cafe24 자사몰 API
# ==========================================================
st.header("Cafe24 자사몰 API (OAuth 2.0)")
st.caption(
    "자사몰 주문·상품 데이터를 Cafe24 Admin API로 직접 수집합니다. "
    "초기 1회 OAuth 인증 후 refresh_token으로 자동 갱신."
)

REDIRECT_URI = "https://oauth.pstmn.io/v1/callback"

# 2개 스토어 (똑똑연구소 / 롤라루) 탭
cafe24_tabs = st.tabs(["똑똑연구소 자사몰", "롤라루 자사몰"])

for tab, brand in zip(cafe24_tabs, ["똑똑연구소", "롤라루"]):
    with tab:
        existing = load_cafe24_client(brand)

        # 상태 배지
        if existing:
            has_token = existing._access_token is not None
            if has_token:
                st.success(
                    f"**{brand}**: 자격증명 + 토큰 보유 (mall: {existing.mall_id})"
                )
            else:
                st.warning(
                    f"**{brand}**: 자격증명 등록됨 (mall: {existing.mall_id}) "
                    "— OAuth 인증 필요"
                )
        else:
            st.info(f"**{brand}**: 미설정")

        # === 1단계 · Client ID/Secret 입력 ===
        with st.expander(
            "1단계 · mall_id + Client ID/Secret 입력",
            expanded=(existing is None),
        ):
            with st.form(f"cafe24_creds_{brand}"):
                c1, c2 = st.columns(2)
                with c1:
                    f_mall = st.text_input(
                        "Mall ID",
                        value=(existing.mall_id if existing else ""),
                        help="관리자 URL의 서브도메인. 예: `ddokmall.cafe24.com` → `ddokmall`"
                    )
                    f_cid = st.text_input("Client ID", type="password",
                                          help="developers.cafe24.com → 앱 상세에서 복사")
                with c2:
                    f_cs = st.text_input("Client Secret", type="password")

                submit = st.form_submit_button("자격증명 저장", type="primary")

            if submit:
                if not all([f_mall, f_cid, f_cs]):
                    st.error("3개 값 모두 입력 필요")
                else:
                    try:
                        save_cafe24_credentials(brand, f_mall, f_cid, f_cs)
                        st.success(f"저장 완료. 페이지 새로고침 후 2단계로 진행.")
                    except Exception as e:
                        st.error(f"저장 실패: {e}")

        # === 2단계 · OAuth 인증 ===
        with st.expander(
            "2단계 · OAuth 인증 (브라우저에서 승인 후 코드 복사)",
            expanded=(existing is not None and not existing._access_token),
        ):
            if existing is None:
                st.info("1단계 먼저 완료하세요.")
            else:
                scopes_chosen = st.multiselect(
                    "요청 스코프",
                    DEFAULT_SCOPES,
                    default=DEFAULT_SCOPES,
                    key=f"scopes_{brand}",
                )

                auth_url = existing.authorize_url(REDIRECT_URI, scopes=scopes_chosen)
                st.markdown(
                    f"**1. 아래 링크 클릭 → Cafe24 로그인 → 앱 권한 승인:**  \n"
                    f"[▶ Cafe24 OAuth 인증 시작]({auth_url})  \n\n"
                    f"**2. 승인 후 브라우저가 `{REDIRECT_URI}?code=XXXXXX&state=...` 로 리다이렉트**됩니다. "
                    "주소창의 `code=` 뒤 값 (몇십자 문자열)을 복사하세요.  \n"
                    "(리다이렉트된 페이지가 404 나와도 괜찮습니다 — URL만 필요)"
                )

                code_input = st.text_input(
                    "code 값 붙여넣기",
                    key=f"code_input_{brand}",
                    placeholder="예: hJmC6...길고 임의의 문자열",
                )

                if st.button("토큰 교환", type="primary", key=f"exchange_{brand}"):
                    if not code_input.strip():
                        st.warning("code 값을 입력하세요.")
                    else:
                        with st.spinner("토큰 교환 중..."):
                            try:
                                data = existing.exchange_code_for_token(
                                    code_input.strip(), REDIRECT_URI,
                                )
                                expires_in = data.get("expires_in", 7200)
                                st.success(
                                    f"인증 완료. access_token 발급 (유효 {expires_in}초). "
                                    "3단계에서 연결 테스트 + 첫 동기화 가능."
                                )
                            except requests.HTTPError as e:
                                body = e.response.text[:400] if e.response else ""
                                st.error(f"HTTP {e.response.status_code if e.response else '?'}: {body}")
                            except Exception as e:
                                st.error(f"{type(e).__name__}: {e}")

        # === 3단계 · 연결 테스트 & 수동 동기화 ===
        with st.expander(
            "3단계 · 연결 테스트 + 동기화",
            expanded=(existing is not None and existing._access_token is not None),
        ):
            if existing is None or not existing._access_token:
                st.info("2단계 OAuth 완료 후 사용 가능.")
            else:
                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("연결 테스트", key=f"test_{brand}"):
                        with st.spinner("테스트 중..."):
                            ok, msg = existing.test_connection()
                            if ok:
                                st.success(msg)
                            else:
                                st.error(msg)

                with col_b:
                    days = st.number_input(
                        "동기화 기간 (일)", min_value=1, max_value=90, value=30,
                        key=f"days_{brand}",
                    )
                    if st.button("지금 동기화", type="primary", key=f"sync_{brand}"):
                        from datetime import date, timedelta
                        from utils.data import merge_store_orders

                        store_name = f"자사몰_{brand}"
                        until = date.today() - timedelta(days=1)
                        since = until - timedelta(days=days - 1)

                        with st.spinner(f"{days}일치 조회 중..."):
                            try:
                                df = existing.fetch_orders_df(since, until, store_name)
                                if df.empty:
                                    st.warning("조회 결과 없음.")
                                else:
                                    removed, added = merge_store_orders(df, store_name)
                                    total_rev = int(df["revenue"].sum())
                                    st.success(
                                        f"{len(df)}건 / {total_rev:,}원 수집. "
                                        f"병합: 기존 {removed}행 → 신규 {added}행"
                                    )
                            except Exception as e:
                                st.error(f"{type(e).__name__}: {e}")

# Cafe24 앱 등록 가이드 (축약)
with st.expander("Cafe24 Private App 등록 방법"):
    st.markdown(f"""
1. [developers.cafe24.com](https://developers.cafe24.com) 로그인 (Cafe24 계정)
2. **「My Apps」 → 「앱 만들기」 → 「프라이빗 앱(Private App)」**
3. 앱 정보 입력:
   - 앱 이름: `그로잉업팀 대시보드`
   - **Redirect URI: `{REDIRECT_URI}`** ← 정확히 일치해야 함
   - **개발 권한(Scope)**:
     - ☑ `mall.read_order` — 주문 조회
     - ☑ `mall.read_product` — 상품 조회
     - ☑ `mall.read_customer` — 고객 조회
4. 앱 생성 → **Client ID, Client Secret** 복사
5. **쇼핑몰 2개면 각각 별도 앱 생성** (각 mall에 앱 설치 필요)
    """)

# ==========================================================
# 네이버 커머스 API 상태 (이미 연결됨)
# ==========================================================
st.divider()
st.header("네이버 커머스 API · 쿠팡 Wing API")
st.success(
    "네이버 커머스 API (스마트스토어 2개) + 쿠팡 Wing API (업체배송 + 로켓그로스) "
    "모두 연결 완료 · 매일 10시 자동 동기화 중"
)
st.caption(
    "키 재발급이나 추가 스토어 등록이 필요하면 `.env` 파일 직접 수정 "
    "(`NAVER_COMMERCE_CLIENT_ID_*`, `COUPANG_ACCESS_KEY` 등)"
)
