"""데이터 업로드/관리."""
import streamlit as st

from utils.data import (
    load_ads, load_orders, load_reviews,
    reset_and_generate, merge_channel_ads, merge_store_orders,
    ADS_FILE, ORDERS_FILE, REVIEWS_FILE,
)
from converters import naver_searchad, naver_smartstore
from utils.ui import setup_page


setup_page(
    page_title="설정",
    page_icon="⚙️",
    header_title="⚙️ 데이터 설정",
    header_subtitle="CSV 업로드 · 샘플 데이터 재생성 · 임계값 조정",
)

# ---------- 현재 데이터 상태 ----------
st.subheader("현재 적재된 데이터")
ads = load_ads()
orders = load_orders()
reviews = load_reviews()

c1, c2, c3 = st.columns(3)
c1.metric("광고 데이터", f"{len(ads):,}행",
          f"{ads['date'].min().date()} ~ {ads['date'].max().date()}")
c2.metric("주문 데이터", f"{len(orders):,}행",
          f"{orders['date'].min().date()} ~ {orders['date'].max().date()}")
c3.metric("리뷰 데이터", f"{len(reviews):,}행")

# 채널별 행수
st.caption("**채널별 현재 데이터 (광고 / 주문):**")
by_ads = ads.groupby("channel").size()
by_orders = orders.groupby("channel").size()
cols = st.columns(len(set(list(by_ads.index) + list(by_orders.index))))
for i, ch in enumerate(sorted(set(list(by_ads.index) + list(by_orders.index)))):
    cols[i].caption(f"**{ch}**: 광고 {int(by_ads.get(ch, 0))}행 / 주문 {int(by_orders.get(ch, 0))}행")

st.divider()

# ---------- 네이버 자동 변환 업로드 ----------
st.subheader("네이버 원본 CSV 업로드 (자동 변환)")
st.markdown("""
네이버 검색광고 리포트와 스마트스토어 **2개 스토어(똑똑연구소·롤라루)**의 주문 CSV를
가공 없이 그대로 업로드하면 대시보드 포맷으로 자동 변환·병합됩니다.

- 한글 컬럼명 자동 매핑 / 인코딩 자동 감지 (UTF-8 / CP949)
- 구매자 정보는 **해시 익명화** (로컬에만 보관, 외부 전송 없음)
- 스토어별 독립 업로드 — **다른 스토어 데이터는 그대로 유지**

상세 가이드: `docs/네이버_데이터_연결_가이드.md`
""")

# 세 개 열로 구성 (광고 + 2개 스토어 주문)
up_naver_ads = st.file_uploader(
    "① 네이버 검색광고 CSV (광고비/노출/클릭)",
    type=["csv"],
    key="naver_ads",
    help="검색광고 시스템 → 보고서 → 다차원 보고서 (일자별)"
)

col_ddok, col_rolla = st.columns(2)
with col_ddok:
    up_store_ddok = st.file_uploader(
        "② 똑똑연구소 스마트스토어 주문 CSV",
        type=["csv"],
        key="store_ddok",
        help="똑똑연구소 판매자센터 → 주문/배송 → 주문 내역 → 엑셀 → CSV로 저장"
    )
with col_rolla:
    up_store_rolla = st.file_uploader(
        "③ 롤라루 스마트스토어 주문 CSV",
        type=["csv"],
        key="store_rolla",
        help="롤라루 판매자센터 → 주문/배송 → 주문 내역 → 엑셀 → CSV로 저장"
    )

# 미리보기
preview_ads_df = None
preview_orders_ddok = None
preview_orders_rolla = None

if up_naver_ads:
    try:
        preview_ads_df = naver_searchad.convert(up_naver_ads)
        with st.expander(f"검색광고 변환 미리보기 ({len(preview_ads_df)}일치)", expanded=False):
            st.dataframe(preview_ads_df.head(20), width="stretch", hide_index=True)
            total = preview_ads_df[["spend", "clicks", "conversions", "revenue"]].sum()
            roas = total["revenue"] / total["spend"] * 100 if total["spend"] > 0 else 0
            cc1, cc2, cc3, cc4 = st.columns(4)
            cc1.metric("기간 광고비", f"{int(total['spend']):,}원")
            cc2.metric("기간 매출", f"{int(total['revenue']):,}원")
            cc3.metric("ROAS", f"{roas:.0f}%")
            cc4.metric("총 전환", f"{int(total['conversions']):,}건")
    except Exception as e:
        st.error(f"**검색광고 CSV 변환 실패**\n\n{e}")
        preview_ads_df = None


def _preview_orders(file_obj, store_name: str):
    try:
        df = naver_smartstore.convert(file_obj, store=store_name)
        with st.expander(
            f"{store_name} 스마트스토어 변환 미리보기 ({len(df)}건)",
            expanded=False,
        ):
            st.dataframe(df.head(15), width="stretch", hide_index=True)
            cc1, cc2, cc3 = st.columns(3)
            cc1.metric("주문 수", f"{len(df):,}건")
            cc2.metric("매출 합계", f"{int(df['revenue'].sum()):,}원")
            cc3.metric("고객 수 (고유)", f"{df['customer_id'].nunique():,}명")
            src = df.attrs.get("customer_id_source", "")
            if src:
                st.caption(f"고객 ID 생성 방식: {src}")
        return df
    except Exception as e:
        st.error(f"**{store_name} 스마트스토어 CSV 변환 실패**\n\n{e}")
        return None


if up_store_ddok:
    preview_orders_ddok = _preview_orders(up_store_ddok, "똑똑연구소")
if up_store_rolla:
    preview_orders_rolla = _preview_orders(up_store_rolla, "롤라루")

# 병합 버튼
disabled = (
    preview_ads_df is None
    and preview_orders_ddok is None
    and preview_orders_rolla is None
)
if st.button("네이버 데이터 병합 (업로드한 항목만 교체)",
             type="primary", disabled=disabled):
    results = []
    if preview_ads_df is not None:
        removed, added = merge_channel_ads(preview_ads_df, "네이버")
        results.append(f"광고 (네이버): 기존 {removed}행 제거 → 신규 {added}행 추가")
    if preview_orders_ddok is not None:
        removed, added = merge_store_orders(preview_orders_ddok, "똑똑연구소")
        results.append(f"주문 (똑똑연구소): 기존 {removed}행 제거 → 신규 {added}행 추가")
    if preview_orders_rolla is not None:
        removed, added = merge_store_orders(preview_orders_rolla, "롤라루")
        results.append(f"주문 (롤라루): 기존 {removed}행 제거 → 신규 {added}행 추가")

    for r in results:
        st.success(r)
    st.info("홈/채널별 심화 페이지로 이동하면 실제 네이버 데이터로 대시보드가 그려집니다.")

st.divider()

# ---------- 표준 스키마 CSV 직접 업로드 ----------
st.subheader("표준 스키마 CSV 업로드 (고급)")
st.markdown("""
변환기를 거치지 않고 이미 대시보드 포맷에 맞춘 CSV를 직접 업로드하고 싶은 경우.
**⚠️ 전체 데이터를 덮어씁니다.** 네이버만 교체하고 싶다면 위의 자동 변환 섹션을 쓰세요.

**광고 CSV 컬럼:** `date, channel, spend, impressions, clicks, conversions, revenue`
**주문 CSV 컬럼:** `date, order_id, customer_id, channel, product, quantity, revenue`
**리뷰 CSV 컬럼 (선택):** `date, channel, product, rating, text`

`channel` 값은 반드시 **네이버 / 쿠팡 / 자사몰** 중 하나.
`date` 형식: `YYYY-MM-DD`.
""")

up_ads = st.file_uploader("광고 CSV (전체 덮어쓰기)", type=["csv"], key="ads_up")
up_orders = st.file_uploader("주문 CSV (전체 덮어쓰기)", type=["csv"], key="orders_up")
up_reviews = st.file_uploader("리뷰 CSV (전체 덮어쓰기, 선택)", type=["csv"], key="reviews_up")

if st.button("전체 덮어쓰기 적용"):
    messages = []
    if up_ads:
        with open(ADS_FILE, "wb") as f:
            f.write(up_ads.getvalue())
        messages.append("광고 CSV 저장 완료.")
    if up_orders:
        with open(ORDERS_FILE, "wb") as f:
            f.write(up_orders.getvalue())
        messages.append("주문 CSV 저장 완료.")
    if up_reviews:
        with open(REVIEWS_FILE, "wb") as f:
            f.write(up_reviews.getvalue())
        messages.append("리뷰 CSV 저장 완료.")

    if messages:
        for m in messages:
            st.success(m)
    else:
        st.warning("업로드할 파일이 선택되지 않았습니다.")

st.divider()

# ---------- 샘플 데이터 재생성 ----------
st.subheader("샘플 데이터 재생성")
st.caption("기존 데이터를 모두 삭제하고 60일치 샘플을 새로 생성합니다. 데모 상태로 되돌릴 때 사용.")
if st.button("샘플 재생성"):
    reset_and_generate()
    st.success("샘플 데이터 재생성 완료. 다른 페이지로 이동하면 반영됩니다.")

st.divider()

# ---------- 로드맵 ----------
st.subheader("API 자동 연동 로드맵")
st.markdown("""
현재는 CSV 수동 업로드지만, 승인 후 API로 자동화 가능:

| 소스 | API 문서 | 승인 경로 | 예상 소요 |
|------|---------|-----------|-----------|
| 네이버 검색광고 | https://naver.github.io/searchad-apidoc/ | 검색광고 시스템 → 도구 → API 사용 관리 | 1–2영업일 |
| 네이버 커머스 (스마트스토어) | https://apicenter.commerce.naver.com/ | 커머스 API 센터 → 파트너 신청 | 3–7영업일 |
| 쿠팡 Wing | (개별 문의) | 쿠팡 파트너 지원 | 개별 |
| 메타 Marketing | https://developers.facebook.com/docs/marketing-apis/ | 앱 등록 → 광고 권한 | 즉시 |

승인 후 `.env`에 키를 넣고 대시보드 재시작하면 매일 자동 갱신됩니다.
""")
