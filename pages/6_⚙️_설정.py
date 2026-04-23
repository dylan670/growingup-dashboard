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

# ==========================================================
# 🏷️ 제품명 정규화 규칙 편집 UI
# ==========================================================
st.subheader("🏷️ 제품명 정규화 규칙")
st.caption(
    "상품명이 길거나 채널별로 variant 가 많을 때 → 모델명으로 통일. "
    "여기서 수정하면 전체 대시보드에 즉시 반영됩니다 (매출/제품/CRM 등)."
)

from pathlib import Path as _Path
import pandas as _pd
from utils.products import PRODUCT_NAME_RULES

_RULES_OVERRIDE_FILE = _Path(__file__).parent.parent / "data" / "product_name_rules_override.csv"


def _load_override_rules() -> _pd.DataFrame:
    """사용자 추가 규칙 로드 (override CSV)."""
    if not _RULES_OVERRIDE_FILE.exists():
        return _pd.DataFrame(columns=["pattern", "canonical"])
    try:
        return _pd.read_csv(_RULES_OVERRIDE_FILE)
    except Exception:
        return _pd.DataFrame(columns=["pattern", "canonical"])


def _save_override_rules(df: _pd.DataFrame) -> None:
    _RULES_OVERRIDE_FILE.parent.mkdir(exist_ok=True)
    df.to_csv(_RULES_OVERRIDE_FILE, index=False, encoding="utf-8-sig")


with st.expander("📋 기본 규칙 (코드 내장 — utils/products.py)", expanded=False):
    st.caption(
        f"총 {len(PRODUCT_NAME_RULES)}개 규칙이 코드에 내장되어 있습니다. "
        "아래 표로 확인하고, 필요하면 'Override 규칙' 에서 덮어쓸 수 있습니다."
    )
    builtin_df = _pd.DataFrame(
        PRODUCT_NAME_RULES, columns=["원본 패턴 (substring)", "정규화 결과"]
    )
    st.dataframe(builtin_df, width="stretch", hide_index=True, height=300)


st.markdown("##### ✏️ 내 추가 규칙 (Override)")
st.caption(
    "이 표에 규칙을 추가하면 기본 규칙 **앞에** 우선 적용됩니다. "
    "pattern 이 상품명에 포함되면 canonical 로 치환. 저장 후 하드 리프레시."
)

override_df = _load_override_rules()
edited = st.data_editor(
    override_df if not override_df.empty else _pd.DataFrame(
        {"pattern": [""], "canonical": [""]}
    ),
    num_rows="dynamic",
    width="stretch",
    key="product_rules_editor",
    column_config={
        "pattern": st.column_config.TextColumn(
            "원본 패턴 (substring)",
            help="상품명에 이 문자열이 포함되면 매칭",
            width="large",
            required=False,
        ),
        "canonical": st.column_config.TextColumn(
            "정규화 결과",
            help="대시보드에서 이 이름으로 표시됨",
            width="medium",
            required=False,
        ),
    },
)

rc1, rc2 = st.columns([1, 3])
if rc1.button("💾 규칙 저장", type="primary", width="stretch"):
    # 빈 행 제거
    clean = edited.dropna(subset=["pattern", "canonical"]).copy()
    clean = clean[
        clean["pattern"].astype(str).str.strip() != ""
    ].reset_index(drop=True)
    _save_override_rules(clean)
    st.cache_data.clear()   # 관련 캐시 전체 무효화
    st.success(
        f"✅ 규칙 {len(clean)}개 저장 완료. "
        "대시보드 하드 리프레시(Ctrl+Shift+R) 후 반영 확인."
    )
    st.rerun()

rc2.caption(
    "💡 팁: 긴 제품명을 모델명으로 통일 (예: '롤라루 오프너 확장형 다크그린 20' → '오프너'). "
    "덜 구체적인 규칙은 아래쪽에, 더 구체적인 규칙은 위쪽에 두세요 (위에서부터 매칭)."
)

# ==========================================================
# 💰 월 매출 목표 편집 UI
# ==========================================================
st.divider()
st.subheader("💰 월 매출 목표 편집")
st.caption("브랜드 × 스토어별 월 매출 목표 — 시트에 덮어쓰는 수동 값.")

from utils.products import BRAND_STORE_MONTHLY_TARGETS, BRAND_MONTHLY_TARGETS

_TARGETS_OVERRIDE_FILE = _Path(__file__).parent.parent / "data" / "monthly_targets_override.csv"


def _load_targets_override() -> dict:
    if not _TARGETS_OVERRIDE_FILE.exists():
        return {}
    try:
        df = _pd.read_csv(_TARGETS_OVERRIDE_FILE)
        return {
            (r["brand"], r["store"]): int(r["target"])
            for _, r in df.iterrows()
        }
    except Exception:
        return {}


def _save_targets_override(rows: list[dict]) -> None:
    _TARGETS_OVERRIDE_FILE.parent.mkdir(exist_ok=True)
    _pd.DataFrame(rows).to_csv(
        _TARGETS_OVERRIDE_FILE, index=False, encoding="utf-8-sig"
    )


override_targets = _load_targets_override()

target_rows: list[dict] = []
for brand, stores in BRAND_STORE_MONTHLY_TARGETS.items():
    for store, default_target in stores.items():
        current = override_targets.get((brand, store), default_target)
        target_rows.append({
            "브랜드": brand,
            "스토어": store,
            "목표 (원)": current,
            "기본값": default_target,
        })

targets_df = _pd.DataFrame(target_rows)
edited_targets = st.data_editor(
    targets_df,
    width="stretch",
    hide_index=True,
    disabled=["브랜드", "스토어", "기본값"],
    column_config={
        "목표 (원)": st.column_config.NumberColumn(
            "월 목표 (원)", format="%d",
            min_value=0, step=100000,
        ),
        "기본값": st.column_config.NumberColumn(
            "기본값 (원)", format="%d",
            help="코드 내장 기본값 (참고용)",
        ),
    },
    key="targets_editor",
)

tc1, tc2 = st.columns([1, 3])
if tc1.button("💾 목표 저장", type="primary", width="stretch",
              key="save_targets"):
    rows_to_save = [
        {
            "brand": r["브랜드"],
            "store": r["스토어"],
            "target": int(r["목표 (원)"]),
        }
        for _, r in edited_targets.iterrows()
        if int(r["목표 (원)"]) != int(r["기본값"])
    ]
    _save_targets_override(rows_to_save)
    st.cache_data.clear()
    st.success(f"✅ 변경된 목표 {len(rows_to_save)}개 저장.")
    st.rerun()

tc2.caption(
    "💡 기본값 컬럼은 utils/products.py 내장값입니다. "
    "저장하면 이 값들이 우선 적용됩니다."
)

# ==========================================================
# 🚫 제품 차단 키워드 편집
# ==========================================================
st.divider()
st.subheader("🚫 차단 제품 키워드 (타 브랜드 제외)")
st.caption(
    "쿠팡 CSV 업로드 시 이 키워드가 포함된 상품은 자동 제외됩니다. "
    "현재 '오즈키즈' 등이 코드에 설정되어 있습니다."
)

from utils.products import PRODUCT_BLOCKLIST_KEYWORDS

_BLOCKLIST_OVERRIDE_FILE = _Path(__file__).parent.parent / "data" / "product_blocklist_override.csv"


def _load_blocklist_override() -> list[str]:
    if not _BLOCKLIST_OVERRIDE_FILE.exists():
        return []
    try:
        df = _pd.read_csv(_BLOCKLIST_OVERRIDE_FILE)
        return df["keyword"].dropna().astype(str).tolist()
    except Exception:
        return []


def _save_blocklist_override(keywords: list[str]) -> None:
    _BLOCKLIST_OVERRIDE_FILE.parent.mkdir(exist_ok=True)
    _pd.DataFrame({"keyword": keywords}).to_csv(
        _BLOCKLIST_OVERRIDE_FILE, index=False, encoding="utf-8-sig"
    )


current_blocklist = list(set(PRODUCT_BLOCKLIST_KEYWORDS + _load_blocklist_override()))

blocklist_text = st.text_area(
    "차단 키워드 (줄 단위 또는 쉼표 구분)",
    value="\n".join(current_blocklist),
    height=80,
    help="각 줄 또는 쉼표로 구분된 키워드 중 하나라도 상품명에 포함되면 차단",
)

bc1, bc2 = st.columns([1, 3])
if bc1.button("💾 차단 목록 저장", type="primary", width="stretch",
              key="save_blocklist"):
    # 줄/쉼표로 split
    import re as _re
    parts = _re.split(r"[\n,]+", blocklist_text)
    clean = sorted({p.strip() for p in parts if p.strip()})
    _save_blocklist_override(clean)
    st.cache_data.clear()
    st.success(f"✅ 차단 키워드 {len(clean)}개 저장.")
    st.rerun()

bc2.caption(
    "💡 '오즈키즈' 등 타 브랜드 제품이 쿠팡 발주리스트에 섞여있을 때 사용."
)

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
