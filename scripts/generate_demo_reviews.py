"""데모용 reviews.csv 재생성 — 3개 브랜드 균형 + 실제 product 매칭.

실제 리뷰 API 가 연동되기 전 까지 사용하는 가상 데이터.
orders.csv 의 인기 product 명을 그대로 사용해서 SKU 가 자연스럽게 매칭됨.

실행:
    python scripts/generate_demo_reviews.py
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).parent.parent
ORDERS_FILE = ROOT / "data" / "orders.csv"
REVIEWS_FILE = ROOT / "data" / "reviews.csv"

random.seed(42)  # 재현 가능


def _brand_of_store(s: str) -> str | None:
    s = str(s).replace(" ", "")
    if "똑똑" in s:
        return "똑똑연구소"
    if "롤라루" in s:
        return "롤라루"
    if "루티니" in s:
        return "루티니스트"
    return None


# ==========================================================
# 브랜드별 리뷰 텍스트 풀 (별점별 분리)
# ==========================================================
REVIEW_TEMPLATES: dict[str, dict[int, list[str]]] = {
    "똑똑연구소": {
        5: [
            "유기농이라 믿고 주는 간식. 떡뻥이 쌀맛이 진해서 애기가 좋아해요.",
            "우리 딸 최애 간식. 재구매 3번째! 식감이 부드러워서 이제 막 이유식 뗀 아기도 OK.",
            "구독 신청했어요. 매달 새 맛 출시되면 좋겠어요.",
            "돌아기 간식으로 딱. 손에 안 묻어서 외출할 때 편해요.",
            "24개월 아기가 너무 잘 먹어요. 김이 바삭하고 간이 세지 않아서 좋아요.",
            "유기농 인증 보고 안심하고 구매. 아이가 잘 먹어요. 강추!",
            "이유식 끝나고 첫 간식으로 좋아요. 부드럽고 안전한 맛.",
            "두 돌 아기가 잘 먹어요. 한 봉지로 양 조절하기 좋네요.",
            "재구매 의사 100%. 친구한테도 선물했어요.",
            "조부모님도 안심하고 주실 수 있는 간식. 만족합니다.",
            "야채 맛이 자연스러워서 좋아요. 색소 없고 깔끔.",
            "어린이집 간식으로 챙겨보내고 있어요. 선생님이 칭찬하셨어요.",
        ],
        4: [
            "맛은 좋아요. 다만 양이 좀 적어요.",
            "아기가 잘 먹어요. 다음에는 대용량 출시되면 좋겠네요.",
            "전반적으로 만족. 가격이 살짝 부담스럽긴 함.",
            "유기농 컨셉 마음에 들어요. 포장은 조금 아쉬워요.",
            "아이가 좋아해서 구매. 신선도가 들쭉날쭉한 느낌.",
            "맛 자체는 좋아요. 색이 좀 진해서 망설였어요.",
        ],
        3: [
            "떡뻥이 생각보다 작은 사이즈. 맛은 괜찮아요.",
            "아기가 한 두번 먹다 안 먹네요. 입맛 따라 다른 듯.",
            "보통이에요. 다른 브랜드와 큰 차이를 못 느꼈어요.",
            "괜찮긴 한데 가격이 좀 비싼 편.",
        ],
        2: [
            "기대보다 약해요. 아기가 안 먹어서 아쉬워요.",
            "사이즈가 작아서 양이 부족해요. 가격대비 별로.",
            "포장이 터져서 왔어요. 환불 받았네요.",
        ],
        1: [
            "맛이 이상해요. 아기가 뱉어버렸어요.",
            "유통기한 임박한 제품이 와서 실망. 반품 요청 했어요.",
        ],
    },
    "롤라루": {
        5: [
            "캐리어 정말 가볍고 튼튼해요. 일본 여행 갔다왔는데 흠집 하나 없어요.",
            "디자인 너무 예뻐요. 컬러 선택도 좋았고 사이즈도 딱 적당.",
            "확장형이라 짐 많이 들어가서 만족. 바퀴도 부드럽게 굴러가요.",
            "기내용 20인치 완전 강추. 가벼운데 수납공간 알차요.",
            "20인치 컴팩트한데 짐이 많이 들어가요. 비행기 기내 반입 OK.",
            "여행용 가방으로 최고. 휠이 조용하고 잘 굴러요.",
            "디자인 깔끔하고 마감 좋아요. 다시 살 의사 있어요.",
            "큐보이드 시리즈 진짜 예뻐요. 사람들이 어디서 샀냐고 물어봐요.",
            "오프너 캐리어 편해서 자주 사용. 정말 만족스러운 구매.",
            "네임택까지 챙겨주셔서 감동. 디테일 좋네요.",
            "다크그린 컬러 너무 마음에 들어요. 유니크하고 예뻐요.",
            "장기여행 다녀왔는데 28인치 완벽. 튼튼하고 가벼움.",
            "친구 결혼식 선물로 캐리어 줬는데 너무 좋아하더라구요.",
        ],
        4: [
            "전반적으로 만족하는데 손잡이 부분이 살짝 불편.",
            "디자인 좋아요. 무게가 조금 더 가벼웠으면 완벽했을 듯.",
            "튼튼해서 좋은데 가격대가 조금 부담스러워요.",
            "잘 쓰고 있어요. 다만 자물쇠가 작아서 헷갈려요.",
            "컬러는 예쁜데 사진보다 살짝 어두운 톤이에요.",
            "전반적으로 좋아요. 바퀴 부분 정도만 보강되면 완벽.",
        ],
        3: [
            "사이즈는 적당한데 무게가 좀 무겁네요.",
            "가격 대비 평범한 수준이에요.",
            "디자인은 좋은데 내부 분할이 아쉬워요.",
            "겉면이 생각보다 쉽게 스크래치 나는 편.",
        ],
        2: [
            "바퀴가 한 달만에 헐거워졌어요. 아쉬워요.",
            "지퍼가 약해서 자주 걸려요. 개선 필요.",
            "사이즈가 사진과 달라요. 좀 더 작아요.",
        ],
        1: [
            "받자마자 흠집있어서 실망. 반품 처리했어요.",
            "여행 첫날 손잡이 부러졌어요. 너무 약함.",
        ],
    },
    "루티니스트": {
        5: [
            "러닝조끼 정말 가벼워요. 마라톤 풀코스 뛰는데 너무 편했어요.",
            "메쉬 통기성 최고. 한여름에도 시원해서 쾌적해요.",
            "헤어밴드까지 챙겨주셔서 감동. 디테일이 좋네요.",
            "트레일러닝 갈 때 잘 사용중. 가볍고 보관도 편해요.",
            "사이즈 정확해요. 핏감이 슬림해서 좋아요.",
            "기능성 의류 처음 사봤는데 만족. 친구한테 추천했어요.",
            "땀 흡수 잘 되고 빨리 마름. 매일 운동 갈 때 입어요.",
            "10km 러닝 매주 다녀오는데 진짜 편함. 추천!",
            "기모 장갑 착용감 좋고 따뜻. 겨울 러닝 필수템.",
            "디자인이 깔끔하고 색상도 무난해서 어디서나 입기 좋아요.",
            "마라톤 대회에서 입었는데 답답하지 않고 가벼웠어요.",
            "전체적으로 만족도 높아요. 가격 대비 품질 굿.",
        ],
        4: [
            "통기성 좋은데 사이즈가 살짝 큰 느낌이에요.",
            "디자인은 마음에 드는데 컬러가 좀 어두워요.",
            "착화감 좋아요. 다만 처음엔 살짝 빳빳한 느낌.",
            "운동복으로 좋아요. 디자인 한 가지 더 있으면 좋겠어요.",
            "기능성 괜찮은데 마감이 조금 아쉬워요.",
        ],
        3: [
            "그냥 무난해요. 다른 브랜드 대비 큰 차이는 못 느꼈어요.",
            "가격이 좀 비싼 편이에요. 기능은 평범.",
            "사이즈가 예상과 달라서 교환했어요.",
        ],
        2: [
            "원단이 생각보다 얇아요. 아쉬워요.",
            "땀 흡수가 별로에요. 운동복으로는 아쉬움.",
            "재봉이 거칠어서 불편해요.",
        ],
        1: [
            "한 번 세탁하니 모양이 변형됐어요. 품질 실망.",
            "사이즈가 표기와 너무 달라요. 환불 요청.",
        ],
    },
}


CHANNEL_WEIGHTS = {"쿠팡": 0.55, "네이버": 0.30, "자사몰": 0.15}
RATING_WEIGHTS = {5: 0.55, 4: 0.22, 3: 0.13, 2: 0.07, 1: 0.03}
BRAND_REVIEW_COUNTS = {"똑똑연구소": 80, "롤라루": 80, "루티니스트": 60}


def main():
    orders = pd.read_csv(ORDERS_FILE)
    orders["brand"] = orders["store"].apply(_brand_of_store)

    products_by_brand: dict[str, list[str]] = {}
    for b in ["똑똑연구소", "롤라루", "루티니스트"]:
        sub = orders[orders["brand"] == b]
        top = sub.groupby("product")["quantity"].sum().sort_values(ascending=False)
        products_by_brand[b] = list(top.head(6).index)

    end_date = datetime(2026, 5, 25)
    start_date = end_date - timedelta(days=120)

    rows = []
    for brand, n_reviews in BRAND_REVIEW_COUNTS.items():
        products = products_by_brand.get(brand, [])
        if not products:
            print(f"[WARN] {brand}: 실제 product 없음 — 스킵")
            continue

        prod_weights = [max(1, 8 - i) for i in range(len(products))]

        for _ in range(n_reviews):
            rating = random.choices(
                list(RATING_WEIGHTS.keys()),
                weights=list(RATING_WEIGHTS.values()),
                k=1,
            )[0]
            candidates = REVIEW_TEMPLATES[brand].get(rating, [])
            if not candidates:
                for fb in [rating + 1, rating - 1, 5, 4]:
                    candidates = REVIEW_TEMPLATES[brand].get(fb, [])
                    if candidates:
                        break
            text = random.choice(candidates)
            product = random.choices(products, weights=prod_weights, k=1)[0]
            channel = random.choices(
                list(CHANNEL_WEIGHTS.keys()),
                weights=list(CHANNEL_WEIGHTS.values()),
                k=1,
            )[0]
            days_offset = random.randint(0, 120)
            review_date = start_date + timedelta(days=days_offset)
            rows.append({
                "date": review_date.strftime("%Y-%m-%d"),
                "channel": channel,
                "brand": brand,   # ← brand 직접 저장 (추론 의존 X)
                "product": product,
                "rating": rating,
                "text": text,
            })

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    # 컬럼 순서 명시
    df = df[["date", "channel", "brand", "product", "rating", "text"]]
    df.to_csv(REVIEWS_FILE, index=False, encoding="utf-8-sig")
    # 데모 marker 기록 (페이지 상단 배지 표시용)
    import json as _json
    meta = {
        "source": "demo",
        "generated_at": datetime.now().isoformat(),
        "count": len(df),
        "note": "generate_demo_reviews.py 가 만든 가상 데이터. "
                "실 sync 성공 시 자동으로 'live' 로 전환됨.",
    }
    (ROOT / "data" / "reviews_meta.json").write_text(
        _json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(f"[OK] {REVIEWS_FILE.name} 생성 — {len(df)}건 (source=demo)")

    print("\n=== 브랜드별 리뷰 ===")
    print(df["brand"].value_counts().to_string())
    print("\n=== 별점 분포 ===")
    print(df["rating"].value_counts().sort_index().to_string())
    print("\n=== 브랜드 × 별점 ===")
    print(df.groupby(["brand", "rating"]).size().unstack(fill_value=0).to_string())
    print("\n=== 브랜드별 product 종류 ===")
    for b in ["똑똑연구소", "롤라루", "루티니스트"]:
        sub = df[df["brand"] == b]
        print(f"\n[{b}] — 총 {len(sub)}건, {sub['product'].nunique()}개 SKU")
        for p, cnt in sub["product"].value_counts().items():
            print(f"  {cnt:>3}건  {p[:55]}")


if __name__ == "__main__":
    main()
