# 🚀 Streamlit Community Cloud 배포 가이드

PC 꺼져있어도 **어디서나 24/7 접근** 가능한 클라우드 배포.

---

## 📋 준비된 것 (이미 완료)

- ✅ `requirements.txt` — Streamlit Cloud 의존성
- ✅ `.streamlit/secrets.toml.example` — secrets 템플릿
- ✅ `.github/workflows/daily_sync.yml` — 매일 10시 자동 sync (GitHub Actions)
- ✅ `.gitignore` — 민감 파일 보호 (`.env`, `orders.csv` 등)
- ✅ `utils/env_bootstrap.py` — `st.secrets` → 환경변수 자동 승격

---

## 🎯 배포 단계 (30분)

### 1️⃣ GitHub 사설 리포 생성 (5분)

1. [github.com](https://github.com) 로그인 → 우상단 `+` → **"New repository"**
2. 설정:
   - Repository name: `growingup-dashboard` (또는 원하는 이름)
   - ✅ **Private** 선택 (중요! 코드 · 데이터 비공개)
   - Add README, gitignore, license 전부 **체크 해제**
3. **Create repository**

### 2️⃣ 로컬 코드 push (5분)

터미널(Git Bash 또는 PowerShell):
```bash
cd C:\Users\PC\ddokddok-dashboard

# Git 초기화
git init
git add .
git status   # .env 등 민감 파일이 빠졌는지 꼭 확인!

# 첫 커밋
git commit -m "🎉 Initial commit — 그로잉업팀 대시보드"

# 원격 연결 + push (YOUR_USERNAME 을 본인 GitHub 아이디로)
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/growingup-dashboard.git
git push -u origin main
```

**⚠️ 커밋 전에 반드시 확인:**
```bash
git status
```
목록에 **`.env` · `data/orders.csv` · `data/cafe24_tokens.json`** 이 **없어야** 합니다.  
혹시 있으면 `.gitignore` 체크 후 `git reset` 으로 빼세요.

---

### 3️⃣ GitHub Secrets 등록 (10분)

리포의 **Settings → Secrets and variables → Actions → New repository secret**

아래 **22개 secret** 을 하나씩 등록. `.env` 파일의 각 줄을 복사/붙여넣기:

| Secret 이름 | 값 |
|------------|------|
| `NAVER_SEARCHAD_API_KEY` | (`.env` 참고) |
| `NAVER_SEARCHAD_SECRET_KEY` | |
| `NAVER_SEARCHAD_CUSTOMER_ID` | |
| `NAVER_COMMERCE_CLIENT_ID_DDOK` | |
| `NAVER_COMMERCE_CLIENT_SECRET_DDOK` | |
| `NAVER_COMMERCE_CLIENT_ID_ROLLA` | |
| `NAVER_COMMERCE_CLIENT_SECRET_ROLLA` | |
| `COUPANG_ACCESS_KEY` | |
| `COUPANG_SECRET_KEY` | |
| `COUPANG_VENDOR_ID` | |
| `CAFE24_CLIENT_ID_DDOK` | |
| `CAFE24_CLIENT_SECRET_DDOK` | |
| `CAFE24_MALL_ID_DDOK` | |
| `CAFE24_CLIENT_ID_ROLLA` | |
| `CAFE24_CLIENT_SECRET_ROLLA` | |
| `CAFE24_MALL_ID_ROLLA` | |
| `META_ACCESS_TOKEN_DDOK` | |
| `META_AD_ACCOUNT_ID_DDOK` | |
| `META_ACCESS_TOKEN_ROLLA` | |
| `META_AD_ACCOUNT_ID_ROLLA` | |
| `GOOGLE_SHEET_ID` | `1df0x5sTv5J2jw_GXiMcunMAtfVrjZ6rYQVYBRCdSO_0` |
| `GOOGLE_SHEET_GID` | `0` |

### 4️⃣ Streamlit Cloud 계정 & 앱 연결 (5분)

1. [share.streamlit.io](https://share.streamlit.io) 접속 → **"Sign in with GitHub"**
2. 우상단 **"New app"** → **"From existing repo"**
3. 설정:
   - Repository: `YOUR_USERNAME/growingup-dashboard`
   - Branch: `main`
   - Main file path: `app.py`
   - App URL: `growingup-dashboard` (원하는 subdomain)
4. **"Advanced settings"** → Python version `3.11`
5. **"Deploy!"** 클릭

### 5️⃣ Streamlit Secrets 등록 (5분)

앱이 배포되면 **Settings → Secrets** 탭에:

1. `.streamlit/secrets.toml.example` 내용 그대로 복사
2. 각 `""` 안에 `.env` 의 실제 값 붙여넣기
3. **Save**
4. 앱 자동 재시작 (30초)

### 6️⃣ GitHub Actions 첫 실행 (즉시)

1. GitHub 리포 → **Actions** 탭
2. "🔄 Daily Data Sync" 워크플로우 선택
3. **"Run workflow"** 버튼 → `main` 브랜치 선택 → 실행
4. 3~5분 후 완료되면 `data/` 폴더가 자동 업데이트됨
5. Streamlit Cloud 앱 새로고침 → 최신 데이터 반영

---

## ✅ 완료

- 🌍 **URL**: `https://growingup-dashboard.streamlit.app` (설정한 subdomain)
- 📱 **모바일**: 동일 URL 로 접근
- 🔄 **매일 오전 10시** (KST): GitHub Actions 자동 sync
- 🚀 **코드 수정**: `git push` 하면 자동 재배포 (1분)

---

## 🛡️ 추가 보안 (비공개 원할 때)

### 옵션 A: 앱 자체 Private (무료)
Streamlit Cloud **Settings → Sharing** → **"Only specific people"**  
→ GitHub 콜라보레이터 이메일 추가 → 그 사람들만 로그인 가능

### 옵션 B: 비밀번호 입력 레이어 (가장 간단)
`DASHBOARD_PASSWORD` 를 secret 에 추가하고, `app.py` 맨 위에:

```python
import streamlit as st
import os

def check_password():
    if "authed" not in st.session_state:
        st.session_state.authed = False
    if st.session_state.authed:
        return True
    pw_set = os.getenv("DASHBOARD_PASSWORD", "")
    if not pw_set:
        return True  # 비밀번호 미설정시 통과
    pw_input = st.text_input("🔒 비밀번호", type="password")
    if pw_input == pw_set:
        st.session_state.authed = True
        st.rerun()
    elif pw_input:
        st.error("비밀번호가 틀렸습니다.")
    st.stop()

check_password()
```

---

## 🛠️ 문제 해결

### ❌ "GitHub Actions 실패 — secret 없음"
→ 리포 Settings → Secrets 에서 22개 모두 등록 확인

### ❌ "Streamlit 배포 실패 — requirements.txt 오류"
→ 로컬에서 `pip install -r requirements.txt` 로 먼저 테스트

### ❌ "앱은 뜨는데 데이터가 없음"
→ GitHub Actions 수동 실행 (Run workflow) → 5분 대기 → 앱 재시작

### ❌ "Cafe24 토큰 만료"
→ Cafe24 는 OAuth 토큰이 30일 만료. 로컬에서 갱신 후:
```bash
git add data/cafe24_tokens.json
git commit -m "🔑 Cafe24 토큰 갱신"
git push
```

### ❌ "7일 idle 후 앱 sleep"
→ 앱 URL 한 번 열면 즉시 재기동. 또는 [UptimeRobot](https://uptimerobot.com) 으로 5분마다 ping (무료)

---

## 📊 최종 자동화 구조

```
GitHub 리포
  ├── 코드 변경 → git push → Streamlit 자동 재배포
  │
  └── GitHub Actions (매일 UTC 01:00 = KST 10:00)
        ├── 5개 API 동기화
        ├── precompute.py
        └── data/ 폴더 자동 커밋
              ↓
        Streamlit Cloud 가 변경 감지 → 앱 재시작 (캐시 갱신)
              ↓
        브라우저/모바일에서 즉시 최신 데이터 확인
```

---

## 🎯 배포 후 해야 할 일

1. ✅ 팀원들에게 URL 공유
2. ✅ 모바일 북마크 (홈 화면 추가)
3. ✅ UptimeRobot 등록 (7일 idle sleep 방지)
4. ✅ 기존 PC 의 `sync_all.bat` 작업 **비활성화** (중복 sync 방지)
   - `taskschd.msc` → "sync_all" 작업 사용 안 함

---

**궁금한 것이나 배포 중 에러나면 말씀해주세요.**
