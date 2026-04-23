# 🌐 대시보드 외부 접근 가이드

다른 PC나 모바일에서 대시보드에 접근하는 3가지 방법.

---

## 🟢 방법 1: 같은 WiFi 네트워크 (가장 간단)

**준비**: 모든 기기가 **같은 WiFi** 에 연결돼 있어야 함.

### 단계

1. **대시보드 실행** (PC에서)
   ```cmd
   run_dashboard.bat
   ```
   또는:
   ```cmd
   .venv\Scripts\streamlit run app.py
   ```

2. **PC 의 IP 주소 확인**
   - PowerShell 또는 CMD 에서:
     ```cmd
     ipconfig | findstr IPv4
     ```
   - 예: `192.168.0.15`

3. **Windows 방화벽 허용** (최초 1회)
   - 팝업 `streamlit.exe 의 네트워크 액세스 허용하시겠습니까?` 나오면 **"허용"** 클릭
   - 또는 수동으로: `Windows 방화벽 → 인바운드 규칙 → 새 규칙 → 포트 → TCP 8501 허용`

4. **다른 기기에서 접속**
   - 모바일/다른 PC 브라우저 주소창에 입력:
     ```
     http://192.168.0.15:8501
     ```
   - ⚠️ `https` 가 아닌 `http` 로 접속

### 장단점
- ✅ 무료, 즉시 가능
- ✅ 외부 서비스 불필요, 보안 리스크 낮음
- ❌ 같은 WiFi 안에서만 접근
- ❌ PC 를 계속 켜둬야 함 (슬립 모드면 안 됨)

---

## 🟡 방법 2: Cloudflare Tunnel (어디서나 접근)

외부 인터넷에서도 접근 가능. 공유 가능한 고정 URL 제공.

### 장점
- 🌍 **어디서나 접근** (WiFi 관계없음)
- 🔒 **HTTPS 자동**
- 💰 **무료**
- 🔥 포트 포워딩/공유기 설정 불필요
- 🎁 공유용 짧은 URL 생성 (예: `happy-otter-123.trycloudflare.com`)

### 단계

1. **Cloudflared 설치** (PC 에 1회)
   - 다운로드: <https://github.com/cloudflare/cloudflared/releases>
   - `cloudflared-windows-amd64.exe` → `cloudflared.exe` 로 이름 변경
   - `C:\tools\cloudflared.exe` 또는 PATH 에 추가

2. **대시보드 실행** (평소처럼)
   ```cmd
   run_dashboard.bat
   ```

3. **터널 실행** (새 터미널 창)
   ```cmd
   cloudflared tunnel --url http://localhost:8501
   ```

4. **공유 URL 확인** — 터미널에 뜸:
   ```
   Your quick Tunnel has been created! Visit it at:
   https://XXX-YYY-ZZZ.trycloudflare.com
   ```

5. **이 URL 을 팀원들과 공유** — 어디서나 접근 가능

### 고정 URL 원하시면 (선택)
Cloudflare 계정 + 도메인 연결하면 `dashboard.yourdomain.com` 같은 고정 URL 사용 가능.
[Cloudflare Tunnel 공식 가이드](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)

### 자동 실행 (PC 부팅 시)
Windows 작업 스케줄러에 2개 추가:
1. `run_dashboard.bat` — 부팅 시 실행
2. `cloudflared tunnel --url http://localhost:8501` — 부팅 시 실행

---

## 🟠 방법 3: 클라우드 배포 (24/7 상시 운영)

PC 가 꺼져있어도 접근 가능. 월 비용 발생.

### 옵션 A: Streamlit Community Cloud (무료)
- GitHub 리포지토리 필요
- `.env` → `st.secrets` 전환 필요
- 매일 sync 는 외부 트리거 필요 (GitHub Actions cron)

### 옵션 B: Fly.io / Railway ($5~10/월)
- Docker 또는 buildpack 기반
- 백그라운드 cron 직접 실행 가능
- 공식 무료 티어 있음

### 옵션 C: AWS EC2 / DigitalOcean ($5~20/월)
- 완전 제어
- 직접 서버 관리 필요

→ **대규모 팀 운영 시에만 고려**. 지금 상황엔 **방법 2 (Cloudflare Tunnel)** 가 최적.

---

## 📱 모바일 접근 팁

1. **북마크 추가**: Safari/Chrome "홈 화면에 추가" → 앱처럼 사용 가능
2. **사이드바 자동 접힘**: 작은 화면에서 자동 접힘, 좌상단 햄버거 메뉴로 열기
3. **가로 모드**: KPI 카드 6개 한 줄에 보려면 가로로 회전 권장
4. **다크모드**: 야간 사용 시 사이드바 하단 🌙 토글

---

## 🔐 보안 주의

- 방법 1 (LAN): 같은 WiFi 안의 누구나 접근 가능 → 민감 데이터면 주의
- 방법 2 (Cloudflare): URL 을 아는 누구나 접근 → **공유 범위 제한 필요**
  - 비밀번호 보호: Cloudflare Zero Trust (무료, Email 인증)
- 방법 3 (클라우드): Streamlit 기본 인증 없음 → 별도 auth 레이어 필요

**민감 정보 포함 대시보드면 Cloudflare Zero Trust 설정 권장**:
- <https://developers.cloudflare.com/cloudflare-one/applications/configure-apps/self-hosted-public-app/>
- Email 로만 접근 제한 (팀원 email 등록)

---

## ❓ 추천

| 상황 | 추천 |
|------|------|
| 오늘 당장 모바일 확인 | **방법 1 (LAN)** |
| 외근 중 확인 | **방법 2 (Cloudflare Tunnel)** |
| 팀원들과 공유 | **방법 2** + Zero Trust |
| 24/7 상시 가동 | **방법 3** (Fly.io) |

대부분은 **방법 1 → 방법 2 순** 으로 충분합니다.
