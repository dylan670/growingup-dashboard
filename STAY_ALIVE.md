# 🔴 대시보드 상시 가동 가이드

PC 가 켜져있는 한 대시보드가 **24/7 항상 열려있도록** 설정.

---

## 🚀 원클릭 셋업 (권장)

### PowerShell 관리자 권한으로 실행

1. **시작 메뉴 → "PowerShell" 우클릭 → "관리자 권한으로 실행"**

2. 다음 명령어 복사/붙여넣기:
   ```powershell
   cd C:\Users\PC\ddokddok-dashboard
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
   .\scripts\setup_autostart.ps1
   ```

3. 자동으로 설정되는 것:
   - ✅ **PC 슬립/최대절전 방지** (AC 전원)
   - ✅ **대시보드 부팅시 자동 실행** (크래시 시 자동 재시작)
   - ✅ (cloudflared 있으면) **Cloudflare 터널 자동 실행**

---

## 📋 수동 셋업

원클릭이 안 되거나 세부 조정 원하면:

### 1. 전원 설정 — PC 슬립/최대절전 방지

**방법 A (터미널, 빠름)**:
```cmd
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
```

**방법 B (GUI)**:
1. 제어판 → 전원 옵션 → 고성능 (선택)
2. 계획 설정 변경 → "컴퓨터를 절전 모드로 전환" → **해당 없음**

### 2. 대시보드 자동 실행 등록

1. **작업 스케줄러 실행**: `Win + R` → `taskschd.msc`
2. 우측 "작업 만들기" 클릭
3. 설정값:

   | 탭 | 항목 | 값 |
   |---|---|---|
   | **일반** | 이름 | `그로잉업 대시보드 상시` |
   | **일반** | 옵션 | ☑ 최고 권한으로 실행 |
   | **트리거** | 새로 만들기 | "시작 시" |
   | **동작** | 새로 만들기 | 프로그램 시작 |
   | **동작** | 프로그램 | `C:\Users\PC\ddokddok-dashboard\scripts\run_forever.bat` |
   | **조건** | ☐ | "AC 전원인 경우에만 작업 시작" 체크 해제 |
   | **설정** | ☑ | "실패한 경우 다시 시작" — 1분, 3회 |

4. 저장 → 작업 목록에서 우클릭 → **"실행"** 으로 테스트

### 3. (옵션) 외부 접근 — Cloudflare Tunnel

외근 중이나 팀원 원격 접근 필요시:

1. **cloudflared 다운로드**
   - [GitHub releases](https://github.com/cloudflare/cloudflared/releases) → `cloudflared-windows-amd64.exe`
   - `C:\tools\cloudflared.exe` 로 저장

2. **작업 스케줄러에 추가** (위 2번과 동일, 다만):
   - 이름: `그로잉업 터널 상시`
   - 트리거: 시작 시, **지연 30초**
   - 프로그램: `C:\Users\PC\ddokddok-dashboard\scripts\run_tunnel_forever.bat`

3. **터널 URL 확인**
   - cloudflared 실행 창에서 URL 출력됨
   - 예: `https://happy-otter-xyz.trycloudflare.com`

---

## ✅ 동작 확인

### 방법 A: 지금 바로 테스트
```cmd
scripts\run_forever.bat
```
검은 창 열리고 Streamlit 실행 → 안 꺼지면 상시 작동 ✓

브라우저: `http://localhost:8501` 또는 `http://172.16.1.13:8501`

### 방법 B: 재부팅 테스트
1. PC 재부팅
2. 로그인 완료 후 1분 대기
3. 브라우저 접속 확인

### 방법 C: 작업 상태 확인
```cmd
taskschd.msc
```
"그로잉업 대시보드 상시" → 마지막 실행 결과 `0x0` = 성공

---

## 🛠️ 문제 해결

### ❌ "PowerShell 실행 안 됨 — 관리자 필요"
→ 시작 메뉴 → PowerShell **우클릭** → **"관리자 권한으로 실행"**

### ❌ "작업 스케줄러 등록됐는데 재부팅 후 접속 안됨"
- 사용자가 **로그인** 했는지 확인 (잠금 화면이면 동작 안 할 수도)
- **해결**: 작업 속성 → 일반 → **"사용자 로그온 여부에 관계없이 실행"** 선택 + 비번 입력

### ❌ "Streamlit 이 계속 크래시"
- `run_forever.bat` 창에 에러 표시됨
- `data/sync_log.txt` 에 상세 로그
- 대부분 `.venv` 경로 / Python / 의존성 문제

### ❌ "재부팅 후 sync_all 은 안 돌아감"
- `sync_all.bat` 은 **매일 오전 10시** 실행 (기존 작업)
- 부팅과 별개 — 정상

### ❌ "터널 URL 이 매번 바뀜"
- Cloudflare 무료 티어 기본 (임시 URL)
- **고정 URL 필요**: Cloudflare 계정 + 도메인 연결 ([공식 가이드](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/get-started/))

---

## 🔄 등록 해제

나중에 상시 실행 끄고 싶으면:

```powershell
Unregister-ScheduledTask -TaskName "그로잉업 대시보드 상시" -Confirm:$false
Unregister-ScheduledTask -TaskName "그로잉업 터널 상시" -Confirm:$false
```

---

## 📊 최종 자동화 구조

```
PC 부팅
  ↓
[시작 시] 그로잉업 대시보드 상시 → scripts/run_forever.bat
  └── Streamlit 24/7 실행 (크래시 시 자동 재시작 10초 후)
      - 로컬: http://localhost:8501
      - LAN : http://172.16.1.13:8501

[시작 + 30초] 그로잉업 터널 상시 → scripts/run_tunnel_forever.bat (옵션)
  └── Cloudflare Tunnel
      - 외부 URL: https://XXX.trycloudflare.com

[매일 10:00] sync_all.bat (기존)
  ├── 5개 API 데이터 수집
  └── precompute.py (Parquet/JSON 생성)
      → 대시보드가 5분 캐시 후 자동 최신 반영
```

**결과**: PC 전원만 켜져있으면 누구나 어디서나 24/7 최신 데이터 접근.

---

## 💡 참고

### 전력 소모
- PC 를 계속 켜두면 전기세 발생 (월 약 $10~30, 사용량 따라)
- 저전력 모드(모니터 꺼짐)는 유지되므로 체감 전력은 크지 않음

### 노트북이면
- 뚜껑 닫아도 안 꺼지게: 제어판 → 전원 옵션 → "덮개를 닫을 때" → **아무 것도 안 함**
- AC 전원 연결 필수 (배터리로는 한계)

### 더 안정적 운영 원하면
- **NSSM** 으로 Windows 서비스화 (데몬처럼 동작, 로그인 없어도 실행)
- **Fly.io / Railway** 클라우드 배포 ($5~10/월)
