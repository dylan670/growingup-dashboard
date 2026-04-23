# ================================================================
# 대시보드 상시 가동 자동 셋업 (관리자 권한 필요)
#
# 실행:
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
#   .\scripts\setup_autostart.ps1
#
# 수행:
#   1. PC 슬립/최대절전 방지 (AC 전원)
#   2. Streamlit 부팅시 자동 실행 작업 등록
#   3. (옵션) Cloudflare Tunnel 자동 실행 등록
# ================================================================

$ErrorActionPreference = "Stop"
$projectPath = Split-Path $PSScriptRoot -Parent

Write-Host ""
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  그로잉업팀 대시보드 상시 가동 셋업" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""

# 관리자 권한 체크
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "[X] 관리자 권한 필요. PowerShell 을 '관리자 권한으로 실행' 한 뒤 재시도하세요." -ForegroundColor Red
    exit 1
}

# -----------------------------------------------------------
# 1. 전원 설정 — 슬립/최대절전 방지
# -----------------------------------------------------------
Write-Host "[1/3] 전원 설정 (슬립/최대절전 방지)..." -ForegroundColor Yellow
try {
    powercfg /change standby-timeout-ac 0 | Out-Null
    powercfg /change hibernate-timeout-ac 0 | Out-Null
    powercfg /change monitor-timeout-ac 30 | Out-Null
    Write-Host "    OK - AC 전원: 슬립/최대절전 비활성화. 모니터는 30분 후 꺼짐." -ForegroundColor Green
} catch {
    Write-Host "    FAIL - $($_.Exception.Message)" -ForegroundColor Red
}

# -----------------------------------------------------------
# 2. Streamlit 상시 실행 작업 등록
# -----------------------------------------------------------
Write-Host ""
Write-Host "[2/3] Streamlit 대시보드 작업 등록..." -ForegroundColor Yellow

$taskName = "그로잉업 대시보드 상시"
$batPath = Join-Path $projectPath "scripts\run_forever.bat"

if (-not (Test-Path $batPath)) {
    Write-Host "    FAIL - $batPath 파일 없음." -ForegroundColor Red
    exit 1
}

try {
    # 기존 작업 있으면 삭제
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }

    $action = New-ScheduledTaskAction -Execute $batPath
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $principal = New-ScheduledTaskPrincipal `
        -UserId "$env:USERDOMAIN\$env:USERNAME" `
        -LogonType Interactive `
        -RunLevel Highest
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 1)

    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Principal $principal `
        -Settings $settings `
        -Description "PC 부팅시 Streamlit 대시보드 자동 실행 + 크래시 자동 재시작" `
        -Force | Out-Null

    Write-Host "    OK - 작업 '$taskName' 등록 완료" -ForegroundColor Green
} catch {
    Write-Host "    FAIL - $($_.Exception.Message)" -ForegroundColor Red
}

# -----------------------------------------------------------
# 3. (옵션) Cloudflare Tunnel 자동 실행
# -----------------------------------------------------------
Write-Host ""
Write-Host "[3/3] Cloudflare Tunnel 자동 실행 (옵션)..." -ForegroundColor Yellow

$cloudflared = Get-Command cloudflared -ErrorAction SilentlyContinue
if (-not $cloudflared) {
    $cloudflared = "C:\tools\cloudflared.exe"
    if (-not (Test-Path $cloudflared)) {
        Write-Host "    SKIP - cloudflared 미설치." -ForegroundColor DarkYellow
        Write-Host "           외부(사무실 밖) 접근 필요하면:" -ForegroundColor DarkYellow
        Write-Host "           1. https://github.com/cloudflare/cloudflared/releases 다운로드" -ForegroundColor DarkYellow
        Write-Host "           2. C:\tools\cloudflared.exe 로 저장" -ForegroundColor DarkYellow
        Write-Host "           3. 이 스크립트 재실행" -ForegroundColor DarkYellow
        $cloudflared = $null
    }
}

if ($cloudflared) {
    $tunnelTaskName = "그로잉업 터널 상시"
    $tunnelBat = Join-Path $projectPath "scripts\run_tunnel_forever.bat"

    try {
        if (Get-ScheduledTask -TaskName $tunnelTaskName -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $tunnelTaskName -Confirm:$false
        }

        $tunnelAction = New-ScheduledTaskAction -Execute $tunnelBat
        # 대시보드 먼저 뜨게 30초 지연
        $tunnelTrigger = New-ScheduledTaskTrigger -AtStartup
        $tunnelTrigger.Delay = "PT30S"
        $tunnelPrincipal = New-ScheduledTaskPrincipal `
            -UserId "$env:USERDOMAIN\$env:USERNAME" `
            -LogonType Interactive
        $tunnelSettings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -ExecutionTimeLimit ([TimeSpan]::Zero)

        Register-ScheduledTask `
            -TaskName $tunnelTaskName `
            -Action $tunnelAction `
            -Trigger $tunnelTrigger `
            -Principal $tunnelPrincipal `
            -Settings $tunnelSettings `
            -Description "PC 부팅 30초 후 Cloudflare Tunnel 자동 실행" `
            -Force | Out-Null

        Write-Host "    OK - 작업 '$tunnelTaskName' 등록 완료 (부팅 30초 후)" -ForegroundColor Green
    } catch {
        Write-Host "    FAIL - $($_.Exception.Message)" -ForegroundColor Red
    }
}

# -----------------------------------------------------------
# 완료
# -----------------------------------------------------------
Write-Host ""
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  셋업 완료!" -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "다음 단계:"
Write-Host "  1. 지금 바로 테스트 → scripts\run_forever.bat 더블클릭"
Write-Host "  2. 또는 PC 재부팅 후 1분 대기 → 브라우저에서 접속"
Write-Host ""
Write-Host "등록된 작업 확인:"
Write-Host "  taskschd.msc → 작업 스케줄러 라이브러리"
Write-Host ""
Write-Host "해제하려면:"
Write-Host "  Unregister-ScheduledTask -TaskName '그로잉업 대시보드 상시' -Confirm:`$false"
Write-Host ""
