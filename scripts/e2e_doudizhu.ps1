# e2e_doudizhu.ps1 — 斗地主 3 人端到端集成测试（单机形态）
#
# 验证全链路：匹配 → 叫分 → 出牌 → 结算 → 存档落库。
# 流程：构建 → 生成临时配置（放开 origin + 隔离端口）→ 后台启 server →
#       等端口就绪 → 跑 benchbot doudizhu → 断言 finished=true →
#       经 admin HTTP 校验 3 名玩家存档 → 清理（无论成败）。
#
# 用法（PowerShell，需 Go 1.24+）：
#   .\scripts\e2e_doudizhu.ps1
#   .\scripts\e2e_doudizhu.ps1 -WsPort 18001 -AdminPort 18100 -Codec json
#
# 退出码：0=通过，非 0=失败。

param(
    [int]$WsPort    = 17001,  # 隔离端口，避免与开发中服务冲突
    [int]$AdminPort = 17100,
    [string]$Codec  = "pb",   # pb | json
    [int]$Round     = 1       # 连续对局轮数，验证稳定性
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$tmpCfg   = Join-Path $repoRoot "configs\_e2e_ddz.yaml"
$logFile  = Join-Path $env:TEMP ("e2e_ddz_server_" + [guid]::NewGuid().ToString("N") + ".log")
$env:GOTOOLCHAIN = "local"

$serverProc = $null
$cleanedUp  = $false

function Cleanup {
    if ($script:cleanedUp) { return }
    $script:cleanedUp = $true
    if ($serverProc -and -not $serverProc.HasExited) {
        try { Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue } catch {}
    }
    if (Test-Path $tmpCfg) { Remove-Item $tmpCfg -Force -ErrorAction SilentlyContinue }
}
trap { Cleanup; throw }

try {
    Push-Location $repoRoot

    # 1) 构建（编译期就暴露问题，比 go run 更可控）
    Write-Host "[1/5] building..." -ForegroundColor Cyan
    go build ./...
    if ($LASTEXITCODE -ne 0) { throw "go build failed" }

    # 2) 临时配置：放开 allowed_origins（bot 用 Origin: *），端口隔离，storage=memory 零外部依赖
    Write-Host "[2/5] writing temp config ($tmpCfg)..." -ForegroundColor Cyan
    @"
server:
  role: standalone

tcp:
  addr: ":7000"
  read_timeout: 30s
  write_channel_size: 256
  write_flush_interval: 2ms
  max_frame_size: 65536
  tcp_nodelay: true

ws:
  enabled: true
  addr: ":$WsPort"
  path: "/ws"
  allowed_origins:
    - "*"
  read_timeout: 30s
  write_channel_size: 256
  max_frame_size: 65536

session:
  grace_period: 60s
  replay_buffer: 256
  heartbeat_ms: 15000
  max_login_body: 4096
  sweep_interval: 5s

storage:
  driver: memory
  writeback_queue: 4096
  flush_interval: 10ms
  flush_batch: 256
  flush_retry: 2
  breaker_cooldown: 5s
  sync_timeout: 2s

admin:
  addr: "127.0.0.1:$AdminPort"

profile:
  checkpoint_interval: 60s

log:
  level: warn
"@ | Set-Content -Path $tmpCfg -Encoding UTF8

    # 3) 后台启动 server
    Write-Host "[3/5] starting server (ws=:$WsPort admin=127.0.0.1:$AdminPort)..." -ForegroundColor Cyan
    $serverProc = Start-Process -FilePath "go" `
        -ArgumentList @("run", "./cmd/server", "-config", $tmpCfg) `
        -RedirectStandardOutput $logFile -RedirectStandardError "$logFile.err" `
        -PassThru -WindowStyle Hidden

    # 等 WS 端口就绪（最多 30s）
    $ready = $false
    for ($i = 0; $i -lt 100; $i++) {
        if (Get-NetTCPConnection -LocalPort $WsPort -State Listen -ErrorAction SilentlyContinue) { $ready = $true; break }
        if ($serverProc.HasExited) { break }
        Start-Sleep -Milliseconds 300
    }
    if (-not $ready) {
        Write-Host "--- server log ---" -ForegroundColor Red
        if (Test-Path $logFile) { Get-Content $logFile -Tail 30 }
        if (Test-Path "$logFile.err") { Get-Content "$logFile.err" -Tail 30 }
        throw "server did not become ready on :$WsPort"
    }

    $addr = "ws://127.0.0.1:$WsPort/ws"

    # 4) 跑 N 轮对局，每轮断言自然结算
    for ($r = 1; $r -le $Round; $r++) {
        Write-Host "[4/5] running doudizhu match (round $r/$Round, codec=$Codec)..." -ForegroundColor Cyan
        $out = & go run ./cmd/benchbot -scenario doudizhu -addr $addr -codec $Codec -duration 90s 2>&1
        $out | ForEach-Object { Write-Host $_ }

        $line = ($out | Select-String '^\[doudizhu\] finished=(\S+) landlord="([^"]*)" landlord_win=(\S+) base=(\d+)').Matches
        if (-not $line -or $line.Count -eq 0) { throw "round ${r}: missing [doudizhu] result line" }
        $finished = $line[0].Groups[1].Value
        $landlord = $line[0].Groups[2].Value
        if ($finished -ne "true") { throw "round ${r}: match did not finish (finished=$finished)" }
        if ([string]::IsNullOrEmpty($landlord)) { throw "round ${r}: landlord empty" }
        Write-Host "  round $r OK: landlord=$landlord" -ForegroundColor Green

        # 提取本轮到访 UID（紧跟 "uids:" 之后的缩进行）
        $uids = @()
        $capture = $false
        foreach ($l in $out) {
            if ($l -match '^uids:') { $capture = $true; continue }
            if ($capture) {
                if ($l -match '^\s+(\S+)$') { $uids += $Matches[1] } else { $capture = $false }
            }
        }
        if ($uids.Count -ne 3) { throw "round ${r}: expected 3 uids, got $($uids.Count)" }

        # 5) 校验存档：3 名玩家 extra.doudizhu_plays 落库（金币走 storage gold 表，不在 profile.coin）
        foreach ($uid in $uids) {
            $resp = Invoke-WebRequest "http://127.0.0.1:$AdminPort/admin/players/$uid" -UseBasicParsing -TimeoutSec 5
            if ($resp.Content -notmatch '"doudizhu_plays"\s*:\s*1') {
                throw "round ${r}: archive check failed for $uid : $($resp.Content)"
            }
        }
        Write-Host "  round $r archive OK ($($uids -join ', '))" -ForegroundColor Green
    }

    Write-Host "`nALL PASSED (rounds=$Round)" -ForegroundColor Green
    exit 0
}
finally {
    Pop-Location
    Cleanup
}
