# termx service e2e 统一入口：按套件构建 → 启动 termx-service → 跑对应 e2e → 只停本套件启动的服务。
# 用法：pwsh -File scripts/run-termx-e2e.ps1 [-Suite payload|read-last|incomplete|all] [-Port 8170] [-TargetDir <dir>] [-RuntimeSource <dir>] [-SkipBuild]
# -Suite all 时逐套件独立 config 目录并逐个跑；任一套件失败继续跑其余，最后汇总失败数并以非零退出。
# runtime catalog（termx-pwsh 等整包 pwsh）不随套件重建：每套件 config/runtimes 以目录 junction
# 指向共享部署（-RuntimeSource，缺省 .tmp/termx-e2e-runtime；首次使用需把一份 termx-pwsh 部署放进去，
# 结构为 index.json + termx-pwsh/）。
param(
  [ValidateSet('payload', 'read-last', 'incomplete', 'all')]
  [string]$Suite = 'all',
  [int]$Port = 8170,
  [string]$TargetDir = '',
  [string]$RuntimeSource = '',
  [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not $TargetDir) { $TargetDir = Join-Path $root '.tmp\target-ack' }
if (-not $RuntimeSource) { $RuntimeSource = Join-Path $root '.tmp\termx-e2e-runtime' }
$svcPath = Join-Path $TargetDir 'debug\termx-service.exe'
$pluginDir = Join-Path $root 'opencode-plugin\termx'
$e2eBase = Join-Path $root '.tmp\termx-e2e'
$suites = if ($Suite -eq 'all') { @('payload', 'read-last', 'incomplete') } else { @($Suite) }

function Test-PortBusy {
  # 用 TcpClient 探测端口（Test-NetConnection 每次解析开销大，这里只要快）。
  param([int]$ListenPort)
  try {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
      $ok = $client.ConnectAsync('127.0.0.1', $ListenPort).Wait(500)
      return ($ok -and $client.Connected)
    } finally { $client.Dispose() }
  } catch { return $false }
}

function Wait-PortReady {
  param([int]$ListenPort, [int]$TimeoutMs = 20000)
  $deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)
  while ([DateTime]::UtcNow -lt $deadline) {
    if (Test-PortBusy $ListenPort) { return $true }
    Start-Sleep -Milliseconds 250
  }
  return $false
}

function Invoke-Suite {
  # 跑单个套件：独立 config 目录 → 清 stale → 启动 service → 跑 mjs → 停 service。
  param([string]$Name)

  $suiteDir = Join-Path $e2eBase $Name
  $configDir = Join-Path $suiteDir 'config'
  $logDir = Join-Path $suiteDir 'logs'
  # 先拆掉上一轮遗留的 junction 再删套件目录：不同 pwsh 版本对「删除含 junction 的目录」
  # 是否穿透目标的行为不一致，显式拆链只删链接本身，共享 runtime 部署不受影响。
  $staleJunction = Join-Path $configDir 'runtimes'
  if (Test-Path $staleJunction -PathType Container) {
    $item = Get-Item $staleJunction -Force
    if ($item.LinkType -eq 'Junction') { $item.Delete() }
  }
  if (Test-Path $suiteDir) { Remove-Item -Recurse -Force $suiteDir }
  New-Item -ItemType Directory -Force -Path $configDir, $logDir | Out-Null

  # runtime catalog 不重建：把共享部署 junction 进本套件 config（只读共享）。
  if (-not (Test-Path (Join-Path $RuntimeSource 'index.json'))) {
    throw "missing runtime catalog: $RuntimeSource（需含 index.json 与 termx-pwsh/，可用 -RuntimeSource 指向一份已有部署）"
  }
  New-Item -ItemType Junction -Path (Join-Path $configDir 'runtimes') -Target $RuntimeSource | Out-Null

  # 只清理同 target 下旧实例（精确 exe 路径过滤），绝不动其它 target 的常驻服务。
  Get-Process termx-service -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -eq $svcPath } |
    ForEach-Object {
      Write-Host "[$Name] stop stale target pid=$($_.Id)"
      Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }
  for ($i = 0; $i -lt 20; $i++) {
    if (-not (Test-PortBusy $Port)) { break }
    Start-Sleep -Milliseconds 250
  }
  if (Test-PortBusy $Port) {
    throw "[$Name] port $Port is still in use by another process; refusing to start"
  }

  $svc = Start-Process -FilePath $svcPath `
    -ArgumentList @(
      '--config-dir', $configDir,
      '--port', "$Port",
      '--log-dir', $logDir,
      '--log-enabled', 'true',
      '--log-max-days', '1'
    ) `
    -WorkingDirectory $root -WindowStyle Hidden -PassThru
  Write-Host "[$Name] started pid=$($svc.Id)"

  $code = 1
  try {
    if (-not (Wait-PortReady $Port)) {
      throw "[$Name] termx-service did not become ready on port $Port"
    }
    # Out-Host：node 输出必须绕过赋值捕获（$code = Invoke-Suite 会吞管道输出），直达控制台。
    & node (Join-Path $root "scripts\e2e\$Name-e2e.mjs") $configDir 2>&1 | Out-Host
    $code = $LASTEXITCODE
  } finally {
    # 优先按 PID 停本套件启动的服务；兜底再按精确 exe 路径扫一遍。
    Stop-Process -Id $svc.Id -Force -ErrorAction SilentlyContinue
    Get-Process termx-service -ErrorAction SilentlyContinue |
      Where-Object { $_.Path -eq $svcPath } |
      ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
    Write-Host "[$Name] stopped service; exit=$code"
  }
  return $code
}

if (-not $SkipBuild) {
  Write-Output '==> cargo build -p termx-service'
  cargo build -p termx-service --target-dir $TargetDir
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  Write-Output '==> npm run build (opencode-plugin/termx)'
  Push-Location $pluginDir
  try {
    npm run build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally { Pop-Location }
}

if (-not (Test-Path $svcPath)) { throw "missing test service: $svcPath" }
if (-not (Test-Path (Join-Path $pluginDir 'dist\cli.js'))) {
  throw 'missing plugin dist: opencode-plugin/termx/dist（先 npm run build 或去掉 -SkipBuild）'
}

$failed = @()
foreach ($s in $suites) {
  Write-Output "==> suite: $s"
  try {
    $code = Invoke-Suite -Name $s
    if ($code -ne 0) { $failed += $s }
  } catch {
    Write-Error $_
    $failed += $s
  }
}

if ($failed.Count -gt 0) {
  Write-Output "FAILED suites: $($failed -join ', ')（共 $($failed.Count) 个）"
  exit 1
}
Write-Output 'ALL SUITES OK'
exit 0
