param(
    [double]$Scale = 0.02,
    [switch]$SkipLocal,
    [switch]$SkipLive,
    [switch]$SkipLookup
)
$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$DockerPath = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $DockerPath)) { $DockerPath = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe' }
if (-not (Test-Path $DockerPath)) {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { $DockerPath = $cmd.Source } else { throw 'docker.exe was not found. Start Docker Desktop first.' }
}
$Results = [System.Collections.Generic.List[string]]::new()
Push-Location $ProjectRoot
try {
    Write-Host 'CUSTOMER360 ACCEPTANCE SUITE' -ForegroundColor Cyan
    Write-Host "Project root: $ProjectRoot"
    if (-not $SkipLocal) {
        Write-Host "=== LOCAL END-TO-END ANALYTICS (scale=$Scale) ===" -ForegroundColor Cyan
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File '.\run_local.ps1' -Scale $Scale
        if ($LASTEXITCODE -ne 0) { throw "Local analytics pipeline failed with exit code $LASTEXITCODE" }
        $Results.Add('PASS_LOCAL_ANALYTICS_AND_TESTS')
    } else { Write-Host 'SKIP_LOCAL_ANALYTICS' }

    if (-not $SkipLive) {
        Write-Host '=== LIVE KAFKA -> SPARK -> KAFKA + HBASE CANARY ===' -ForegroundColor Cyan
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File '.\infra\verify_live_integration.ps1'
        if ($LASTEXITCODE -ne 0) { throw "Live integration canary failed with exit code $LASTEXITCODE" }
        $Results.Add('PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION_AND_CANARY_SLA')
    } else { Write-Host 'SKIP_LIVE_INTEGRATION' }

    if (-not $SkipLookup) {
        Write-Host '=== HBASE CUSTOMER LOOKUP P99 ===' -ForegroundColor Cyan
        $lookupOutput = & $DockerPath exec infra-spark-1 bash -lc 'cd /opt/customer360 && PYTHONPATH=/tmp/python-packages python3 hbase/lookup_benchmark.py 1000 200000'
        $lookupExit = $LASTEXITCODE
        $lookupOutput | ForEach-Object { Write-Host $_ }
        if ($lookupExit -ne 0) { throw "HBase lookup latency acceptance failed (exit $lookupExit)" }
        $Results.Add('PASS_HBASE_LOOKUP_P99_LT_100MS')
    } else { Write-Host 'SKIP_HBASE_LOOKUP_BENCHMARK' }

    Write-Host '=== ACCEPTANCE SUMMARY ===' -ForegroundColor Cyan
    $Results | ForEach-Object { Write-Host $_ -ForegroundColor Green }
    Write-Host ("CUSTOMER360_ACCEPTANCE_SUITE=PASS checks=" + $Results.Count) -ForegroundColor Green
} finally {
    Pop-Location
}
