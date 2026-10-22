param(
    [double]$Scale = 0.02,
    [string]$TargetRoot = (Join-Path $env:USERPROFILE 'Downloads\customer360\customer360'),
    [switch]$StartLiveFeeds
)
$ErrorActionPreference = 'Stop'
$SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path.TrimEnd('\','/')
$TargetRoot = [System.IO.Path]::GetFullPath($TargetRoot).TrimEnd('\','/')
$DockerPath = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $DockerPath)) { $DockerPath = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe' }
if (-not (Test-Path $DockerPath)) {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { $DockerPath = $cmd.Source } else { throw 'docker.exe was not found. Start Docker Desktop first.' }
}

Write-Host 'CUSTOMER360 COMPLETE-AND-START WORKFLOW' -ForegroundColor Cyan
$server = & $DockerPath version --format '{{.Server.Version}}'
if ($LASTEXITCODE -ne 0 -or -not $server) { throw 'Docker Desktop engine is not responding. Start Docker Desktop first.' }
Write-Host "PASS Docker Engine $server" -ForegroundColor Green
if (-not (Test-Path (Join-Path $TargetRoot 'infra\docker-compose.yml'))) {
    throw "Existing mounted project root was not found: $TargetRoot. Use -TargetRoot to point to the original project folder."
}

# Resolve a concrete Python executable (not just a PATH command) and capture the full probe output.
# This avoids Windows PowerShell 5.1 treating Python's stderr as a terminating NativeCommandError.
$PythonCandidates = @(
    (Join-Path $SourceRoot '.venv\Scripts\python.exe'),
    (Join-Path (Split-Path $TargetRoot -Parent) '.venv\Scripts\python.exe'),
    (Join-Path $TargetRoot '.venv\Scripts\python.exe')
)
$PythonExe = $null
foreach ($candidatePython in $PythonCandidates) {
    if (Test-Path -LiteralPath $candidatePython -PathType Leaf) {
        $PythonExe = (Resolve-Path -LiteralPath $candidatePython).Path
        break
    }
}
if (-not $PythonExe) {
    $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($pythonCommand) { $PythonExe = $pythonCommand.Source }
}
if (-not $PythonExe) {
    throw 'Python 3 was not found. Install Python 3 or create a .venv with requirements-local.txt before running this workflow.'
}
# Make subsequent child scripts resolve the same interpreter through both python and python.exe.
$env:PATH = "$(Split-Path -Parent $PythonExe);$env:PATH"
$priorErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$pythonProbe = @(& $PythonExe -c 'import numpy, pandas, sklearn, pytest; print("CUSTOMER360_PYTHON_DEPS=PASS")' 2>&1)
$pythonProbeExitCode = $LASTEXITCODE
$ErrorActionPreference = $priorErrorActionPreference
$pythonProbeText = $pythonProbe -join [Environment]::NewLine
if ($pythonProbeExitCode -ne 0 -or $pythonProbeText -notmatch 'CUSTOMER360_PYTHON_DEPS=PASS') {
    throw "Python dependency probe failed using '$PythonExe' (exit $pythonProbeExitCode). Output:`n$pythonProbeText"
}
Write-Host "PASS Python analytics dependencies ($PythonExe)" -ForegroundColor Green

Write-Host '=== 1/3 Back up and update the source mounted into Docker ===' -ForegroundColor Cyan
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $SourceRoot 'infra\install_project_update.ps1') -TargetRoot $TargetRoot
    if ($LASTEXITCODE -ne 0) { throw "Source update failed with exit code $LASTEXITCODE" }
} else {
    Write-Host 'Source and Docker-mounted project roots are the same; skipping file copy.'
}

Write-Host '=== 2/3 Run local, live-canary, and HBase latency acceptance checks ===' -ForegroundColor Cyan
Push-Location $SourceRoot
try {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $SourceRoot 'infra\verify_project.ps1') -Scale $Scale
    if ($LASTEXITCODE -ne 0) { throw "Acceptance suite failed with exit code $LASTEXITCODE. Production stream was not started." }
} finally {
    Pop-Location
}

Write-Host '=== 3/3 Start the normal production cart stream ===' -ForegroundColor Cyan
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $TargetRoot 'infra\run_streaming.ps1')
if ($LASTEXITCODE -ne 0) { throw "Production stream did not start (exit $LASTEXITCODE). Inspect /tmp/streaming_abandonment.log in infra-spark-1." }

$streamReady = $false
$streamDeadline = (Get-Date).AddSeconds(90)
while ((Get-Date) -lt $streamDeadline) {
    $streamStatus = & $DockerPath exec infra-spark-1 bash -lc "if grep -q 'CUSTOMER360_STREAM_STARTED' /tmp/streaming_abandonment.log 2>/dev/null; then echo READY; elif grep -qE 'CUSTOMER360_STREAM_FATAL|ERROR MicroBatchExecution|Query.*terminated with error' /tmp/streaming_abandonment.log 2>/dev/null; then echo FAILED; tail -n 35 /tmp/streaming_abandonment.log; else echo WAITING; fi"
    if ($streamStatus -match 'READY') { $streamReady = $true; break }
    if ($streamStatus -match 'FAILED') { throw "Production stream failed during startup:`n$($streamStatus -join [Environment]::NewLine)" }
    Start-Sleep -Seconds 3
}
if (-not $streamReady) {
    $streamTail = & $DockerPath exec infra-spark-1 bash -lc 'tail -n 40 /tmp/streaming_abandonment.log 2>/dev/null || true'
    throw "Production stream did not report CUSTOMER360_STREAM_STARTED within 90 seconds:`n$($streamTail -join [Environment]::NewLine)"
}
Write-Host 'PASS_CUSTOMER360_PRODUCTION_STREAM_STARTED' -ForegroundColor Green

if ($StartLiveFeeds) {
    Write-Host 'Starting optional Bluesky/weather/clickstream replay profile...' -ForegroundColor Cyan
    $composeFile = Join-Path $TargetRoot 'infra\docker-compose.yml'
    & $DockerPath compose --project-name infra -f $composeFile --profile live-feeds up -d
    if ($LASTEXITCODE -ne 0) { throw 'Optional live-feeds Compose profile failed to start. The core cart stream was already started.' }
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $TargetRoot 'infra\run_feeds.ps1')
    if ($LASTEXITCODE -ne 0) { throw 'The separate Spark brand/weather feed landing stream failed to start.' }
    $feedsReady = $false
    $feedsDeadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $feedsDeadline) {
        $feedsStatus = & $DockerPath exec infra-spark-1 bash -lc "if grep -q 'CUSTOMER360_FEEDS_STARTED' /tmp/streaming_feeds.log 2>/dev/null; then echo READY; elif grep -qE 'CUSTOMER360_FEEDS_FATAL|ERROR MicroBatchExecution|Query.*terminated with error' /tmp/streaming_feeds.log 2>/dev/null; then echo FAILED; tail -n 35 /tmp/streaming_feeds.log; else echo WAITING; fi"
        if ($feedsStatus -match 'READY') { $feedsReady = $true; break }
        if ($feedsStatus -match 'FAILED') { throw "Feed landing stream failed during startup:`n$($feedsStatus -join [Environment]::NewLine)" }
        Start-Sleep -Seconds 3
    }
    if (-not $feedsReady) {
        $feedsTail = & $DockerPath exec infra-spark-1 bash -lc 'tail -n 40 /tmp/streaming_feeds.log 2>/dev/null || true'
        throw "Feed landing stream did not report CUSTOMER360_FEEDS_STARTED within 90 seconds:`n$($feedsTail -join [Environment]::NewLine)"
    }
    Write-Host 'PASS_CUSTOMER360_FEED_LANDING_STARTED' -ForegroundColor Green
    Write-Host 'Live public feeds and feed landing were requested. Check their logs for external connectivity:' -ForegroundColor Green
    Write-Host '  /tmp/streaming_feeds.log in infra-spark-1'
    Write-Host '  docker logs weather-producer / docker logs bluesky-producer'
}

Write-Host 'CUSTOMER360_COMPLETE_WORKFLOW=PASS' -ForegroundColor Green
Write-Host 'Normal cart stream log: /tmp/streaming_abandonment.log in infra-spark-1'
Write-Host 'Check positive KAFKA_ALERT_BATCH and HBASE_BATCH counts after events are ingested.'
Write-Host 'Airflow remains stopped to conserve memory; start it only for the orchestration acceptance tasks.'
