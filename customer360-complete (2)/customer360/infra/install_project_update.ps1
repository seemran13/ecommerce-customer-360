param(
    [string]$TargetRoot = (Join-Path $env:USERPROFILE 'Downloads\customer360\customer360')
)
$ErrorActionPreference = 'Stop'
$SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path.TrimEnd('\','/')
$TargetRoot = [System.IO.Path]::GetFullPath($TargetRoot).TrimEnd('\','/')

if (-not (Test-Path (Join-Path $SourceRoot 'infra\docker-compose.yml'))) {
    throw "Source root is not a Customer360 project: $SourceRoot"
}
if (-not (Test-Path (Join-Path $TargetRoot 'infra\docker-compose.yml'))) {
    throw "Target root is not an existing Customer360 project: $TargetRoot. Pass -TargetRoot to the existing project folder."
}
if ($SourceRoot.Equals($TargetRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Source and target project roots are identical; no copy is needed.'
}

$BackupRoot = Join-Path (Split-Path $TargetRoot -Parent) ("customer360_code_backup_" + (Get-Date -Format 'yyyyMMdd_HHmmss'))
$Copied = 0
$BackedUp = 0
$SkipSegments = @('.git', '.venv', '__pycache__', '.pytest_cache', 'data', 'data_small', 'output', 'logs', 'checkpoints')
$Files = Get-ChildItem -LiteralPath $SourceRoot -File -Recurse | Where-Object {
    if ($_.Extension -eq '.pyc') { return $false }
    $rel = $_.FullName.Substring($SourceRoot.Length).TrimStart('\','/')
    $parts = $rel -split '[\\/]'
    foreach ($part in $parts) {
        if ($SkipSegments -contains $part -or $part -like 'data_small_run_*' -or $part -like 'data_full_run_*') { return $false }
    }
    return $true
}

foreach ($file in $Files) {
    $relative = $file.FullName.Substring($SourceRoot.Length).TrimStart('\','/')
    $destination = Join-Path $TargetRoot $relative
    $destinationDir = Split-Path $destination -Parent
    New-Item -ItemType Directory -Force -Path $destinationDir | Out-Null
    if (Test-Path -LiteralPath $destination -PathType Leaf) {
        $backup = Join-Path $BackupRoot $relative
        New-Item -ItemType Directory -Force -Path (Split-Path $backup -Parent) | Out-Null
        Copy-Item -LiteralPath $destination -Destination $backup -Force
        $BackedUp++
    }
    Copy-Item -LiteralPath $file.FullName -Destination $destination -Force
    $Copied++
}

Write-Host "CUSTOMER360_CODE_UPDATE=PASS" -ForegroundColor Green
Write-Host "Source:   $SourceRoot"
Write-Host "Target:   $TargetRoot"
Write-Host "Files copied: $Copied"
Write-Host "Existing files backed up: $BackedUp"
Write-Host "Backup folder: $BackupRoot"
Write-Host 'No data directories, Docker volumes, or Spark checkpoints were deleted.' -ForegroundColor Yellow
