$ErrorActionPreference = 'Stop'
$DockerPath = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $DockerPath)) { $DockerPath = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe' }
if (-not (Test-Path $DockerPath)) {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { $DockerPath = $cmd.Source } else { throw 'docker.exe was not found. Start Docker Desktop first.' }
}
$SchemaFile = Join-Path $PSScriptRoot '..\hbase\provision_schema.hbase'
if (-not (Test-Path $SchemaFile)) { throw "HBase schema file not found: $SchemaFile" }
$RemoteSchema = '/tmp/customer360_provision_schema.hbase'
& $DockerPath cp $SchemaFile "hbase:$RemoteSchema"
if ($LASTEXITCODE -ne 0) { throw 'Failed to copy the non-destructive HBase schema script into the hbase container.' }
$Deadline = (Get-Date).AddMinutes(2)
$LastOutput = @()
while ((Get-Date) -lt $Deadline) {
    $LastOutput = & $DockerPath exec hbase bash -lc "/hbase/bin/hbase shell -n < $RemoteSchema 2>&1"
    $Exit = $LASTEXITCODE
    if ($Exit -eq 0 -and ($LastOutput -join "`n") -match 'customer_360') {
        $LastOutput | ForEach-Object { Write-Host $_ }
        Write-Host 'PASS_HBASE_CUSTOMER360_SCHEMA families=profile,metrics,cart' -ForegroundColor Green
        exit 0
    }
    Start-Sleep -Seconds 5
}
$LastOutput | ForEach-Object { Write-Host $_ }
throw "HBase schema provisioning did not succeed within 2 minutes. Existing data was not dropped."
