$ErrorActionPreference = 'Stop'
$DockerPath = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $DockerPath)) { $DockerPath = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe' }
if (-not (Test-Path $DockerPath)) {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { $DockerPath = $cmd.Source } else { throw 'docker.exe was not found. Start Docker Desktop first.' }
}
$Top = & $DockerPath top infra-spark-1
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect Spark container process list.' }
if ($Top | Select-String '/opt/customer360/spark/streaming_feeds.py') {
    throw 'Customer360 feed landing stream already appears to be running; refusing to start a duplicate.'
}
$LinuxScript = @'
set -e
export HOME=/tmp/spark-home
export PYTHONPATH=/tmp/python-packages:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/customer360/spark
export PYSPARK_PYTHON=python3
mkdir -p /tmp/spark-ivy
/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 512m --conf spark.sql.shuffle.partitions=2 --conf spark.sql.adaptive.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /opt/customer360/spark/streaming_feeds.py > /tmp/streaming_feeds.log 2>&1
'@
$Encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxScript))
& $DockerPath exec -d infra-spark-1 bash -lc "echo $Encoded | base64 -d | bash"
if ($LASTEXITCODE -ne 0) { throw "Docker exec failed with exit code $LASTEXITCODE" }
Write-Host 'Started Customer360 brand/weather feed landing stream.' -ForegroundColor Green
Write-Host 'Log: /tmp/streaming_feeds.log inside infra-spark-1'
Write-Host 'Output: data/lake/feeds/brand_mentions and data/lake/feeds/weather'
