$ErrorActionPreference = 'Stop'
$DockerPath = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $DockerPath)) {
    $DockerPath = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
}
if (-not (Test-Path $DockerPath)) {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { $DockerPath = $cmd.Source } else { throw 'docker.exe was not found. Set $DockerPath in this script to your Docker Desktop CLI.' }
}

# Refuse to create two drivers using the same production checkpoint.
$ExistingTop = & $DockerPath top infra-spark-1
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect Spark processes in infra-spark-1.' }
$ExistingStream = $ExistingTop | Select-String '/opt/customer360/spark/streaming_abandonment.py'
if ($ExistingStream) {
    throw "A Customer360 streaming process already appears to be running. Stop/inspect it before starting another driver:`n$($ExistingStream -join [Environment]::NewLine)"
}

# Base64 transports the script as one argument so PowerShell does not corrupt
# the Linux shell quoting or the Spark package coordinates.
$LinuxCommand = @'
set -e
export HOME=/tmp/spark-home
export PYTHONPATH=/tmp/python-packages:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/customer360/spark
export PYSPARK_PYTHON=python3
export CKPT=/checkpoints/abandonment_v2
export MAX_OFFSETS_PER_TRIGGER=3000
export SPARK_STREAMING_SHUFFLE_PARTITIONS=4
mkdir -p /tmp/spark-ivy
/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 768m --conf spark.sql.shuffle.partitions=4 --conf spark.sql.adaptive.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /opt/customer360/spark/streaming_abandonment.py > /tmp/streaming_abandonment.log 2>&1
'@
$Encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxCommand))
& $DockerPath exec -d infra-spark-1 bash -lc "echo $Encoded | base64 -d | bash"
if ($LASTEXITCODE -ne 0) { throw "Docker exec failed with exit code $LASTEXITCODE" }
Write-Host 'Started Customer360 streaming job in infra-spark-1.' -ForegroundColor Green
Write-Host 'Log: /tmp/streaming_abandonment.log inside the container.'
Write-Host 'Checkpoint: /checkpoints/abandonment_v2 (existing checkpoint tree remains untouched).'
