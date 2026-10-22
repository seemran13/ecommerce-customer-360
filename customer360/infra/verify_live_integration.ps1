$ErrorActionPreference = 'Stop'

# End-to-end canary test against the existing Docker Desktop stack.
# Uses isolated Kafka topics, a separate Spark checkpoint, and a temporary HBase row.
# The temporary HBase row is removed by the Python probe; original data/checkpoints are untouched.

$DockerPath = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $DockerPath)) {
    $DockerPath = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
}
if (-not (Test-Path $DockerPath)) {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { $DockerPath = $cmd.Source } else { throw 'docker.exe was not found. Start Docker Desktop and set $DockerPath in this script.' }
}

$SparkContainer = 'infra-spark-1'
$RequiredContainers = @($SparkContainer, 'hbase', 'kafka1', 'kafka2', 'kafka3')
$Tag = Get-Date -Format 'yyyyMMddHHmmss'
$InputTopic = "c360_it_events_$Tag"
$AlertTopic = "c360_it_alerts_$Tag"
$Checkpoint = "/checkpoints/c360_live_it_$Tag"
$WindowPath = "/tmp/c360_live_it_$Tag/windows"
$LogPath = "/tmp/c360_live_it_$Tag.log"
$PidPath = "/tmp/c360_live_it_$Tag.pid"
$RemoteStream = "/tmp/c360_live_it_stream_$Tag.py"
$RemoteProbe = "/tmp/c360_live_it_probe_$Tag.py"
$StreamSource = Join-Path $PSScriptRoot '..\spark\streaming_abandonment.py'

function Invoke-SparkBash([string]$LinuxScript) {
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxScript))
    $output = & $DockerPath exec $SparkContainer bash -lc "echo $encoded | base64 -d | bash"
    if ($LASTEXITCODE -ne 0) {
        throw "Command inside $SparkContainer failed (exit $LASTEXITCODE). Output: $($output -join [Environment]::NewLine)"
    }
    return $output
}

function Test-ContainerRunning([string]$Name, [string[]]$StatusLines) {
    foreach ($line in $StatusLines) {
        if ($line -match ('^' + [regex]::Escape($Name) + '\|Up\b')) { return $true }
    }
    return $false
}

$SparkStarted = $false
try {
    Write-Host '=== 1/6 Docker engine ===' -ForegroundColor Cyan
    $serverVersion = & $DockerPath version --format '{{.Server.Version}}'
    if ($LASTEXITCODE -ne 0 -or -not $serverVersion) { throw 'Docker server is unavailable. Start Docker Desktop first.' }
    Write-Host "PASS Docker Engine $serverVersion" -ForegroundColor Green

    Write-Host '=== 2/6 required containers ===' -ForegroundColor Cyan
    $status = & $DockerPath ps --format '{{.Names}}|{{.Status}}'
    if ($LASTEXITCODE -ne 0) { throw 'Could not read Docker container status.' }
    foreach ($name in $RequiredContainers) {
        if (-not (Test-ContainerRunning -Name $name -StatusLines $status)) {
            throw "Required container '$name' is not running. Current containers:`n$($status -join [Environment]::NewLine)"
        }
        Write-Host "PASS container $name is Up" -ForegroundColor Green
    }

    # Avoid launching a second Spark streaming driver alongside a production stream.
    $top = & $DockerPath top $SparkContainer
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect Spark container processes.' }
    $alreadyStreaming = $top | Select-String 'streaming_abandonment\.py|streaming_abandonment_recovery|c360_live_it_stream_'
    if ($alreadyStreaming) {
        throw "A Customer360 Spark stream appears to be running already. Stop that stream before running this isolated canary test. Found:`n$($alreadyStreaming -join [Environment]::NewLine)"
    }

    Write-Host '=== 3/6 network and existing resources ===' -ForegroundColor Cyan
    $networkScript = @'
set -e
for spec in hbase:9090 hbase:16020 kafka1:9092 kafka2:9092 kafka3:9092; do
  host=${spec%:*}; port=${spec##*:}
  if timeout 3 bash -c ":</dev/tcp/$host/$port" 2>/dev/null; then echo "PASS_TCP $spec"; else echo "FAIL_TCP $spec"; exit 2; fi
done
'@
    $networkOutput = Invoke-SparkBash $networkScript
    $networkOutput | ForEach-Object { Write-Host $_ }

    # Provision required HBase families non-destructively. Existing rows are retained;
    # missing profile/metrics families are added so the nightly bulk sync can also run.
    Write-Host '=== HBase customer_360 schema ===' -ForegroundColor Cyan
    $SchemaSource = Join-Path $PSScriptRoot '..\hbase\provision_schema.hbase'
    if (-not (Test-Path $SchemaSource)) { throw "HBase schema script not found: $SchemaSource" }
    $RemoteSchema = '/tmp/customer360_provision_schema.hbase'
    & $DockerPath cp $SchemaSource "hbase:$RemoteSchema"
    if ($LASTEXITCODE -ne 0) { throw 'Could not copy HBase schema provisioning script into the hbase container.' }
    $schemaReady = $false
    $schemaDeadline = (Get-Date).AddMinutes(2)
    $schemaOutput = @()
    while ((Get-Date) -lt $schemaDeadline) {
        $schemaOutput = & $DockerPath exec hbase bash -lc "/hbase/bin/hbase shell -n < $RemoteSchema 2>&1"
        $schemaExit = $LASTEXITCODE
        if ($schemaExit -eq 0 -and (($schemaOutput -join "`n") -match 'customer_360')) { $schemaReady = $true; break }
        Start-Sleep -Seconds 5
    }
    if (-not $schemaReady) { throw "HBase schema provisioning failed or Master not ready:`n$($schemaOutput -join [Environment]::NewLine)" }
    $schemaOutput | ForEach-Object { Write-Host $_ }
    Write-Host 'PASS_HBASE_CUSTOMER360_SCHEMA families=profile,metrics,cart' -ForegroundColor Green

    # TCP can open before the HBase master/RegionServer can answer metadata RPCs.
    # Wait for a real Thrift round-trip before starting a Spark canary.
    Write-Host '=== HBase Thrift RPC readiness ===' -ForegroundColor Cyan
    $hbaseReadyPython = @'
import sys, time
import happybase

deadline = time.monotonic() + 90
last_error = None
while time.monotonic() < deadline:
    conn = None
    try:
        conn = happybase.Connection("hbase", port=9090, timeout=10000, autoconnect=False)
        conn.open()
        tables = conn.tables()
        if b"customer_360" not in tables:
            raise RuntimeError("customer_360 table not found")
        families = set(conn.table("customer_360").families())
        required = {b"cart", b"profile", b"metrics"}
        if not required.issubset(families):
            raise RuntimeError("customer_360 is missing column families: " + repr(sorted(required - families)))
        conn.table("customer_360").row(b"0000000101")
        print("PASS_HBASE_THRIFT_RPC customer_360")
        sys.exit(0)
    except Exception as exc:
        last_error = exc
        time.sleep(3)
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass
print("FAIL_HBASE_THRIFT_RPC", type(last_error).__name__ if last_error else "Timeout", repr(last_error), file=sys.stderr)
sys.exit(1)
'@
    $encodedHBaseReady = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($hbaseReadyPython))
    $hbaseReadyOutput = Invoke-SparkBash "echo $encodedHBaseReady | base64 -d | PYTHONPATH=/tmp/python-packages python3"
    $hbaseReadyOutput | ForEach-Object { Write-Host $_ }

    $topicList = & $DockerPath exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list
    if ($LASTEXITCODE -ne 0) { throw 'Kafka CLI could not list topics.' }
    foreach ($topic in @('cart_events', 'cart_alerts')) {
        if (-not ($topicList -contains $topic)) { throw "Required Kafka topic '$topic' is missing. Run infra\create_topics.sh first." }
        Write-Host "PASS Kafka topic $topic exists" -ForegroundColor Green
    }

    if (-not (Test-Path $StreamSource)) { throw "Updated stream source not found: $StreamSource. Extract the complete fixed project into your existing project folder first." }
    Write-Host '=== 4/6 prepare isolated canary ===' -ForegroundColor Cyan
    & $DockerPath cp $StreamSource "${SparkContainer}:$RemoteStream"
    if ($LASTEXITCODE -ne 0) { throw 'Could not copy the updated streaming script into the Spark container.' }

    foreach ($topic in @($InputTopic, $AlertTopic)) {
        & $DockerPath exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 3 --config min.insync.replicas=2
        if ($LASTEXITCODE -ne 0) { throw "Could not create isolated Kafka integration-test topic '$topic'." }
    }
    Write-Host "Created isolated topics: $InputTopic, $AlertTopic" -ForegroundColor Green

    $linuxStart = @"
set -eu
export HOME=/tmp/spark-home
export PYTHONPATH=/tmp/python-packages:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/customer360/spark
export PYSPARK_PYTHON=python3
export KAFKA_INPUT_TOPIC='$InputTopic'
export ALERT_TOPIC='$AlertTopic'
export CKPT='$Checkpoint'
export IDLE_SEC=30
export WATERMARK_DELAY='5 seconds'
export MAX_OFFSETS_PER_TRIGGER=100
export SPARK_STREAMING_SHUFFLE_PARTITIONS=2
export WINDOW_PATH='$WindowPath'
mkdir -p /tmp/spark-ivy
nohup /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 768m --conf spark.sql.shuffle.partitions=2 --conf spark.sql.adaptive.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 '$RemoteStream' > '$LogPath' 2>&1 &
echo `$! > '$PidPath'
"@
    $startOutput = Invoke-SparkBash $linuxStart
    $startOutput | ForEach-Object { Write-Host $_ }
    $SparkStarted = $true

    # Wait for the streaming queries to bind successfully. Collect log tail on failure.
    $ready = $false
    $deadline = (Get-Date).AddMinutes(3)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 2
        $probeReady = @"
if grep -q 'CUSTOMER360_STREAM_STARTED' '$LogPath' 2>/dev/null; then echo READY; elif grep -qE 'CUSTOMER360_STREAM_FATAL|ERROR MicroBatchExecution|Query.*terminated with error' '$LogPath' 2>/dev/null; then echo FAILED; tail -n 35 '$LogPath'; else echo WAITING; fi
"@
        $readyOutput = Invoke-SparkBash $probeReady
        if ($readyOutput -match 'READY') { $ready = $true; break }
        if ($readyOutput -match 'FAILED') { throw "Spark failed during startup:`n$($readyOutput -join [Environment]::NewLine)" }
    }
    if (-not $ready) {
        $tail = Invoke-SparkBash "tail -n 50 '$LogPath' 2>/dev/null || true"
        throw "Spark did not report CUSTOMER360_STREAM_STARTED within 3 minutes:`n$($tail -join [Environment]::NewLine)"
    }
    Write-Host "PASS Spark streaming query started on $InputTopic" -ForegroundColor Green

    Write-Host '=== 5/6 publish canary and wait for Kafka alert + HBase write ===' -ForegroundColor Cyan
    $probePython = @'
import json, random, sys, time, uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer, KafkaProducer
import happybase

input_topic, alert_topic, tag = sys.argv[1:4]
brokers = ["kafka1:9092", "kafka2:9092", "kafka3:9092"]
conn = None
consumer = None
producer = None
row_key = None
try:
    conn = happybase.Connection("hbase", port=9090, timeout=10000, autoconnect=False)
    conn.open()
    names = conn.tables()
    if b"customer_360" not in names:
        raise RuntimeError("HBase table customer_360 is missing")
    table = conn.table("customer_360")
    families = table.families()
    if b"cart" not in families:
        raise RuntimeError("HBase customer_360 table is missing the cart column family")

    # Find a currently unused key so the test never overwrites a real customer.
    for _ in range(100):
        customer_id = random.randint(900000000, 999999998)
        candidate = f"{customer_id:010d}".encode("ascii")
        if not table.row(candidate):
            row_key = candidate
            break
    if row_key is None:
        raise RuntimeError("Could not find a free canary customer row key")

    session_id = "c360-integration-" + uuid.uuid4().hex
    now = datetime.now(timezone.utc).replace(microsecond=0)
    cart_ts = (now - timedelta(seconds=30)).replace(tzinfo=None).isoformat(timespec="seconds")
    tick1 = (now + timedelta(seconds=120)).replace(tzinfo=None).isoformat(timespec="seconds")
    tick2 = (now + timedelta(seconds=150)).replace(tzinfo=None).isoformat(timespec="seconds")

    consumer = KafkaConsumer(
        alert_topic, bootstrap_servers=brokers,
        group_id="c360-integration-" + tag,
        enable_auto_commit=False, auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    consumer.poll(timeout_ms=1000)  # establish assignment before publishing the canary
    producer = KafkaProducer(
        bootstrap_servers=brokers, acks="all", retries=5,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )

    common = {
        "session_id": session_id,
        "customer_id": customer_id,
        "product_id": 987654321,
        "price": 12.5,
        "qty": 2,
        "order_id": None,
        "device": "integration-test",
        "ip": "127.0.0.1",
    }
    cart_event = dict(common, event_id=str(uuid.uuid4()), event_ts=cart_ts, event_type="add_to_cart")
    meta = {"session_id": "__watermark__", "customer_id": 0, "product_id": None, "price": None, "qty": None, "order_id": None, "device": "system", "ip": None}
    tick_event_1 = dict(meta, event_id=str(uuid.uuid4()), event_ts=tick1, event_type="watermark_tick")
    tick_event_2 = dict(meta, event_id=str(uuid.uuid4()), event_ts=tick2, event_type="watermark_tick")

    producer.send(input_topic, key=customer_id, value=cart_event).get(timeout=10)
    producer.flush(timeout=10)
    time.sleep(6)  # allow the cart event to be committed before advancing its watermark
    producer.send(input_topic, key=0, value=tick_event_1).get(timeout=10)
    producer.flush(timeout=10)
    time.sleep(8)
    producer.send(input_topic, key=0, value=tick_event_2).get(timeout=10)
    producer.flush(timeout=10)

    got_alert = None
    got_hbase = None
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        for messages in consumer.poll(timeout_ms=1000, max_records=500).values():
            for message in messages:
                alert = message.value
                if isinstance(alert, dict) and alert.get("session_id") == session_id:
                    got_alert = alert
        current = table.row(row_key)
        if current.get(b"cart:active_cart") == b"ABANDONED":
            got_hbase = current
        if got_alert is not None and got_hbase is not None:
            break
        time.sleep(1)

    if got_alert is None:
        raise TimeoutError("Spark did not publish the canary session to the Kafka alert topic within 180s")
    if got_hbase is None:
        raise TimeoutError("Spark did not write the canary customer to HBase within 180s")
    if str(got_alert.get("customer_id")) != str(customer_id):
        raise RuntimeError("Kafka alert customer_id did not match the canary customer")
    if got_hbase.get(b"cart:cart_value") != b"25.0":
        raise RuntimeError("HBase cart value mismatch: " + repr(got_hbase))
    if got_hbase.get(b"cart:active_cart") != b"ABANDONED":
        raise RuntimeError("HBase active_cart status mismatch: " + repr(got_hbase))

    # The cart is timestamped 30 seconds before test start and IDLE_SEC=30,
    # so the threshold is approximately test start. Measure real post-threshold latency.
    due_epoch = int(got_alert.get("last_event_ts", 0)) + 30
    detected_epoch = int(got_alert.get("detected_at", 0))
    alert_latency = detected_epoch - due_epoch
    if alert_latency < 0:
        raise RuntimeError(f"Alert detected before event-time threshold (latency={alert_latency}s); check host/container clocks")
    if alert_latency >= 60:
        raise TimeoutError(f"Canary alert missed the 60-second post-threshold SLA: {alert_latency}s")

    print("PASS_ALERT_SLA", json.dumps({"post_threshold_seconds": alert_latency, "target_seconds": 60}))
    print("PASS_KAFKA_ALERT", json.dumps({"topic": alert_topic, "session_id": session_id, "customer_id": customer_id, "alert_id": got_alert.get("alert_id")}))
    print("PASS_HBASE_WRITE", json.dumps({"table": "customer_360", "row_key": row_key.decode(), "cart_value": got_hbase.get(b"cart:cart_value", b"").decode(), "active_cart": got_hbase.get(b"cart:active_cart", b"").decode()}))
    print("PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION")
finally:
    if row_key is not None and conn is not None:
        try:
            conn.table("customer_360").delete(row_key)
            print("CLEANUP_CANARY_HBASE_ROW=done", row_key.decode())
        except Exception as exc:
            print("CLEANUP_CANARY_HBASE_ROW=failed", repr(exc), file=sys.stderr)
    if producer is not None:
        try: producer.close(timeout=5)
        except Exception: pass
    if consumer is not None:
        try: consumer.close()
        except Exception: pass
    if conn is not None:
        try: conn.close()
        except Exception: pass
'@
    $encodedProbe = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($probePython))
    $writeProbe = "echo $encodedProbe | base64 -d > '$RemoteProbe'"
    $null = Invoke-SparkBash $writeProbe
    $probeRun = & $DockerPath exec $SparkContainer bash -lc "PYTHONPATH=/tmp/python-packages:/opt/customer360/ingestion python3 '$RemoteProbe' '$InputTopic' '$AlertTopic' '$Tag'"
    $probeExit = $LASTEXITCODE
    $probeRun | ForEach-Object { Write-Host $_ }
    if ($probeExit -ne 0) { throw "Canary probe failed (exit $probeExit). Inspect $LogPath inside $SparkContainer; topics $InputTopic and $AlertTopic were left for diagnosis." }

    Write-Host '=== 6/6 final evidence ===' -ForegroundColor Cyan
    $evidence = Invoke-SparkBash "grep -E 'CUSTOMER360_STREAM_STARTED|KAFKA_ALERT_BATCH|HBASE_BATCH|HBASE_BATCH_RETRY|CUSTOMER360_STREAM_FATAL|QUERY_EXCEPTION' '$LogPath' | tail -n 25 || true"
    $evidence | ForEach-Object { Write-Host $_ }
    Write-Host 'LIVE INTEGRATION VERIFIED: Kafka input -> Spark stateful alert -> Kafka alert topic AND HBase customer_360; canary alert < 60s after threshold.' -ForegroundColor Green
    Write-Host "Test topics: $InputTopic / $AlertTopic"
    Write-Host "Test log: $LogPath inside $SparkContainer"
    Write-Host "Test checkpoint retained for diagnosis: $Checkpoint"
}
finally {
    if ($SparkStarted) {
        # Stop only the Java Spark driver whose command line names this canary script.
        $stopScript = @'
set +e
script='__REMOTE_STREAM__'
pids=$(ps -ef | awk -v s="$script" '$0 ~ /java/ && index($0, s) > 0 {print $2}')
for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
sleep 5
pids=$(ps -ef | awk -v s="$script" '$0 ~ /java/ && index($0, s) > 0 {print $2}')
for pid in $pids; do kill -KILL "$pid" 2>/dev/null || true; done
'@
        $stopScript = $stopScript.Replace('__REMOTE_STREAM__', $RemoteStream)
        try { $null = Invoke-SparkBash $stopScript } catch { Write-Warning "Could not stop canary Spark driver automatically. Check $LogPath inside $SparkContainer." }
    }
}
