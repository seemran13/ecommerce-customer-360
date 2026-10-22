# Problem Statement #01 - E-Commerce Customer 360

Customer 360 unifies orders, customers/products, clickstream, and support tickets; computes cart-abandonment analytics; exposes customer-level data in HBase; and supports RAG over tickets/reviews.

## Current validation status

- **Local end-to-end analytics:** re-run and passed at 2% synthetic scale on 2026-10-20. Data generation, malformed-line quarantine/deduplication, abandonment analytics, propensity model, segmentation, scoring, RAG evaluation, BI CSV export, and reconciliation all completed. The generated run passed 24/24 tests and 9/9 local reconciliation checks.
- **User-host live Kafka/Spark/Kafka-alert/HBase canary:** a real isolated canary passed on the target Docker Desktop stack, including a Kafka alert and the matching `customer_360` HBase write. The packaged verifier now adds an explicit post-threshold `<60s` assertion; rerun the current verifier to confirm this stricter assertion for the current source revision.
- **Full-scale reference results:** retained under `results/` and shown in the supplied dashboard PDF. They are synthetic-data results, not live business outcomes or a rerun of the current revision at 100% scale.
- The dashboard screenshots are the design/metrics reference. This archive includes the Power BI CSV extracts/specification plus a self-contained offline browser dashboard at `<run-data>/bi/dashboard.html`; it does not include a Power BI `.pbix` file.

## Quick run without Docker

This is the fastest way to get the main analytics project running and verify the data path locally.

### Windows PowerShell

```powershell
python -m pip install -r requirements-local.txt
.\run_local.ps1 -Scale 0.02
```

Each run creates a new timestamped output folder (for example, `data_small_run_20261020_114500/`), with `ml_artifacts/` and `rag_index/` inside it. The runner does not delete or overwrite your existing `data/` folder or the checked-in full-scale reference metrics. Use `-Scale 1.0` for a full synthetic run; it is substantially larger and takes longer.

### Linux / WSL / Git Bash

```bash
python3 -m pip install -r requirements-local.txt
bash run_local.sh 0.02
```

Expected local outputs include:

- `<generated-data-dir>/clean/cleanse_summary.json`
- `<generated-data-dir>/abandonment_report.json`
- `<generated-data-dir>/gold/customer_360.csv` and `customer_360_scored.csv`
- `<generated-data-dir>/bi/page1_sales_funnel.csv`, `page2_cohort_retention.csv`, `page3_abandoned_carts.csv`, `dim_customer_segments.csv`, and the self-contained `dashboard.html` preview
- reconciliation output and pytest summary in the terminal.

## Docker streaming setup

The live stack is optional for the local analytics path. It is required only when demonstrating the actual Kafka -> Spark Structured Streaming -> Kafka/HBase flow.

In a fresh Windows PowerShell window, define the Docker CLI path once:

```powershell
$D = "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe"
if (-not (Test-Path $D)) { $D = (Get-Command docker.exe -ErrorAction Stop).Source }
```

1. **Apply this source revision to the folder mounted into the existing Compose stack**, then start the services and create topics. If you extracted the ZIP into `customer360-fixed`, run this from that extracted project's root in Windows PowerShell:

   ```powershell
   .\infra\install_project_update.ps1 -TargetRoot "$env:USERPROFILE\Downloads\customer360\customer360"
   ```

   The installer backs up replaced code files into a timestamped sibling folder and does not delete local data, Docker volumes, or checkpoints. Adjust `-TargetRoot` if your original project is in another location.

   Start the normal data services (Airflow is opt-in to leave RAM for HBase/Spark on Docker Desktop):

   ```powershell
   & $D compose -f .\infra\docker-compose.yml up -d
   & $D exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic cart_events --partitions 12 --replication-factor 3 --config min.insync.replicas=2
   & $D exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic cart_alerts --partitions 6 --replication-factor 3 --config min.insync.replicas=2
   ```

   Airflow is optional. Start it only when running orchestration tasks with:

   ```powershell
   & $D compose --profile orchestration -f .\infra\docker-compose.yml up -d airflow
   ```

2. Provision the customer table schema before nightly profile/metrics sync. Provisioning is idempotent and non-destructive: it adds the `profile`, `metrics`, and `cart` families without dropping rows.

   ```powershell
   .\infra\provision_hbase.ps1
   ```

   Then start the upgraded streaming job. It uses `/checkpoints/abandonment_v2` by default so it **does not overwrite the previous checkpoint tree**. The stream log is `/tmp/streaming_abandonment.log` inside the Spark container.

   Windows PowerShell:

   ```powershell
   .\infra\run_streaming.ps1
   & $D exec infra-spark-1 bash -lc "grep -E 'CUSTOMER360_STREAM_STARTED|KAFKA_ALERT_BATCH|HBASE_BATCH|CUSTOMER360_STREAM_FATAL' /tmp/streaming_abandonment.log | tail -n 30"
   ```

   Linux / WSL / Git Bash:

   ```bash
   bash infra/run_streaming.sh
   docker exec infra-spark-1 bash -lc "grep -E 'CUSTOMER360_STREAM_STARTED|KAFKA_ALERT_BATCH|HBASE_BATCH|CUSTOMER360_STREAM_FATAL' /tmp/streaming_abandonment.log | tail -n 30"
   ```

3. Start a historical clickstream replay **without `--loop`** first. Run producers inside the Compose Spark container so they can reach `kafka1:9092` and so the replay-clock file is shared with the heartbeat producer:

   Windows PowerShell:

   ```powershell
   & $D exec -d infra-spark-1 bash -lc 'cd /opt/customer360/ingestion && export PYTHONPATH=/tmp/python-packages:/opt/customer360/ingestion && export KAFKA_BOOTSTRAP=kafka1:9092,kafka2:9092,kafka3:9092 && python3 replay_producer.py --src /opt/customer360/data/weblogs --speedup 100 > /tmp/replay_producer.log 2>&1'
   ```

   If you are using only the packaged sample dataset, change `--src` to `/opt/customer360/sample_data/weblogs`. Start the replay **before** the heartbeat producer, and don't use `--loop` unless you deliberately want a continuously rebased replay.

4. Optional: start safe watermark ticks. The updated heartbeat producer follows the latest replay event timestamp while replay is active and won't jump the watermark ahead of historic events:

   Windows PowerShell:

   ```powershell
   & $D exec -d infra-spark-1 bash -lc 'cd /opt/customer360/ingestion && export PYTHONPATH=/tmp/python-packages:/opt/customer360/ingestion && export KAFKA_BOOTSTRAP=kafka1:9092,kafka2:9092,kafka3:9092 && python3 heartbeat_producer.py > /tmp/heartbeat_producer.log 2>&1'
   ```

   **Do not set `C360_HEARTBEAT_WALL_CLOCK=1` while a historical Kafka backlog is still being replayed.** It intentionally advances ticks to current UTC time and is only appropriate for a live-only feed or after the replay backlog has completely drained. Legacy `event_type=heartbeat` messages are ignored before Spark's watermark; the updated producer emits `watermark_tick` messages.

5. Verify the live result. Look for `KAFKA_ALERT_BATCH ... rows=N` and `HBASE_BATCH ... alerts=N customers=N` with positive counts. Then read the HBase table and verify new or updated customer keys. A running process or an old HBase row alone is not an end-to-end pass. Avoid running `wsl --shutdown` while the live pipeline is active: it forcibly stops containers and invalidates HBase ZooKeeper sessions, so services must be restarted before validation can continue.

### Streaming changes in this revision

- One stateful alert computation fans out to Kafka and HBase, rather than computing the same stateful logic in two separate streaming queries.
- `approx_count_distinct` replaces exact `countDistinct` for the streaming window aggregation.
- Legacy wall-clock heartbeat events are filtered before the event-time watermark; updated `watermark_tick` messages use replay-aware timestamps.
- Default stream checkpoint root is `abandonment_v2`; the previous `/checkpoints/abandonment` data is preserved.
- Production streaming defaults are tuned for memory-limited Docker Desktop: `local[2]`, 4 shuffle partitions, and a 3,000-offset cap per micro-batch. Override `MAX_OFFSETS_PER_TRIGGER` and `SPARK_STREAMING_SHUFFLE_PARTITIONS` for a larger host after measuring throughput.
- HBase Thrift writes retry transient failures and log row counts; query exceptions are printed rather than disappearing behind generic executor-heartbeat messages.

## Optional live brand/weather feeds

To start the external producers (Bluesky Jetstream, Open-Meteo, sample clickstream replay, and replay-aware watermark ticks), use the opt-in Compose profile. These services use the Kafka internal Docker DNS names and the checked-in small sample clickstream dataset:

```powershell
& $D compose -f .\infra\docker-compose.yml --profile live-feeds up -d
.\infra\run_feeds.ps1
& $D exec infra-spark-1 bash -lc "grep -E 'CUSTOMER360_FEEDS_STARTED|CUSTOMER360_FEED_QUERY_STOPPED|CUSTOMER360_FEEDS_FATAL' /tmp/streaming_feeds.log | tail -n 20"
```

The feed landing job writes partitioned Parquet to `data/lake/feeds/brand_mentions` and `data/lake/feeds/weather`, with its own `/checkpoints/customer360_feeds` checkpoint. To land into Databricks Delta tables, run `databricks/05_live_feeds_kafka.py` in a workspace with a Spark-Kafka connector matching that runtime and `KAFKA_BOOTSTRAP_SERVERS` set to externally routable brokers. Then run `databricks/06_live_brand_sentiment.py` as a continuous Databricks job; a lightweight `rate` trigger refreshes `c360.gold_brand_sentiment_last_minute` from the trailing 60-second Delta window every 10 seconds. Configure the BI visual to query that table on a one-minute refresh cadence. Compose service names such as `kafka1` are not resolvable from Databricks by default.

## Full-scale reference results

`results/` contains the previous full synthetic run: 3,602,945 raw clickstream lines, 3,549,093 clean rows after malformed-line quarantine/deduplication, 200,000 customers, propensity AUC 0.834 / F1 0.755, and 9/9 reconciliation checks passing. The dashboard PDF includes these values and explicitly labels alert-delay figures as modeled rather than live measurements.

## Important limitations

- Recovery rate (+18%), lifetime-value lift (+12%), HBase lookup latency (<100 ms), and real alert SLA (<60 seconds) require live measurement/A-B testing; the dashboard reference marks some of these as not measured.
- The local runner by itself validates business analytics and reconciliation but does not prove Kafka/Spark/HBase behavior; use the live canary and current SLA assertion for that.
- Kafka sinks are at-least-once. Alert IDs are deterministic, HBase writes are idempotent upserts, and the notifier now persists processed alert IDs in a SQLite database. A crash between an external email/SMS side effect and the local SQLite commit can still cause duplicate delivery; a production sender should also use provider-side idempotency where available.
- Databricks/AWS/Sqoop/Hive/Airflow modules are included as integration assets; the local runner doesn't execute those external services.
## One-command acceptance suite

In Windows PowerShell from the extracted project root, use the suite to run local analytics, the live canary, and the HBase lookup-latency acceptance check:

```powershell
.\infra\verify_project.ps1 -Scale 0.02
```

This script stops on the first failed acceptance check. It can stop briefly at the HBase benchmark, and the canary intentionally creates uniquely named test topics/checkpoint paths for diagnosis. Do not run the Spark restart drill or broker-kill drill unless a brief service disruption is acceptable.


## One-command complete and start workflow

For the recommended Windows workflow, extract this archive to `Downloads\customer360-complete` while keeping the original project folder. With Docker Desktop running and no existing Customer360 streaming driver active, run from PowerShell:

```powershell
$root = "$env:USERPROFILE\Downloads\customer360-complete\customer360"
powershell -ExecutionPolicy Bypass -File "$root\infra\complete_customer360.ps1" -Scale 0.02 -TargetRoot "$env:USERPROFILE\Downloads\customer360\customer360" -StartLiveFeeds
```

This helper first backs up and updates source files in the original Docker-mounted project folder (it does not delete data/checkpoints), runs the local analytics/test suite, the isolated Kafka -> Spark -> Kafka alert + HBase canary, and the HBase lookup latency benchmark. Only if those checks pass does it start the normal cart stream. `-StartLiveFeeds` additionally enables the optional Bluesky/weather/sample-replay feed profile and the feed landing stream. Airflow remains stopped to conserve memory; run it separately for DAG acceptance. This host workflow is included and contract-tested in source, but must be run on your Windows Docker Desktop host to verify the full current revision.

## Verify live Kafka -> Spark -> HBase integration

After replacing your working project files with this version and confirming Docker Desktop is running, open PowerShell in the project root and run:

```powershell
.\infra\verify_live_integration.ps1
```

The canary verification checks Docker containers and service connectivity, creates isolated timestamped Kafka test topics, launches a short-lived Spark streaming query using a separate checkpoint, publishes a test abandoned cart plus event-time ticks, and requires the resulting alert to appear in Kafka and the corresponding canary customer row to appear in HBase. The script then deletes only its unique canary HBase row and stops its test Spark driver. It does not delete or reuse the existing checkpoints or production Kafka topics. The timestamped test topics and checkpoint are intentionally retained to support diagnosis.

A successful run must print `PASS_ALERT_SLA` and `PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION`. If the script fails, share the printed exception and the `Test log` path; the script is deliberately not considered a pass on TCP checks alone.


## Final validation record

See `docs/final_validation_status.md` for the precise status of local analytics, live canary integration, SLA checks, and infrastructure-dependent capstone modules. The isolated live integration test verifies a real Kafka input -> Spark abandonment alert -> Kafka alert topic and HBase `customer_360` write. It does not by itself prove the normal production-topic backlog has drained or the full capstone is SLA-certified.
