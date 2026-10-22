# Live Kafka -> Spark -> HBase integration check

The current authoring environment has no Docker CLI/daemon, so the live service-to-service test must run against the Docker Desktop stack on the project host. This is an executable canary, not a TCP-only readiness check.

## Run it

1. Extract the updated `customer360-fixed.zip` onto your existing Customer360 project folder, keeping its directory layout. Do not remove Docker volumes or the existing `infra_ckpt`/`ckpt` volume.
2. Start Docker Desktop and start `hbase`, `kafka1`, `kafka2`, `kafka3`, and `infra-spark-1`. Airflow is not needed for the test. Ensure main topics `cart_events` and `cart_alerts` already exist.
3. Provision the full HBase customer schema before the nightly bulk sync. This is non-destructive: it creates `customer_360` if absent and adds/normalizes `profile`, `metrics`, and `cart` families without dropping customer rows. On Windows PowerShell run ` .\infra\provision_hbase.ps1`; on Linux/WSL run `bash hbase/provision_schema.sh`.
4. Open PowerShell at the `customer360` project root and run:

   ```powershell
   .\infra\verify_live_integration.ps1
   ```

The script checks container status and network connectivity, non-destructively provisions the HBase families, waits for an actual Thrift RPC (table list, family check and row read), creates uniquely named temporary Kafka topics, copies the updated streaming script into the Spark container, and starts a test query on a separate checkpoint with a short event-time timeout. It produces an abandoned-cart canary event plus watermark ticks. A pass requires both (1) the matching alert to be observed on the canary Kafka alert topic and (2) the matching `customer_360` HBase row with the expected cart value. The Python probe removes only its own randomly selected canary HBase row in its `finally` block. Existing customer rows and checkpoints are not deleted or reused. The test topic names, log path, and checkpoint path are printed for diagnosis; test topics/checkpoint files are left in place intentionally. The test Spark driver is stopped automatically.

A full pass prints `PASS_ALERT_SLA` (post-threshold latency for the 30-second canary threshold) and `PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION`. This is a canary test of the post-threshold timing logic, not a sustained 15-minute production-load SLA benchmark. If it fails, share the console output, especially the `Test log` path and exception. Do not run it while another Customer360 Spark streaming driver is active; the script detects that condition and exits instead of starting a duplicate.

## What this test proves

It proves that the deployed Kafka brokers accept input events, the updated Spark Structured Streaming job parses them and fires an event-time abandonment alert, the alert is published to Kafka, and the HBase Thrift sink writes the corresponding customer row. It does not measure the business recovery rate or validate full-dataset accuracy. It asserts a single canary alert arrives under 60 seconds after its configured 30-second idle threshold; a separate sustained 15-minute production-load test is still required to certify the advertised production SLA.
