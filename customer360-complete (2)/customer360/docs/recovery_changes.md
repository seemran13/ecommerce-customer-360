# Recovery revision notes

This revision starts from the uploaded Customer360 project; it does not replace the architecture with a separate demo application.

## Problems addressed in code

1. **Watermark poisoning during replay.** Old `event_type=heartbeat` records used wall-clock timestamps even though clickstream events were historical (July-October 2026). Spark now filters those legacy heartbeat messages before the watermark. The updated heartbeat producer emits `watermark_tick` records tied to a replay clock so that ticks do not jump ahead of queued historical data.
2. **Repeated stateful work.** The previous stream started one `applyInPandasWithState` query for Kafka alerts and another for HBase. The revised code computes the stateful abandonment output once and fans the micro-batch out to both sinks.
3. **Streaming aggregation support/performance.** Exact `countDistinct` was replaced with `approx_count_distinct`, and local stream shuffle partitions / offsets per trigger have smaller defaults.
4. **Fragile HBase writes.** Thrift writes now use bounded retries, guaranteed connection cleanup, latest-session-per-customer selection, and batch-count logging.
5. **Silent query termination.** Query failures and statuses are emitted to the Spark log before the process exits.
6. **Notifier restarts.** Alert IDs are stored in SQLite, and Kafka offsets are committed per processed record rather than by committing all positions returned in a poll.
7. **Checkpoint safety.** The revised stream uses `/checkpoints/abandonment_v2` by default. The previous `/checkpoints/abandonment` tree is left in place because the new alert sink topology has different checkpoint requirements.
8. **Local-run repeatability.** The local runner writes to a fresh timestamped dataset/output folder per run, preserving existing full-size data and tracked reference metrics.

## Validation performed for this revision

- Python compilation passed for the changed streaming, producer, notifier, and test files.
- Shell syntax passed for `run_local.sh`, `infra/run_streaming.sh`, and `drills/spark_restart_drill.sh`.
- The local pipeline ran on a freshly generated 2% synthetic dataset. It generated 72,018 clickstream lines, quarantined 713 malformed lines, removed 307 duplicate redeliveries, reached RAG precision@5 = 1.000, reconciled all nine count/money/customer checks, and passed 24/24 tests when the generated dataset was supplied via `C360_DATA`.
- The user subsequently ran the live Kafka/Spark/HBase canary on Docker Desktop and supplied output with `PASS_KAFKA_ALERT`, `PASS_HBASE_WRITE`, and `PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION`. The latest revision of the verifier adds an explicit post-threshold latency assertion that still needs to be rerun on the user's host. The full production SLA/business KPIs remain unmeasured.

10. **Offline dashboard.** `bi/build_dashboard.py` assembles the same three BI pages into a self-contained HTML file with embedded CSV-derived data and no external JS/CSS dependencies; it is created automatically by `bi/export_powerbi.py`. It is a convenient browser preview, not a replacement `.pbix` file.

9. **Live integration canary.** `infra/verify_live_integration.ps1` provides an isolated end-to-end Kafka -> Spark -> Kafka alert + HBase write test on Docker Desktop. The canary uses unique Kafka topics and a new checkpoint, provisions missing HBase families non-destructively, and deletes only its own temporary HBase customer row. The user-host canary has passed the integration path once; the stricter current-script SLA marker still needs its own run.
