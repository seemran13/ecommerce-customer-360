# Customer360 validation status

Validation revision: 2026-10-20. This report separates tested local results, evidence from the user's Docker Desktop host, and integrations that still need their external environment/credentials. Do not treat a code asset as a deployed service or a synthetic KPI as a business outcome.

## Checks completed

| Scope | Result | Evidence / limitation |
|---|---|---|
| Local analytics workflow, synthetic data at 2% scale | PASS | A fresh run `DATA_ROOT=/tmp/c360_release_acceptance_20261020_final bash run_local.sh 0.02` completed generation, cleansing/quarantine/dedup, abandonment analytics, propensity model, segmentation, scoring, RAG build/evaluation, BI CSV export, reconciliation and pytest. |
| Local automated tests in the generated run | PASS: 24/24 | The data-dependent tests run when `C360_DATA` points to the generated dataset; static/replay/integration-contract tests also ran. A plain `pytest -q tests` with no generated data skips five data-dependent tests by design; static dashboard tests run without data. |
| Local reconciliation | PASS: 9/9 | 72,018 raw rows; 713 malformed; 307 duplicate rows removed; 70,998 clean rows; 4,000/4,000 Gold customers; matching LTV and order/line totals. |
| Propensity model on 2% synthetic split | PASS against model thresholds | AUC 0.8316 (target >= 0.75), F1 0.7176 (target >= 0.65). Synthetic development data only. |
| Customer segmentation | PASS | Best K=4, silhouette 0.4819 on the synthetic run. |
| Offline dashboard preview | PASS | `bi/export_powerbi.py` now generates a self-contained `bi/dashboard.html` with the three business pages; no external network assets required. Not a Power BI `.pbix` file. |
| RAG retrieval evaluation | PASS | precision@5 = 1.000 (35/35 deterministic templated evaluation hits); this is not an independent human relevance study. |
| User-host live Kafka -> Spark -> Kafka alert + HBase canary | PASS | User-provided console output included `PASS_KAFKA_ALERT`, `PASS_HBASE_WRITE`, and `PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION`; the unique temporary HBase canary row was removed. This proved the real canary path once. |
| HBase Thrift read and existing data | PASS | User-host HappyBase request listed `customer_360`, read the `cart` family, and returned existing customer rows. |
| HBase provisioning and batch sync safeguards | Code complete; cluster validation pending | Provisioning adds `profile`, `metrics`, and `cart` without dropping customer rows. Both local and Spark-distributed sync preflight families and preserve real-time cart status when no source `active_cart` column exists. |
| Live brand/weather producers and feed landing | Code included; host run pending | Optional Compose `live-feeds` profile starts the Bluesky/Open-Meteo producers, sample clickstream replay and replay-aware watermark ticks. A separate Spark job lands brand and weather topics as partitioned Parquet. |
| Databricks Bronze/Silver/Gold/live feed and reconciliation assets | Code included; workspace run pending | Requires a Databricks workspace, S3 paths and a Kafka connector/runtime compatible with the workspace. Docker service names are not routable there by default. |
| Sqoop incremental import and Hive model | Code hardened; cluster run pending | Saved lastmodified jobs, saved append job for order items, stable dimension keys and order-header deduplication are included. Requires MySQL/HDFS/Sqoop metastore/Hive on a Hadoop cluster. |

## Still needs verification on the target environment

| Requirement | Status | Remaining action |
|---|---|---|
| Current verifier's canary alert latency <60 seconds after threshold | PENDING re-run of the latest package | The prior live canary passed alert + HBase output. The current verifier additionally prints `PASS_ALERT_SLA`; run this latest verifier and retain that marker. A single 30-second canary is not a sustained production 15-minute idle-SLA benchmark. |
| Normal `cart_events` production stream | PENDING | Start it with `infra/run_streaming.ps1`; verify it stays active and emits positive `KAFKA_ALERT_BATCH` and `HBASE_BATCH` counts against the normal topics/checkpoint `/checkpoints/abandonment_v2`. |
| Restart/checkpoint recovery drill | PENDING | Run `drills/spark_restart_drill.sh` only when temporary stream interruption is acceptable; verify offset/checkpoint resume and no missing alerts. |
| Three-broker kill drill | PENDING | Run `drills/broker_kill_drill.sh` only in a test window; it temporarily stops `kafka2` and should restore it via trap/cleanup. |
| HBase point lookup p99 <100 ms | PENDING host measurement | Run `hbase/lookup_benchmark.py` via `infra/verify_project.ps1`; retain observed p50/p95/p99. |
| Full synthetic dataset on this revision | PENDING | The checked-in full-scale metrics are historical references. This revision was freshly run at 2% scale; scale 1.0 still needs a resource-appropriate run. |
| Airflow DAG execution | PENDING | Airflow remains an opt-in Compose profile; the DAG needs the external Hive/Sqoop/HDFS/YARN/HBase stack. |
| AWS S3/Glue/Athena; Databricks deployment; Power BI `.pbix`; production RAG source integration | PENDING deployment | SQL, notebook, documentation, and CSV-export assets are included; actual cloud resources/workspaces/Power BI deployment are not bundled. |
| Recovery rate >=18% and LTV lift >=12% | NOT MEASURED | Requires real or controlled A/B evidence. Do not present synthetic development metrics as business lift. |

## One-command completion helper

`infra/complete_customer360.ps1` is included to back up and update code in the existing project folder, run the local analytics suite + isolated live canary + HBase lookup benchmark, then start the normal cart stream only if the acceptance checks pass. Add `-StartLiveFeeds` to enable the optional Bluesky/Open-Meteo/sample replay producers and separate feed landing stream. Airflow stays off to conserve RAM and should be started only for DAG acceptance. The helper requires Windows PowerShell and a reachable Docker Desktop engine; its PowerShell orchestration has contract tests but must still be run on the target host.

```powershell
$root = "$env:USERPROFILE\Downloads\customer360-complete\customer360"
powershell -ExecutionPolicy Bypass -File "$root\infra\complete_customer360.ps1" -Scale 0.02 -TargetRoot "$env:USERPROFILE\Downloads\customer360\customer360" -StartLiveFeeds
```

The live canary from the user's Docker host passed once, including a Kafka alert and HBase upsert. The helper's current revision adds a post-threshold `<60s` canary SLA marker plus the HBase lookup p99 benchmark before starting the production stream. Do not call those two added acceptance criteria passed until the helper completes on the host and prints their PASS markers.

## Safe next verification on Windows

1. Extract this ZIP into a new folder and keep the original project as backup.
2. Apply files into the original project with `infra/install_project_update.ps1 -TargetRoot <existing-project-root>`; the installer backs up replaced source files and does not delete data/checkpoints.
3. Run `infra/verify_project.ps1 -Scale 0.02` from PowerShell. It stops on failed local, live-canary, or HBase lookup checks. If an existing Customer360 stream is already active, stop/inspect it first to avoid duplicate drivers.
4. Run feed landing separately with `infra/run_feeds.ps1` after starting the optional producer profile. Run broker-kill and restart drills only in a test window.

Never delete `/checkpoints`, the Compose checkpoint volume, Kafka data, HBase data, or WAL files as part of routine troubleshooting.

## Final local retest after dashboard and resource-default update

- `python -m pytest -q tests` without a generated data root: static/clock/dashboard tests pass; data-dependent tests skip by design.
- Full end-to-end local run at `0.02` scale on this exact source revision passed 24/24 tests. It generated 72,018 raw event lines, quarantined 713 malformed lines, removed 307 duplicates, reconciled 4,000/4,000 customer profiles, built the ML/RAG artifacts, and generated the offline dashboard. A 100% scale rerun remains pending.
- The production streaming launcher is now set to `local[2]`, 4 shuffle partitions, and `maxOffsetsPerTrigger=3000` by default to reduce CPU/memory pressure on this project's Docker Desktop host. Tune upward only after measurement.
- A full `1.0` synthetic-data run was attempted in an earlier constrained validation window but did not finish within five minutes; it is not marked as a pass. The prior full-scale metrics in `results/` are reference results from an earlier run.
