# What has actually been run vs. what still needs your environment

| Component | Status |
|---|---|
| Cart state machine, simulator, sentiment, identity map, Bluesky parser, replay scheduler, notifier de-dup, KPI formulas, feature leakage check, data generator, RAG parser/retrieval | **Executed: 24 unit tests pass** |
| `scripts/demo_local.py` end-to-end (generate -> detect -> KPIs -> ML -> RAG -> BI extracts) | **Executed** (numbers below, synthetic data) |
| All Python files | **Compile-checked** (`py_compile`); `docker-compose.yml` YAML-parsed |
| Spark jobs (batch, streaming, SCD2, Hive, HBase sync, reconcile) | **Written, compile-checked, NOT run** - no Spark/Kafka/network in the authoring sandbox |
| Kafka/MySQL/HDFS/HBase containers, Sqoop, Airflow DAG, AWS scripts | **Written, NOT run** |
| Power BI `.pbix` | **Not produced** (binary authored in Power BI Desktop) - spec/DAX/extracts provided |
| Failure drills | **Scripts written, NOT run** - they need the docker stack |
| Real Olist/Retailrocket data | **Not used** (not provided) - synthetic generator with the same shape + real-file adapter |

Expect to debug first-run issues in the un-run layers (package versions, container networking, Delta/Spark config). The
runbook orders the steps so each layer is validated before the next.

## Results from the synthetic-data demo (not claims about a real marketplace)
* Abandoned carts detected: 7,796 of 84,257 events; simulated detection latency p95 4.7 s (5 s trigger) - this is a model of the
  design, **not** a measurement of the Spark job; measure the real one with the notifier's `sla_s` column.
* Natural 7-day return rate after abandonment (no messages sent): 20.9%.
* Propensity model, temporal test split: **AUC 0.765 (target >= 0.75: met), F1 0.511 (target >= 0.65: NOT met)**. F1 depends
  heavily on the base rate (21.8% positives here); re-evaluate on the real data before drawing conclusions.
* K-Means: k=3, silhouette 0.51.
