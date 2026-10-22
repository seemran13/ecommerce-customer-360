# E-Commerce Customer 360 - Data Engineering Capstone (Problem Statement #01)

Unifies transactions (MySQL), clickstream (Kafka, live + 100x replay) and support tickets/reviews into a Customer 360:
real-time abandoned-cart detection (<60 s), HBase profiles (<100 ms), medallion lakehouse with SCD2, Hive star schema,
ML segmentation + propensity, RAG chatbot, Power BI spec, Airflow orchestration, and failure drills.

```
MySQL --Sqoop/JDBC--> HDFS raw --Spark Core cleanse--> Bronze -> Silver(SCD2) -> Gold --> Hive star | HBase | S3/Glue/Athena | Power BI
Live sim / Replay 100x / Bluesky / Open-Meteo --> Kafka(3 brokers) --Structured Streaming--> abandoned-cart state machine
        --> Delta gold/abandoned_carts + HBase c:* + Kafka cart_alerts --> notifier (email/SMS, dedup, SLA log)
Gold + events --> K-Means / Random Forest --> scores --> customer_360;   tickets+reviews --> embeddings --> RAG chatbot
Airflow nightly: import -> cleanse -> bronze -> silver -> gold -> ML -> hive/hbase -> s3 -> notify
```

Start here: `make test && make demo` (no infrastructure), then `docs/RUNBOOK.md`.
**Read `docs/VERIFICATION_STATUS.md` first** - it states exactly what was executed and what still needs your environment.
`docs/DESIGN_DECISIONS.md` resolves the eight open items from your design document.

| Folder | Contents |
|---|---|
| `c360/` | config, pure-Python cart state machine, simulator, sentiment, identity map, synthetic data generator |
| `producers/` | live simulator, 100x replay, Bluesky Jetstream, Open-Meteo (all `acks=all`, idempotent) |
| `spark/jobs/` | batch cleanse (accumulator + broadcast join), bronze, silver SCD2, gold, streaming detector, brand sentiment, Hive, HBase sync, reconcile |
| `ingestion/`, `sql/` | MySQL schema + loader, Sqoop incremental script |
| `hbase/`, `hive/`, `aws/` | table creation + latency benchmark, star-schema DDL, S3/Glue/Athena |
| `airflow/dags/` | nightly master DAG |
| `ml/`, `rag/`, `kpi/`, `bi/` | models, chatbot, KPI formulas, Power BI spec/DAX/extracts |
| `drills/` | broker-kill, Spark-restart, scenario producer + verifiers |
| `tests/` | 24 unit tests |
