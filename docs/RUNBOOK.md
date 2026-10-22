# Runbook - bring the platform up layer by layer

Prereqs: Docker, Java 11/17, Python 3.10+, `pip install -r requirements.txt`, (optional) Sqoop 1.4.7.

0. `make test && make demo`                      # logic works on your machine, no infra
1. `docker compose up -d && make topics hbase-init`   # Kafka x3, MySQL, HDFS, HBase  (wait ~30 s)
   Check: Kafka UI http://localhost:8080, HDFS http://localhost:9870, HBase http://localhost:16010
2. `make data && make load`                       # MySQL load + incremental landing (JDBC; or run ingestion/sqoop/import_incremental.sh)
   Re-run incremental: `python -m ingestion.load_mysql --apply-updates 50 && scripts/submit.sh spark/jobs/jdbc_incremental.py` -> only 50 rows land.
   (No MySQL? `python scripts/land_raw_local.py`)
3. `make batch`                                   # bronze -> silver (SCD2) -> gold -> ML -> Hive -> HBase
   Verify SCD2: change a customer's tier in MySQL, re-run import + silver, query `dim_customer_scd2` -> 2 rows, one `is_current`.
4. Streaming (one terminal each):
   `make stream-start` ; `python -m notifier.notify_consumer` ; `scripts/submit.sh spark/jobs/stream_bronze_silver.py`
   `scripts/submit.sh spark/jobs/stream_brand_sentiment.py` ; `python -m producers.bluesky_producer` ; `python -m producers.weather_producer`
   Then traffic: `make live` and/or `make replay` (100x).
   For a fast demo use `--idle 60` on the detector and `--speed 100` on the replay.
5. Acceptance:
   * Alert SLA: `sqlite3 data/notifier.sqlite "select max(sla_s), avg(sla_s) from sent"` (< 60)
   * HBase: `python -m hbase.benchmark_lookup` (p99 < 100 ms)
   * Drills + reconciliation: `make drills`
   * KPIs: `kpi/measure_kpis.py` (use a holdout group - see DESIGN_DECISIONS #6)
6. Cloud: set `C360_S3_BUCKET`, `python -m aws.s3_sync --src data/lake/gold --prefix gold`, `python -m aws.glue_catalog --role <arn>`,
   run `aws/athena_queries.sql`. Databricks: set `C360_AUTOLOADER=1` and lake roots to `s3a://`/`dbfs:/`, attach the job files as Jobs.
7. Airflow: copy `airflow/dags/customer360_master.py` into your DAGs folder, set `C360_HOME`.

Troubleshooting: if Spark workers cannot import `c360` on a cluster, ship it: `zip -r c360.zip c360 && --py-files c360.zip`.
If Kafka clients can't connect, check the EXTERNAL advertised ports (9092/9192/9292) match `KAFKA_BOOTSTRAP`.
