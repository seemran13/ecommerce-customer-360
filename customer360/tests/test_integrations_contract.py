"""Static tests for orchestration and external integration contracts."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_hive_fact_order_load_deduplicates_incremental_sqoop_headers():
    hive = read("hive/02_load_star_schema.sql")
    sqoop = read("ingestion/sqoop_import.sh")
    assert "--incremental lastmodified" in sqoop
    assert "PARTITION BY order_id" in hive
    assert "FROM (SELECT order_id, customer_id, order_ts, total_amount, status, payment_type FROM latest_orders WHERE rn=1) o" in hive


def test_mysql_customer_delta_updates_escape_sql_literals_and_fail_fast():
    source = read("ingestion/load_mysql.sh")
    assert "set -euo pipefail" in source
    assert "def sql_quote(value):" in source
    assert '.replace("\\\\", "\\\\\\\\").replace("' in source
    assert "check=True" in source


def test_airflow_nightly_dependencies_run_ddl_before_load_and_schema_before_sync():
    dag = read("airflow/dags/customer360_master.py")
    assert "[hive_ddl, sqoop, cleanse] >> hive_load" in dag
    assert "[hbase_schema, aggregate] >> hbase_sync" in dag
    assert "[hive_views, hbase_sync] >> recon >> ok" in dag
    assert "spark/batch_hbase_sync.py" in dag


def test_bulk_sync_does_not_clear_live_cart_status_if_source_has_no_cart_value():
    source = read("hbase/sync_to_hbase.py")
    condition = 'if "active_cart" in row and row.get("active_cart") is not None'
    assert condition in source
    assert 'cells[b"cart:active_cart"]' in source


def test_acceptance_suite_includes_local_live_canary_and_lookup_latency():
    script = read("infra/verify_project.ps1")
    assert ".\\run_local.ps1" in script
    assert ".\\infra\\verify_live_integration.ps1" in script
    assert "hbase/lookup_benchmark.py 1000 200000" in script
    assert "CUSTOMER360_ACCEPTANCE_SUITE=PASS" in script


def test_live_sentiment_has_dedicated_one_minute_streaming_gold_writer():
    live_gold = read("databricks/06_live_brand_sentiment.py")
    batch_gold = read("databricks/03_gold_customer360.py")
    assert 'CHECKPOINT = os.getenv(' in live_gold and 'C360_SENTIMENT_CHECKPOINT' in live_gold
    assert 'F.expr("INTERVAL 1 MINUTE")' in live_gold
    assert 'trigger(processingTime="10 seconds")' in live_gold
    assert 'format("rate")' in live_gold
    assert 'gold_brand_sentiment_last_minute' in live_gold
    assert 'sentiment.write.mode("overwrite")' not in batch_gold


def test_distributed_hbase_sync_checks_written_count_against_gold_count():
    source = read("spark/batch_hbase_sync.py")
    assert "mapPartitions(" in source
    assert "if written != expected:" in source
    assert "HBase sync row-count mismatch" in source


def test_one_command_completion_workflow_checks_before_starting_production_stream():
    source = read("infra/complete_customer360.ps1")
    verify_pos = source.index(r"infra\verify_project.ps1")
    stream_pos = source.index(r"infra\run_streaming.ps1")
    assert verify_pos < stream_pos
    assert "install_project_update.ps1" in source
    assert "StartLiveFeeds" in source
    assert "CUSTOMER360_PYTHON_DEPS=PASS" in source
    assert "PASS_CUSTOMER360_PRODUCTION_STREAM_STARTED" in source
    assert "PASS_CUSTOMER360_FEED_LANDING_STARTED" in source
    live_canary = read("infra/verify_live_integration.ps1")
    assert "PASS_ALERT_SLA" in live_canary
    assert "CUSTOMER360_COMPLETE_WORKFLOW=PASS" in source
