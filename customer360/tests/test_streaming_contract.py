"""Static contract tests for live streaming paths that need the Docker/Spark runtime."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_abandonment_stream_uses_replay_safe_watermark_and_one_stateful_query():
    source = read("spark/streaming_abandonment.py")
    assert 'INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "cart_events")' in source
    assert 'CKPT = os.getenv("CKPT", "/checkpoints/abandonment_v2")' in source
    assert 'F.col("event_type") != F.lit("heartbeat")' in source
    assert 'withWatermark("event_ts", WATERMARK_DELAY)' in source
    assert source.count("applyInPandasWithState(") == 1
    assert "alerts.writeStream.foreachBatch(alert_sinks)" in source
    assert "approx_count_distinct" in source
    assert "HBASE_BATCH_RETRY" in source


def test_live_canary_uses_unique_topics_fresh_checkpoint_and_checks_sla():
    source = read("infra/verify_live_integration.ps1")
    assert '"c360_it_events_$Tag"' in source
    assert '"c360_it_alerts_$Tag"' in source
    assert '"/checkpoints/c360_live_it_$Tag"' in source
    assert "PASS_ALERT_SLA" in source
    assert "alert_latency >= 60" in source
    assert "PASS_LIVE_KAFKA_SPARK_HBASE_INTEGRATION" in source


def test_hbase_bulk_sync_families_are_provisioned_and_preflighted():
    provision = read("hbase/provision_schema.hbase")
    sync = read("hbase/sync_to_hbase.py")
    for family in ("profile", "metrics", "cart"):
        assert family in provision
        assert family in sync
    assert "missing families" in sync.lower()
    assert "run bash hbase/provision_schema.sh" in sync
    assert "conn.close()" in sync


def test_sqoop_incremental_jobs_are_saved_and_do_not_swallow_failures():
    source = read("ingestion/sqoop_import.sh")
    assert "sqoop job --create \"$job\"" in source
    assert "--incremental lastmodified" in source
    assert "sqoop job --create inc_order_items" in source
    assert "--incremental append" in source
    assert "|| true" not in source


def test_live_public_feeds_have_a_landing_stream_and_databricks_consumer():
    stream = read("spark/streaming_feeds.py")
    databricks = read("databricks/05_live_feeds_kafka.py")
    compose = read("infra/docker-compose.yml")
    assert '"brand_mentions"' in stream and '"weather"' in stream
    assert "customer360_feeds" in stream or "CUSTOMER360_FEEDS_STARTED" in stream
    assert 'subscribe", topic' in databricks
    assert '"brand_mentions"' in databricks
    assert 'toTable(f"c360.{table_name}")' in databricks
    assert 'profiles: ["live-feeds"]' in compose
    assert "clickstream-replay" in compose and "watermark-tick-producer" in compose


def test_databricks_quarantine_handles_null_event_type_and_hive_keys_are_stable():
    silver = read("databricks/02_silver_scd2.py")
    hive_load = read("hive/02_load_star_schema.sql")
    assert "~F.coalesce(valid_condition, F.lit(False))" in silver
    assert "missing_event_type" in silver
    assert "CAST(product_id AS BIGINT)" in hive_load
    assert "CAST(customer_id AS BIGINT)" in hive_load
