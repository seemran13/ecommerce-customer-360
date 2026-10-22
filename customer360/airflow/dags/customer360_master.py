"""Nightly Customer360 orchestration on a Hadoop/Sqoop/Hive/Spark cluster.

The local Compose stack is for Kafka/Spark/HBase development; this DAG requires
an external cluster with Sqoop, HDFS, Hive, YARN, HBase shell and the Spark Python
packages available to its workers. Offline model training/evaluation is provided
by run_local.sh / run_local.ps1 and is deliberately not faked with stale local data.
"""
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT = os.getenv("CUSTOMER360_PROJECT_ROOT", "/opt/customer360")


def notify(context, status="FAILED"):
    ti = context.get("task_instance") or context.get("ti")
    import requests
    url = os.getenv("SLACK_WEBHOOK")
    if url and ti:
        try:
            response = requests.post(
                url,
                json={"text": f"[C360] {status}: {ti.dag_id}.{ti.task_id} run {context.get('ds', 'unknown date')}"},
                timeout=5,
            )
            response.raise_for_status()
        except Exception as exc:
            # A notification outage must not hide the task's original state.
            print(f"Customer360 notification failed: {type(exc).__name__}: {exc}", flush=True)


def notify_ok(**ctx):
    notify(ctx, "SUCCESS")


def reconcile(**ctx):
    import subprocess
    subprocess.run(["python3", f"{PROJECT}/drills/reconcile.py", "--mode", "hive"], check=True, timeout=600)


def make_task(task_id, bash_command):
    return BashOperator(task_id=task_id, bash_command=bash_command, execution_timeout=timedelta(hours=2))


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify,
    "sla": timedelta(hours=2),
}

with DAG(
    "customer360_master",
    start_date=datetime(2026, 7, 1),
    schedule="0 1 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["capstone", "customer360"],
) as dag:
    sqoop = make_task("sqoop_incremental_import", f"bash {PROJECT}/ingestion/sqoop_import.sh {{{{ ds }}}}")
    weblogs = make_task("land_weblogs_and_tickets", f"bash {PROJECT}/ingestion/put_weblogs_hdfs.sh")
    cleanse = make_task(
        "spark_cleanse_weblogs",
        f"spark-submit --master yarn --deploy-mode cluster {PROJECT}/spark/batch_cleanse.py "
        "/landing/weblogs /landing/mysql/products /lake/silver/events /lake/quarantine/events/{{{{ ds }}}}",
    )
    hive_ddl = make_task("hive_ddl", f"hive -f {PROJECT}/hive/01_ddl_star_schema.sql")
    hive_load = make_task("hive_load", f"hive -f {PROJECT}/hive/02_load_star_schema.sql")
    hive_views = make_task("hive_analytics_views", f"hive -f {PROJECT}/hive/03_analytics_views.sql")
    aggregate = make_task(
        "spark_batch_aggregation",
        f"spark-submit --master yarn --deploy-mode cluster {PROJECT}/spark/batch_aggregate.py "
        "/lake/silver/events /lake/gold/customer_360",
    )
    hbase_schema = make_task("hbase_schema_provision", f"hbase shell -n < {PROJECT}/hbase/provision_schema.hbase")
    hbase_sync = make_task(
        "hbase_sync",
        f"spark-submit --master yarn --deploy-mode cluster {PROJECT}/spark/batch_hbase_sync.py "
        "/lake/gold/customer_360 --host ${{HBASE_THRIFT_HOST:-hbase}} --port ${{HBASE_THRIFT_PORT:-9090}}",
    )
    recon = PythonOperator(task_id="reconciliation_check", python_callable=reconcile, execution_timeout=timedelta(minutes=15))
    ok = PythonOperator(task_id="notify_success", python_callable=notify_ok, trigger_rule=TriggerRule.ALL_SUCCESS)

    sqoop >> hive_ddl
    [sqoop, weblogs] >> cleanse
    [hive_ddl, sqoop, cleanse] >> hive_load
    hive_load >> [hive_views, aggregate]
    [hbase_schema, aggregate] >> hbase_sync
    [hive_views, hbase_sync] >> recon >> ok
