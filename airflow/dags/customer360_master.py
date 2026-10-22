"""
Nightly master DAG:  ingest -> Spark batch -> Hive load -> HBase sync -> notify.
Each task runs only after the previous one succeeds (default trigger rule all_success).
Set Airflow Variable/env C360_HOME to the repo path.  Works with Airflow 2.7+ (BashOperator keeps it dependency-free).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

HOME = os.environ.get("C360_HOME", "/opt/customer360")
SUB = f"cd {HOME} && scripts/submit.sh"


def notify(context):
    # failure hook: wire to Slack/email; kept as a log line to stay dependency-free
    print(f"FAILED task={context['task_instance'].task_id} run={context['run_id']}")


default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5),
                "on_failure_callback": notify, "execution_timeout": timedelta(hours=2)}

with DAG("customer360_master", start_date=datetime(2026, 1, 1), schedule="0 1 * * *", catchup=False,
         default_args=default_args, max_active_runs=1, tags=["customer360"]) as dag:
    ingest = BashOperator(task_id="sqoop_incremental_import",
                          bash_command=f"cd {HOME} && ingestion/sqoop/import_incremental.sh || {SUB} spark/jobs/jdbc_incremental.py")
    cleanse = BashOperator(task_id="spark_batch_cleanse",
                           bash_command=f"{SUB} spark/jobs/batch_cleanse.py --weblogs {HOME}/data/source/weblogs")
    bronze = BashOperator(task_id="bronze_load", bash_command=f"{SUB} spark/jobs/bronze_batch.py")
    silver = BashOperator(task_id="silver_scd2", bash_command=f"{SUB} spark/jobs/medallion_silver.py")
    gold = BashOperator(task_id="gold_customer360", bash_command=f"{SUB} spark/jobs/medallion_gold.py")
    ml = BashOperator(task_id="ml_score", bash_command=f"cd {HOME} && python -m ml.score")
    gold2 = BashOperator(task_id="gold_with_scores", bash_command=f"{SUB} spark/jobs/medallion_gold.py")
    hive = BashOperator(task_id="hive_load", bash_command=f"{SUB} spark/jobs/hive_load.py")
    hbase = BashOperator(task_id="hbase_sync", bash_command=f"{SUB} spark/jobs/hbase_sync.py")
    s3 = BashOperator(task_id="s3_sync", bash_command=f"cd {HOME} && python -m aws.s3_sync --src data/lake/gold --prefix gold")
    done = BashOperator(task_id="notify_success", trigger_rule="all_success",
                        bash_command='echo "customer360 nightly run succeeded {{ ds }}"')
    ingest >> cleanse >> bronze >> silver >> gold >> ml >> gold2 >> [hive, hbase] >> s3 >> done
