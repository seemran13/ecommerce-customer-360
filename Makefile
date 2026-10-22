PY=python3
export PYTHONPATH:=$(PWD)

.PHONY: help data test demo up topics load batch stream-start replay live ml rag hbase-init drills kpi zip
help:            ## list targets
	@grep -E '^[a-z-]+:.*##' Makefile | sed 's/:.*##/ -/'
data:            ## generate synthetic source data
	$(PY) -m c360.datagen.generate --out data/source
test:            ## unit tests (no infrastructure needed)
	$(PY) -m pytest -q tests
demo:            ## infrastructure-free end-to-end demo
	$(PY) scripts/demo_local.py
up:              ## start Kafka x3, MySQL, HDFS, HBase
	docker compose up -d
topics:          ## create Kafka topics (RF3, min ISR 2)
	kafka/create_topics.sh
hbase-init:      ## create HBase table
	$(PY) hbase/create_table.py
load:            ## load MySQL, then land raw (Sqoop if installed, else Spark JDBC)
	$(PY) -m ingestion.load_mysql
	scripts/submit.sh spark/jobs/jdbc_incremental.py
batch:           ## bronze -> silver -> gold -> Hive -> HBase (what Airflow runs nightly)
	scripts/submit.sh spark/jobs/batch_cleanse.py
	scripts/submit.sh spark/jobs/bronze_batch.py
	scripts/submit.sh spark/jobs/medallion_silver.py
	scripts/submit.sh spark/jobs/medallion_gold.py
	$(PY) -m ml.train && $(PY) -m ml.score
	scripts/submit.sh spark/jobs/medallion_gold.py
	scripts/submit.sh spark/jobs/hive_load.py
	scripts/submit.sh spark/jobs/hbase_sync.py
stream-start:    ## run the detector (foreground; use a separate terminal per job)
	scripts/submit.sh spark/jobs/stream_cart_abandonment.py
replay:          ## replay history at 100x
	$(PY) -m producers.replay_producer --speed 100 --loop
live:            ## live clickstream simulator
	$(PY) -m producers.live_cart_producer --rate 3
ml:              ## train + score
	$(PY) -m ml.train && $(PY) -m ml.score
rag:             ## build index and ask a question
	$(PY) -m rag.chatbot --build && $(PY) -m rag.chatbot -q "What are customers saying about delivery?"
drills:          ## failure drills (need the docker stack + running streaming job)
	drills/broker_kill_drill.sh
	drills/spark_restart_drill.sh
	scripts/submit.sh spark/jobs/reconcile.py
zip:
	cd .. && zip -qr customer360.zip customer360 -x 'customer360/data/*' '*/__pycache__/*'
