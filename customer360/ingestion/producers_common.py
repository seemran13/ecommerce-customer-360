import json, os
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094")
def make_producer():
    from kafka import KafkaProducer   # pip install kafka-python
    return KafkaProducer(bootstrap_servers=BOOTSTRAP.split(","), acks="all", retries=10, linger_ms=20,
                         # kafka-python has no idempotent producer; acks=all + retries + consumer-side dedup (event_id) gives effectively-once
                         value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
                         key_serializer=lambda k: str(k).encode() if k is not None else None)
