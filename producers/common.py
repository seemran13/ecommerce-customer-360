"""Durable Kafka producer settings shared by all producers.
acks=all + idempotence + RF3/min.insync.replicas=2  =>  surviving one broker failure loses nothing."""
import json
import logging

from c360.config import settings as S

log = logging.getLogger("producer")


def make_producer(client_id: str):
    from confluent_kafka import Producer  # imported lazily so unit tests need no broker libs
    return Producer({
        "bootstrap.servers": S.kafka_bootstrap, "client.id": client_id,
        "acks": "all", "enable.idempotence": True, "max.in.flight.requests.per.connection": 5,
        "retries": 2147483647, "delivery.timeout.ms": 120000, "linger.ms": 20,
        "compression.type": "lz4",
    })


def delivery_report(err, msg):
    if err is not None:
        log.error("delivery failed topic=%s key=%s err=%s", msg.topic(), msg.key(), err)


def send(producer, topic: str, key: str, value: dict):
    while True:
        try:
            producer.produce(topic, key=key, value=json.dumps(value, default=str), on_delivery=delivery_report)
            producer.poll(0)
            return
        except BufferError:       # local queue full -> back-pressure instead of dropping
            producer.poll(0.5)
