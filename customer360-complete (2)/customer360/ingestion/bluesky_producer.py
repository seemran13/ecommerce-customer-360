#!/usr/bin/env python3
"""Bluesky Jetstream WebSocket -> Kafka brand_mentions with a simple lexicon score.

Set KAFKA_BOOTSTRAP to broker addresses that this process can reach. Messages are
flushed in small batches; reconnects don't kill the long-running process.
"""
import json
import os
import re
import time

import websocket
from producers_common import make_producer

URL = os.getenv("BLUESKY_JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post")
BRANDS = [b.strip().lower() for b in os.getenv("BRANDS", "amazon,flipkart,myntra,nykaa,zomato,swiggy").split(",") if b.strip()]
POS = set("love great awesome fast excellent happy good amazing recommend best reliable helpful".split())
NEG = set("hate terrible worst slow broken scam refund late bad angry disappointed fraud awful".split())
FLUSH_EVERY = max(1, int(os.getenv("BLUESKY_FLUSH_EVERY", "25")))


def sentiment(text):
    words = re.findall(r"[a-z']+", text.lower())
    score = sum(word in POS for word in words) - sum(word in NEG for word in words)
    return max(-1.0, min(1.0, score / 3.0))


def main():
    producer = make_producer()
    sent = 0
    try:
        while True:
            socket = None
            try:
                socket = websocket.create_connection(URL, timeout=30, enable_multithread=True)
                socket.settimeout(45)
                while True:
                    message = json.loads(socket.recv())
                    record = message.get("commit", {}).get("record", {})
                    text = record.get("text", "")
                    if not isinstance(text, str) or not text:
                        continue
                    matched = [brand for brand in BRANDS if brand in text.lower()]
                    if not matched:
                        continue
                    payload = {
                        "event_ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                        "brand": matched[0],
                        "text": text[:300],
                        "sentiment": sentiment(text),
                        "did": message.get("did"),
                    }
                    producer.send("brand_mentions", key=matched[0], value=payload)
                    sent += 1
                    if sent % FLUSH_EVERY == 0:
                        producer.flush(timeout=10)
                        print(f"BLUESKY_PUBLISHED count={sent}", flush=True)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                try:
                    producer.flush(timeout=5)
                except Exception:
                    pass
                print(f"BLUESKY_RECONNECT error={type(exc).__name__}: {exc}", flush=True)
                time.sleep(5)
            finally:
                if socket is not None:
                    try:
                        socket.close()
                    except Exception:
                        pass
    finally:
        try:
            producer.flush(timeout=10)
        finally:
            producer.close(timeout=10)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("BLUESKY_STOPPED", flush=True)
