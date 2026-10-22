"""Bluesky Jetstream (public WebSocket) -> brand_mentions.  Keeps only posts mentioning a brand keyword
and attaches a sentiment score.  Auto-reconnects; resumes from the last cursor (time_us) to avoid gaps.
    python -m producers.bluesky_producer
"""
import asyncio
import json
import logging
import re

from c360.config import settings as S
from c360.core.sentiment import label, score
from producers.cart_event import iso_now
from producers.common import make_producer, send

URL = ("wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post")
log = logging.getLogger("bluesky")


def match_brand(text: str, keywords=None):
    kws = keywords or S.brand_keywords
    t = (text or "").lower()
    for k in kws:
        if re.search(rf"\b{re.escape(k)}\b", t):
            return k
    return None


def to_mention(msg: dict, keywords=None):
    """Pure function: Jetstream message -> brand mention dict (or None)."""
    c = msg.get("commit") or {}
    if msg.get("kind") != "commit" or c.get("operation") != "create":
        return None
    text = (c.get("record") or {}).get("text", "")
    brand = match_brand(text, keywords)
    if not brand:
        return None
    s = score(text)
    return {"mention_id": f'{msg["did"]}/{c.get("rkey")}', "event_ts": iso_now(), "brand": brand,
            "text": text[:500], "sentiment_score": s, "sentiment": label(s), "source": "bluesky",
            "time_us": msg.get("time_us")}


async def run():
    import websockets
    p, cursor = make_producer("bluesky"), None
    while True:
        try:
            url = URL + (f"&cursor={cursor}" if cursor else "")
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("connected cursor=%s", cursor)
                async for raw in ws:
                    msg = json.loads(raw)
                    cursor = msg.get("time_us", cursor)
                    m = to_mention(msg)
                    if m:
                        send(p, S.topic_brand, m["brand"], m)
        except Exception as e:                                  # network blips are normal
            log.warning("ws error %s; reconnecting in 3s", e)
            p.flush(5)
            await asyncio.sleep(3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
