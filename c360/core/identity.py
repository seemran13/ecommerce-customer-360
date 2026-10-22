"""Identity mapping between Retailrocket (visitorid/itemid ints) and the Olist-style master
data (hex customer_id/product_id). Deterministic, so replays are reproducible (open item #3)."""
import hashlib
from typing import Sequence


def _h(x) -> int:
    return int(hashlib.sha1(str(x).encode()).hexdigest(), 16)


def map_visitor(visitor_id, customer_ids: Sequence[str]) -> str:
    return customer_ids[_h(("v", visitor_id)) % len(customer_ids)]


def map_item(item_id, product_ids: Sequence[str]) -> str:
    return product_ids[_h(("i", item_id)) % len(product_ids)]
