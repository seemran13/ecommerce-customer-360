"""Load support tickets (text files) and reviews (CSV) into uniform documents for indexing."""
import glob
import os
import re
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class Doc:
    doc_id: str
    text: str
    meta: dict = field(default_factory=dict)


def parse_tickets(folder: str):
    docs = []
    for fp in sorted(glob.glob(os.path.join(folder, "*.txt"))):
        for block in open(fp, encoding="utf-8").read().split("---\n"):
            if not block.strip():
                continue
            g = lambda k: (re.search(rf"^{k}: (.*)$", block, re.M) or [None, ""])[1].strip()
            tid = g("TICKET_ID")
            if tid:
                docs.append(Doc(tid, g("BODY"), {"type": "ticket", "customer_id": g("CUSTOMER_ID"),
                                                 "category": g("CATEGORY"), "created": g("CREATED")}))
    return docs


def parse_reviews(csv_path: str):
    df = pd.read_csv(csv_path)
    return [Doc(r.review_id, str(r.comment), {"type": "review", "customer_id": r.customer_id,
                                              "score": int(r.score), "product_id": r.product_id})
            for r in df.itertuples()]


def load_all(src="data/source"):
    return parse_tickets(os.path.join(src, "tickets")) + parse_reviews(os.path.join(src, "reviews.csv"))
