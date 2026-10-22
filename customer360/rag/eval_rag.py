#!/usr/bin/env python3
"""Retrieval quality check: for template-derived questions, the top-5 hits must come from the expected ticket category (precision@5)."""
import sys; sys.path.insert(0, "rag")
from chatbot import RAG
Q = {"delivery": ["package has not arrived tracking stuck", "wrong address delivery redelivery"], "refund": ["refund not credited after return", "refund timeline UPI"],
     "product": ["product stopped working defective"], "payment": ["money debited twice duplicate payment"], "cart": ["cart empties at checkout coupon will not apply"]}
r = RAG(); hit = tot = 0
for cat, qs in Q.items():
    for q in qs:
        h = r.retrieve(q, k=5, source="ticket"); hit += int((h.category == cat).sum()); tot += len(h)
print(f"precision@5 = {hit / max(1, tot):.3f}  ({hit}/{tot})"); sys.exit(0 if hit / max(1, tot) >= 0.8 else 1)
