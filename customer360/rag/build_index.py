#!/usr/bin/env python3
"""Build the vector index over support tickets + product reviews.
Backends:
  tfidf (default, offline, no downloads): TF-IDF sparse vectors (1-2 grams) -- fast, fully local.
  st    : sentence-transformers 'all-MiniLM-L6-v2' dense embeddings + FAISS  (pip install sentence-transformers faiss-cpu)
Each chunk keeps metadata (source, ticket_id, customer_id, category, sentiment, date) for filtered retrieval."""
import argparse, glob, os, re, sys, pickle, numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from textutil import analyzer

def parse_tickets(d):
    rows = []
    for f in sorted(glob.glob(f"{d}/tickets/*.txt")):
        for blk in open(f).read().split("\n---\n"):
            kv = dict(re.findall(r"^([A-Z_]+): (.*)$", blk, flags=re.M))
            if "TICKET_ID" in kv: rows.append(dict(source="ticket", doc_id=kv["TICKET_ID"], customer_id=int(kv["CUSTOMER_ID"]), date=kv["DATE"][:10],
                                                   category=kv.get("CATEGORY"), sentiment=kv.get("SENTIMENT"), text=kv.get("BODY", "")))
    return rows

def parse_reviews(d):
    r = pd.read_csv(f"{d}/mysql/reviews.csv")
    return [dict(source="review", doc_id=f"R{x.review_id}", customer_id=int(x.customer_id), date=x.review_date, category=f"product_{x.product_id}",
                 sentiment="positive" if x.rating >= 4 else "negative" if x.rating <= 2 else "neutral", text=f"[{x.rating}/5] {x.review_text}") for x in r.itertuples()]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--data", default="data"); ap.add_argument("--backend", default="tfidf"); ap.add_argument("--out", default="rag/index")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    docs = pd.DataFrame(parse_tickets(a.data) + parse_reviews(a.data)); print("documents:", len(docs))
    # retrieval text = category + sentiment + body, so metadata words (delivery, refund, negative ...) are searchable too
    docs["index_text"] = docs.category.fillna("") + " " + docs.sentiment.fillna("") + " " + docs.text
    if a.backend == "st":
        from sentence_transformers import SentenceTransformer; import faiss
        emb = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2").encode(docs.index_text.tolist(), batch_size=256, normalize_embeddings=True, show_progress_bar=True)
        ix = faiss.IndexFlatIP(emb.shape[1]); ix.add(emb.astype("float32")); faiss.write_index(ix, f"{a.out}/faiss.index"); obj = {}
    else:
        from sklearn.feature_extraction.text import TfidfVectorizer
        vec = TfidfVectorizer(analyzer=analyzer, min_df=2, sublinear_tf=True); X = vec.fit_transform(docs.index_text); obj = dict(vec=vec, X=X)
    pickle.dump(dict(docs=docs, backend=a.backend, **obj), open(f"{a.out}/index.pkl", "wb")); print("index built:", a.backend)

if __name__ == "__main__": main()
