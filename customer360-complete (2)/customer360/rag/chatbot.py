#!/usr/bin/env python3
"""GenAI RAG chatbot over support tickets + reviews.
  retrieve(): top-k similar chunks (cosine), optional metadata filters (customer_id / category / sentiment / source)
  answer()  : if ANTHROPIC_API_KEY is set -> Claude answers ONLY from the retrieved context and cites doc_ids;
              otherwise an extractive, offline summary (counts by category/sentiment + top evidence) is returned.
Usage:  python rag/chatbot.py "why are customers unhappy with delivery?"   |   --customer 4242 "what did this customer complain about?"
"""
import argparse, os, sys, pickle, json, collections, numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from textutil import analyzer  # noqa: needed to unpickle the vectorizer

class RAG:
    def __init__(self, path=os.getenv("RAG_INDEX", "rag/index/index.pkl")):
        o = pickle.load(open(path, "rb")); self.docs, self.backend = o["docs"], o["backend"]
        if self.backend == "tfidf": self.vec, self.X = o["vec"], o["X"]
        else:
            import faiss; from sentence_transformers import SentenceTransformer
            self.ix = faiss.read_index(os.path.join(os.path.dirname(path), "faiss.index")); self.m = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    def retrieve(self, q, k=8, customer_id=None, category=None, sentiment=None, source=None):
        mask = np.ones(len(self.docs), bool)
        if customer_id is not None: mask &= (self.docs.customer_id.values == customer_id)
        if category: mask &= self.docs.category.str.contains(category, na=False).values
        if sentiment: mask &= (self.docs.sentiment.values == sentiment)
        if source: mask &= (self.docs.source.values == source)
        if self.backend == "tfidf": sc = (self.X @ self.vec.transform([q]).T).toarray().ravel()
        else:
            e = self.m.encode([q], normalize_embeddings=True).astype("float32"); D, I = self.ix.search(e, min(len(self.docs), 5000)); sc = np.zeros(len(self.docs)); sc[I[0]] = D[0]
        sc = np.where(mask, sc, -1); top = np.argsort(-sc)[:k]; top = [i for i in top if sc[i] > 0]
        if not top and not mask.all():                       # filters given but no lexical match -> most recent matching items
            idx = np.where(mask)[0]; idx = idx[np.argsort(self.docs.date.values[idx])[::-1][:k]]
            return self.docs.iloc[idx].assign(score=0.0)
        return self.docs.iloc[top].assign(score=sc[top])

    def answer(self, q, **flt):
        hits = self.retrieve(q, **flt)
        if hits.empty: return "No relevant tickets or reviews found.", hits
        ctx = "\n".join(f"[{r.doc_id}] ({r.source}, {r.category}, {r.sentiment}) {r.text}" for r in hits.itertuples())
        if os.getenv("ANTHROPIC_API_KEY"):
            import anthropic
            msg = anthropic.Anthropic().messages.create(model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6"), max_tokens=500,
                system="You are a customer-insights analyst. Answer ONLY from the provided support tickets/reviews. Cite doc ids in brackets. If the context is insufficient, say so.",
                messages=[{"role": "user", "content": f"Question: {q}\n\nContext:\n{ctx}"}])
            return msg.content[0].text, hits
        cat = collections.Counter(hits.category); sen = collections.Counter(hits.sentiment)
        txt = f"Top themes: {dict(cat)}; sentiment mix: {dict(sen)}.\nEvidence:\n" + "\n".join(f"- [{r.doc_id}] {r.text}" for r in hits.head(3).itertuples())
        return txt, hits

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("question"); ap.add_argument("--customer", type=int); ap.add_argument("--category"); ap.add_argument("--sentiment"); ap.add_argument("-k", type=int, default=8)
    a = ap.parse_args(); ans, hits = RAG().answer(a.question, k=a.k, customer_id=a.customer, category=a.category, sentiment=a.sentiment); print(ans)
