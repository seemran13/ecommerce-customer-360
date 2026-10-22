"""Minimal persistent vector store (cosine similarity over a normalised matrix, metadata filters).
Exact search is the right choice at this scale (<~1M chunks); swap for Chroma/FAISS/OpenSearch beyond that -
the interface (`add`, `search`, `save`, `load`) is the only thing the chatbot depends on."""
import json
import os

import numpy as np


class VectorStore:
    def __init__(self):
        self.vecs, self.docs = None, []

    def add(self, vectors: np.ndarray, docs):
        self.vecs = vectors if self.vecs is None else np.vstack([self.vecs, vectors])
        self.docs.extend(docs)

    def search(self, qvec: np.ndarray, k=5, where=None):
        sims = self.vecs @ qvec
        order = np.argsort(-sims)
        out = []
        for i in order:
            d = self.docs[i]
            if where and any(d.meta.get(a) != b for a, b in where.items()):
                continue
            out.append((float(sims[i]), d))
            if len(out) == k:
                break
        return out

    def save(self, folder):
        os.makedirs(folder, exist_ok=True)
        np.save(f"{folder}/vecs.npy", self.vecs)
        json.dump([{"doc_id": d.doc_id, "text": d.text, "meta": d.meta} for d in self.docs], open(f"{folder}/docs.json", "w"))

    @classmethod
    def load(cls, folder):
        from rag.ingest import Doc
        s = cls()
        s.vecs = np.load(f"{folder}/vecs.npy")
        s.docs = [Doc(**d) for d in json.load(open(f"{folder}/docs.json"))]
        return s
