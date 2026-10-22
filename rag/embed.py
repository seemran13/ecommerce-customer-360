"""Embedders. Default: sentence-transformers `all-MiniLM-L6-v2` (384-d, CPU-friendly, strong for short support text).
Fallback when that package/model is unavailable: TF-IDF (still a real, deterministic embedding - used by the tests)."""
import re

import numpy as np

_STOP = {"the", "a", "an", "is", "are", "was", "were", "do", "does", "what", "say", "about", "of", "to", "and",
         "in", "my", "i", "it", "for", "on", "with", "customers", "customer", "this", "that", "any", "have", "has"}


def analyzer(text: str):
    """lower-case, light suffix stripping (refunds->refund, delayed->delay), drop stop words; + bigrams."""
    toks = []
    for t in re.findall(r"[a-z]+", text.lower()):
        if t in _STOP:
            continue
        for suf in ("ing", "ed", "es", "s"):
            if t.endswith(suf) and len(t) - len(suf) >= 4:
                t = t[: -len(suf)]
                break
        toks.append(t)
    return toks + [f"{a}_{b}" for a, b in zip(toks, toks[1:])]


class TfidfEmbedder:
    name = "tfidf"

    def __init__(self):
        from sklearn.feature_extraction.text import TfidfVectorizer
        self.v = TfidfVectorizer(analyzer=analyzer, sublinear_tf=True)

    def fit(self, texts):
        self.v.fit(texts)
        return self

    def encode(self, texts):
        m = self.v.transform(texts).toarray().astype("float32")
        return m / np.clip(np.linalg.norm(m, axis=1, keepdims=True), 1e-9, None)


class STEmbedder:
    name = "all-MiniLM-L6-v2"

    def __init__(self):
        from sentence_transformers import SentenceTransformer
        self.m = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    def fit(self, texts):
        return self

    def encode(self, texts):
        return self.m.encode(list(texts), normalize_embeddings=True, batch_size=64).astype("float32")


def get_embedder(prefer="auto"):
    if prefer in ("auto", "st"):
        try:
            return STEmbedder()
        except Exception:
            if prefer == "st":
                raise
    return TfidfEmbedder()
