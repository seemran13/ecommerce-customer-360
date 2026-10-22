"""
RAG chatbot over support tickets + reviews.

  question -> embed -> top-k retrieval (optional filter: customer_id / doc type) -> grounded prompt -> LLM answer
Answers must cite doc ids like [T000123]; if the retrieved text does not contain the answer the model must say so.
LLM: Anthropic API when ANTHROPIC_API_KEY is set (model via RAG_MODEL, default claude-sonnet-5-5);
otherwise an extractive fallback returns the top passages so the pipeline still works offline.

    python -m rag.chatbot --build                       # index data/source
    python -m rag.chatbot -q "What are customers complaining about in deliveries?"
    python -m rag.chatbot -q "Any issues for this customer?" --customer <id>
"""
import argparse
import os
import pickle

from rag.embed import get_embedder
from rag.ingest import load_all
from rag.store import VectorStore

INDEX = "data/rag_index"
SYSTEM = ("You are a customer-insights analyst. Answer ONLY from the numbered context passages. "
          "Cite passage ids in square brackets, e.g. [T000123]. If the context does not contain the answer, say "
          "'I can't find that in the tickets or reviews.' Be concise and quantify when the passages allow it.")


def build_index(src="data/source", folder=INDEX, prefer="auto"):
    docs = load_all(src)
    emb = get_embedder(prefer).fit([d.text for d in docs])
    st = VectorStore()
    st.add(emb.encode([d.text for d in docs]), docs)
    st.save(folder)
    pickle.dump(emb, open(f"{folder}/embedder.pkl", "wb")) if emb.name == "tfidf" else open(f"{folder}/embedder.txt", "w").write(emb.name)
    return len(docs), emb.name


class Chatbot:
    def __init__(self, folder=INDEX, llm=None):
        self.store = VectorStore.load(folder)
        self.emb = pickle.load(open(f"{folder}/embedder.pkl", "rb")) if os.path.exists(f"{folder}/embedder.pkl") else get_embedder("st")
        self.llm = llm if llm is not None else self._default_llm()

    @staticmethod
    def _default_llm():
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return None
        import anthropic
        client = anthropic.Anthropic()
        model = os.environ.get("RAG_MODEL", "claude-sonnet-5-5")

        def call(system, user):
            r = client.messages.create(model=model, max_tokens=600, system=system,
                                       messages=[{"role": "user", "content": user}])
            return r.content[0].text
        return call

    def retrieve(self, question, k=6, customer_id=None, doc_type=None):
        where = {}
        if customer_id:
            where["customer_id"] = customer_id
        if doc_type:
            where["type"] = doc_type
        return self.store.search(self.emb.encode([question])[0], k, where or None)

    def ask(self, question, k=6, customer_id=None, doc_type=None, min_sim=0.05):
        hits = [(s, d) for s, d in self.retrieve(question, k, customer_id, doc_type) if s >= min_sim]
        if not hits:
            return {"answer": "I can't find that in the tickets or reviews.", "sources": []}
        ctx = "\n".join(f"[{d.doc_id}] ({d.meta.get('type')}, {d.meta.get('category') or 'score ' + str(d.meta.get('score'))}) {d.text}"
                        for _, d in hits)
        if self.llm is None:      # offline extractive fallback
            ans = "Top matching passages:\n" + ctx
        else:
            ans = self.llm(SYSTEM, f"Context:\n{ctx}\n\nQuestion: {question}")
        return {"answer": ans, "sources": [{"id": d.doc_id, "score": round(s, 3), **d.meta} for s, d in hits]}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--src", default="data/source")
    ap.add_argument("-q", "--question")
    ap.add_argument("--customer")
    ap.add_argument("--embedder", default="auto", choices=["auto", "st", "tfidf"])
    a = ap.parse_args()
    if a.build:
        print("indexed", build_index(a.src, prefer=a.embedder))
    if a.question:
        r = Chatbot().ask(a.question, customer_id=a.customer)
        print(r["answer"])
