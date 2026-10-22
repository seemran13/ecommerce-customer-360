from rag.chatbot import Chatbot, build_index
from rag.ingest import Doc, parse_tickets


def test_ticket_parser(tmp_path):
    (tmp_path / "t.txt").write_text("TICKET_ID: T1\nCUSTOMER_ID: c1\nCREATED: x\nCATEGORY: refund\nBODY: refund missing\n---\n"
                                    "TICKET_ID: T2\nCUSTOMER_ID: c2\nCREATED: y\nCATEGORY: praise\nBODY: great service\n---\n")
    d = parse_tickets(str(tmp_path))
    assert [x.doc_id for x in d] == ["T1", "T2"] and d[0].meta["category"] == "refund"


def test_retrieval_filter_and_grounded_fallback(tmp_path):
    import os
    src = tmp_path / "src"; (src / "tickets").mkdir(parents=True)
    (src / "tickets" / "a.txt").write_text(
        "TICKET_ID: T1\nCUSTOMER_ID: c1\nCREATED: x\nCATEGORY: refund\nBODY: refund still missing after ten days\n---\n"
        "TICKET_ID: T2\nCUSTOMER_ID: c2\nCREATED: y\nCATEGORY: delivery\nBODY: package delivery is very late\n---\n")
    (src / "reviews.csv").write_text("review_id,order_id,customer_id,product_id,score,comment,created_at\n"
                                     "R1,o1,c1,p1,5,excellent fast shipping,2026-01-01\n")
    idx = str(tmp_path / "idx")
    build_index(str(src), idx, prefer="tfidf")
    bot = Chatbot(idx, llm=None)
    r = bot.ask("late delivery")
    assert r["sources"][0]["id"] == "T2"
    r = bot.ask("anything", customer_id="c1")
    assert {s["id"] for s in r["sources"]} <= {"T1", "R1"}
    assert "can't find" in bot.ask("zzzz qqqq")["answer"]
    seen = {}
    bot.llm = lambda system, user: seen.update(u=user) or "ok [T2]"
    bot.ask("late delivery")
    assert "Context:" in seen["u"] and "[T2]" in seen["u"]
