"""Tiny lexicon sentiment scorer (no external model needed; swap for VADER/transformers if wanted).
Returns a score in [-1, 1]."""
import re

POS = {"love", "great", "good", "excellent", "amazing", "awesome", "fast", "perfect", "happy",
       "best", "recommend", "thanks", "thank", "nice", "wonderful", "satisfied", "easy"}
NEG = {"bad", "terrible", "awful", "worst", "slow", "late", "broken", "refund", "scam", "angry",
       "disappointed", "hate", "poor", "never", "damaged", "wrong", "missing", "cancel", "useless"}
NEGATORS = {"not", "no", "never", "n't", "hardly"}
_tok = re.compile(r"[a-z']+")


def score(text: str) -> float:
    toks = _tok.findall((text or "").lower())
    s = n = 0
    for i, t in enumerate(toks):
        v = 1 if t in POS else -1 if t in NEG else 0
        if v:
            if i and (toks[i - 1] in NEGATORS or toks[i - 1].endswith("n't")):
                v = -v
            s += v
            n += 1
    return 0.0 if n == 0 else max(-1.0, min(1.0, s / n))


def label(sc: float) -> str:
    return "positive" if sc > 0.15 else "negative" if sc < -0.15 else "neutral"
