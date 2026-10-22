import re
_STOP = set("a an the is are was were be been to of and or in on for with my i me it this that at as by from have has had not no".split())
def _stem(w):
    for suf in ("ies", "ied", "ing", "ed", "es", "s", "y"):
        if w.endswith(suf) and len(w) - len(suf) >= 4: return w[: -len(suf)]
    return w
def analyzer(text):
    toks = [_stem(t) for t in re.findall(r"[a-z0-9']+", text.lower()) if t not in _STOP]
    return toks + [a + "_" + b for a, b in zip(toks, toks[1:])]   # unigrams + bigrams
