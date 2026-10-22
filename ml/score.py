"""Score every customer with the trained models -> data/ml/scores.parquet (customer_id, segment, propensity)."""
import os

import joblib
import numpy as np
import pandas as pd

from c360.config import settings as S
from ml.features import FEATURES, build_features, load_inputs
from ml.train import SEG_FEATURES


def score(src="data/source", out=None):
    out = out or S.ml_root
    m = joblib.load(f"{out}/models.joblib")
    orders, events = load_inputs(src)
    T = events.event_ts.max().normalize() + pd.Timedelta(days=1)
    X = build_features(orders, events, T, events.customer_id.unique())
    seg = m["km"].predict(m["scaler"].transform(np.log1p(X[SEG_FEATURES])))
    df = pd.DataFrame({"customer_id": X.customer_id, "segment": [m["seg_names"][int(s)] for s in seg],
                       "propensity": m["rf"].predict_proba(X[FEATURES])[:, 1].round(4)})
    try:
        df.to_parquet(f"{out}/scores.parquet", index=False)
    except ImportError:                      # pyarrow missing (it is in requirements.txt; fallback keeps the demo usable)
        df.to_csv(f"{out}/scores.csv", index=False)
    return df


if __name__ == "__main__":
    d = score()
    print(d.segment.value_counts().to_string(), "\n", d.propensity.describe().round(3).to_string())
