"""Skip MySQL/Sqoop for a quick Spark-only run: write generated tables into the raw landing zone as headerless CSV
(identical layout to Sqoop output) + tickets + reviews.   python scripts/land_raw_local.py"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd

from c360.config import settings as S

SRC = "data/source"
for t in ("customers", "products", "orders", "order_items"):
    d = os.path.join(S.raw_root, t)
    os.makedirs(d, exist_ok=True)
    pd.read_csv(f"{SRC}/mysql/{t}.csv").to_csv(f"{d}/part-00000.csv", header=False, index=False)
shutil.copytree(f"{SRC}/tickets", os.path.join(S.raw_root, "tickets"), dirs_exist_ok=True)
os.makedirs(os.path.join(S.raw_root, "reviews"), exist_ok=True)
shutil.copy(f"{SRC}/reviews.csv", os.path.join(S.raw_root, "reviews", "reviews.csv"))
print("landed raw data under", S.raw_root)
