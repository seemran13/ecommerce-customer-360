"""Spark Core (RDD) cleansing of raw web logs.
 * parses each JSON line; malformed lines are counted in an Accumulator and written to a quarantine path
 * broadcast join attaches product details (small dim)
 * dedups on event_id (Kafka replay / at-least-once)
Usage: spark-submit batch_cleanse.py <weblogs_in> <products_in> <out_clean> <out_bad> [run_date]"""
import sys, json
from datetime import datetime
from pyspark.sql import Row, functions as F
from pyspark.sql.types import *
from common import spark

def main(inp, prod_in, out, bad_out):
    sp = spark("cleanse_weblogs"); sc = sp.sparkContext
    bad_acc = sc.accumulator(0); ok_acc = sc.accumulator(0)
    REQ = ("event_id", "event_ts", "session_id", "customer_id", "event_type")

    def parse(line):
        try:
            d = json.loads(line)
            if any(d.get(k) in (None, "") for k in REQ): raise ValueError("missing")
            datetime.fromisoformat(d["event_ts"]); int(d["customer_id"])
            if d["event_type"] not in ("view", "add_to_cart", "checkout", "purchase", "remove_from_cart"): raise ValueError("type")
            ok_acc.add(1)
            return [("ok", Row(event_id=d["event_id"], event_ts=d["event_ts"], session_id=d["session_id"], customer_id=int(d["customer_id"]),
                              event_type=d["event_type"], product_id=int(d.get("product_id") or 0), price=float(d.get("price") or 0),
                              qty=int(d.get("qty") or 0), order_id=int(d.get("order_id") or 0), device=d.get("device"), ip=d.get("ip")))]
        except Exception:
            bad_acc.add(1); return [("bad", line)]

    raw = sc.textFile(inp).filter(lambda l: l.strip()).flatMap(parse).cache()
    good = raw.filter(lambda kv: kv[0] == "ok").map(lambda kv: kv[1])
    bad = raw.filter(lambda kv: kv[0] == "bad").map(lambda kv: kv[1])
    bad.saveAsTextFile(bad_out)
    df = sp.createDataFrame(good).dropDuplicates(["event_id"])

    prod = sp.read.format("avro").load(prod_in) if prod_in.endswith("avro") or "landing" in prod_in else sp.read.csv(prod_in, header=True, inferSchema=True)
    prod = prod.select("product_id", "product_name", "category", "brand", F.col("price").alias("list_price"))
    out_df = (df.join(F.broadcast(prod), "product_id", "left")           # broadcast join
                .withColumn("event_ts", F.to_timestamp("event_ts")).withColumn("event_date", F.to_date("event_ts")))
    out_df.write.mode("overwrite").partitionBy("event_date").parquet(out)
    print(f"CLEANSE_SUMMARY ok={ok_acc.value} malformed={bad_acc.value} deduped_rows={out_df.count()}")
    sp.stop()

if __name__ == "__main__": main(*sys.argv[1:5])
