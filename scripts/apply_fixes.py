def edit(path, fn):
    s = open(path).read()
    new = fn(s)
    if new == s:
        print("already ok   ", path)
    else:
        open(path, "w").write(new)
        print("patched      ", path)

def fix_common(s):
    if "fs.defaultFS" in s:
        return s
    return s.replace('''         .config("spark.sql.session.timeZone", "UTC")''', '''         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.hadoop.fs.defaultFS", os.environ.get("C360_DEFAULT_FS", "file:///"))
         .config("spark.sql.warehouse.dir", os.path.abspath(os.environ.get("C360_WAREHOUSE", "data/lake/_warehouse")))''')

def fix_features(s):
    if "def coerce_numeric" in s:
        return s
    s = s.replace("    return orders, events\n", '    return coerce_numeric(orders, ["total_amount"]), coerce_numeric(events, ["unit_price", "quantity"])\n', 1)
    func = '''

def coerce_numeric(df, cols):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].map(lambda v: None if v is None else float(v))
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df'''
    return s.replace("\n\ndef build_features", func + "\n\ndef build_features", 1)

def fix_gold(s):
    s = s.replace('orders.select("order_id", "customer_id", "order_ts", "total_amount").toPandas()',
                  'orders.select("order_id", "customer_id", "order_ts", F.col("total_amount").cast("double").alias("total_amount")).toPandas()')
    if 'withColumn("unit_price"' not in s:
        s = s.replace('ev.toPandas()', 'ev.withColumn("unit_price", F.col("unit_price").cast("double")).toPandas()')
    return s

edit("spark/common.py", fix_common)
edit("ml/features.py", fix_features)
edit("spark/jobs/medallion_gold.py", fix_gold)
