p = "spark/jobs/stream_bronze_silver.py"
s = open(p).read()
if "mode(\"ignore\")" in s:
    print("already ok   ", p)
else:
    anchor = '    trig = {"availableNow": True} if a.once else {"processingTime": f"{a.trigger} seconds"}\n'
    add = anchor + '''
    spark.createDataFrame([], "key string, value string, topic string, partition int, offset long, timestamp timestamp") \\
        .write.format("delta").mode("ignore").save(path(S.bronze_root, "kafka_cart_events"))
'''
    assert anchor in s, "anchor not found"
    open(p, "w").write(s.replace(anchor, add, 1))
    print("patched      ", p)
