from pyspark.sql.types import (DecimalType, DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampType)

S, T, I, D = StringType(), TimestampType(), IntegerType(), DecimalType(12, 2)
RAW = {  # column order == MySQL table order == Sqoop text output order
    "customers": StructType([StructField(n, t) for n, t in [
        ("customer_id", S), ("name", S), ("email", S), ("city", S), ("state", S), ("zip", S), ("tier", S),
        ("created_at", T), ("updated_at", T)]]),
    "products": StructType([StructField(n, t) for n, t in [
        ("product_id", S), ("category", S), ("price", D), ("name", S), ("updated_at", T)]]),
    "orders": StructType([StructField(n, t) for n, t in [
        ("order_id", S), ("customer_id", S), ("status", S), ("order_ts", T), ("total_amount", D),
        ("updated_at", T)]]),
    "order_items": StructType([StructField(n, t) for n, t in [
        ("order_id", S), ("item_no", I), ("product_id", S), ("quantity", I), ("unit_price", D),
        ("updated_at", T)]]),
}
KEYS = {"customers": ["customer_id"], "products": ["product_id"], "orders": ["order_id"],
        "order_items": ["order_id", "item_no"]}

# Kafka cart_events JSON payload
CART_EVENT = StructType([
    StructField("event_id", S), StructField("event_ts", T), StructField("orig_ts", S),
    StructField("customer_id", S), StructField("session_id", S), StructField("event_type", S),
    StructField("product_id", S), StructField("quantity", I), StructField("unit_price", DoubleType()),
    StructField("speed", DoubleType()), StructField("source", S), StructField("seq", I)])

BRAND_MENTION = StructType([
    StructField("mention_id", S), StructField("event_ts", T), StructField("brand", S), StructField("text", S),
    StructField("sentiment_score", DoubleType()), StructField("sentiment", S), StructField("source", S)])
