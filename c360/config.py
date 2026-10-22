"""Central settings. Everything is overridable by environment variable (see .env.example)."""
import os
from dataclasses import dataclass, field


def _e(key, default):
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Settings:
    # --- Kafka ---------------------------------------------------------------
    kafka_bootstrap: str = _e("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9192,localhost:9292")
    topic_cart: str = _e("TOPIC_CART", "cart_events")
    topic_brand: str = _e("TOPIC_BRAND", "brand_mentions")
    topic_weather: str = _e("TOPIC_WEATHER", "weather_events")
    topic_alerts: str = _e("TOPIC_ALERTS", "cart_alerts")
    # --- Cart abandonment ----------------------------------------------------
    idle_threshold_s: int = int(_e("IDLE_THRESHOLD_S", "900"))        # 15 minutes
    alert_sla_s: int = int(_e("ALERT_SLA_S", "60"))
    # --- Storage roots (local FS, hdfs://, s3a://, dbfs:/ all work) ----------
    raw_root: str = _e("C360_RAW", "data/lake/raw")
    bronze_root: str = _e("C360_BRONZE", "data/lake/bronze")
    silver_root: str = _e("C360_SILVER", "data/lake/silver")
    gold_root: str = _e("C360_GOLD", "data/lake/gold")
    checkpoint_root: str = _e("C360_CHECKPOINTS", "data/lake/_checkpoints")
    ml_root: str = _e("C360_ML", "data/ml")
    # --- MySQL ---------------------------------------------------------------
    mysql_host: str = _e("MYSQL_HOST", "localhost")
    mysql_port: int = int(_e("MYSQL_PORT", "3306"))
    mysql_user: str = _e("MYSQL_USER", "c360")
    mysql_password: str = _e("MYSQL_PASSWORD", "c360pass")
    mysql_db: str = _e("MYSQL_DB", "shop")
    # --- HBase (Thrift) ------------------------------------------------------
    hbase_host: str = _e("HBASE_HOST", "localhost")
    hbase_port: int = int(_e("HBASE_PORT", "9090"))
    hbase_table: str = _e("HBASE_TABLE", "customer360")
    # --- AWS -----------------------------------------------------------------
    s3_bucket: str = _e("C360_S3_BUCKET", "c360-lakehouse-demo")
    aws_region: str = _e("AWS_REGION", "ap-south-1")
    glue_db: str = _e("GLUE_DB", "c360")
    athena_output: str = _e("ATHENA_OUTPUT", "s3://c360-lakehouse-demo/athena-results/")
    # --- Brand tracking ------------------------------------------------------
    brand_keywords: tuple = field(default_factory=lambda: tuple(
        k.strip().lower() for k in _e("BRAND_KEYWORDS", "olist,retailrocket,nexturn").split(",") if k.strip()))

    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"


settings = Settings()
