-- Run after Bronze, Silver, and Gold notebooks. Each row is an invariant; every passed value must be true.
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM c360.bronze_events) AS bronze_n,
    (SELECT COUNT(*) FROM c360.silver_events) AS silver_n,
    (SELECT COUNT(*) FROM c360.silver_events_quarantine) AS quarantine_n,
    (SELECT COALESCE(SUM(duplicate_count), 0) FROM c360.silver_event_duplicates) AS duplicates_removed,
    (SELECT COUNT(*) FROM c360.silver_events) AS silver_rows_again,
    (SELECT COUNT(DISTINCT event_id) FROM c360.silver_events) AS silver_unique_event_ids,
    (SELECT COUNT(*) FROM c360.silver_dim_customer WHERE is_current = true) AS current_customer_n,
    (SELECT COUNT(*) FROM c360.gold_customer_360) AS gold_customer_n,
    (SELECT COUNT(*) FROM c360.gold_abandoned_carts) AS gold_abandoned_cart_n
)
SELECT
  'bronze_equals_silver_plus_quarantine_plus_duplicates' AS check_name,
  bronze_n AS expected,
  silver_n + quarantine_n + duplicates_removed AS actual,
  bronze_n = silver_n + quarantine_n + duplicates_removed AS passed
FROM counts
UNION ALL
SELECT
  'silver_event_ids_are_unique',
  silver_rows_again,
  silver_unique_event_ids,
  silver_rows_again = silver_unique_event_ids
FROM counts
UNION ALL
SELECT
  'gold_customer_count_matches_current_scd2_dimension',
  current_customer_n,
  gold_customer_n,
  current_customer_n = gold_customer_n
FROM counts
UNION ALL
SELECT
  'gold_abandoned_cart_table_is_populated',
  CAST(1 AS BIGINT),
  gold_abandoned_cart_n,
  gold_abandoned_cart_n > 0
FROM counts;
