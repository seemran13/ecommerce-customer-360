# S3 raw zone, Glue Data Catalog, Athena

```bash
aws s3 mb s3://c360-raw
aws s3 mb s3://c360-lake
aws s3 sync data/weblogs  s3://c360-raw/weblogs/
aws s3 sync data/mysql    s3://c360-raw/mysql/
aws s3 sync data/tickets  s3://c360-raw/tickets/
aws glue create-database --database-input Name=c360_raw
aws glue create-crawler --name c360-raw-crawler --role AWSGlueServiceRole-c360 --database-name c360_raw \
  --targets '{"S3Targets":[{"Path":"s3://c360-raw/mysql/"},{"Path":"s3://c360-raw/weblogs/"}]}'
aws glue start-crawler --name c360-raw-crawler
```
Athena ad-hoc (convert to partitioned Parquet with CTAS for speed/cost):
```sql
CREATE TABLE c360_raw.events_parquet WITH (format='PARQUET', partitioned_by=ARRAY['dt'], external_location='s3://c360-lake/athena/events/') AS
SELECT event_id, session_id, customer_id, event_type, product_id, price, qty, device, dt FROM c360_raw.weblogs WHERE event_id IS NOT NULL;

-- top abandoned categories
SELECT p.category, count(DISTINCT e.session_id) abandoned_sessions
FROM c360_raw.events_parquet e
JOIN c360_raw.products p ON e.product_id=p.product_id
WHERE e.event_type='add_to_cart'
  AND NOT EXISTS (
    SELECT 1 FROM c360_raw.events_parquet c
    WHERE c.session_id=e.session_id AND c.event_type IN ('checkout','purchase')
  )
GROUP BY 1 ORDER BY 2 DESC;
```
