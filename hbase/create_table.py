"""Create the customer360 table. Row key = customer_id (32-char hex => naturally well distributed, no hot-spotting).
Families: p (profile) m (model) c (live cart). BLOOMFILTER=ROW + in-memory block cache keep point gets < 100 ms."""
import happybase

from c360.config import settings as S

conn = happybase.Connection(S.hbase_host, S.hbase_port)
if S.hbase_table.encode() not in conn.tables():
    conn.create_table(S.hbase_table, {
        "p": dict(max_versions=1, bloom_filter_type="ROW", in_memory=True),
        "m": dict(max_versions=1, bloom_filter_type="ROW", in_memory=True),
        "c": dict(max_versions=3, bloom_filter_type="ROW", in_memory=True, time_to_live=604800)})
    print("created", S.hbase_table)
else:
    print("exists", S.hbase_table)
