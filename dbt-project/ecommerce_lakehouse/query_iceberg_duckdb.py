import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")

print("=== Clickstream Events (Bronze) ===")
clickstream = con.execute("""
    SELECT event_type, COUNT(*) as event_count
    FROM iceberg_scan('/opt/iceberg-warehouse/bronze/clickstream_events')
    GROUP BY event_type
    ORDER BY event_count DESC
""").fetchdf()
print(clickstream)

print("\n=== Order Events (Bronze) — distinct statuses ===")
orders = con.execute("""
    SELECT status, COUNT(*) as status_count
    FROM iceberg_scan('/opt/iceberg-warehouse/bronze/order_events')
    GROUP BY status
    ORDER BY status_count DESC
""").fetchdf()
print(orders)
