# Databricks Notebook: 03_gold_aggregate.py
# Purpose: Create Gold layer aggregations for analytics
# Author: Peter Kondacs (Showcase Project)
# Context: Simulated Dev→Prod pipeline for legacy platform replacement

from pyspark.sql.functions import sum as _sum, avg

# Read cleaned Silver layer data
df_silver = spark.read.table("hive_metastore.commodities.trades_clean")

# --- Business Aggregation ---
# Aggregate total volume and average price per commodity
df_gold = df_silver.groupBy("commodity").agg(
    _sum("quantity").alias("total_volume"),
    avg("price").alias("avg_unit_price"),
    _sum("trade_value").alias("total_trade_value")
)

# Preview the aggregated result
# df_gold.display()

# Write to Gold Delta table
gold_table = "hive_metastore.commodities.trades_summary"
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table)

# Stakeholder note (simulated):
# "Gold layer summarizes total volume, trade value, and average price per commodity."
print(f"✅ Aggregated summary written to: {gold_table}")
