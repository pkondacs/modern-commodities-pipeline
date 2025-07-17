# Databricks Notebook: 02_silver_clean.py
# Purpose: Clean and validate raw trade data (Bronze → Silver)
# Author: Peter Kondacs (Showcase Project)
# Context: Part of Dev→Prod Lakehouse pipeline for financial services

from pyspark.sql.functions import col, to_date

# Read raw trade data from Bronze table
bronze_df = spark.read.table("bronze.trades_raw")

# --- Data Cleansing ---
# Drop rows with nulls in critical fields
clean_df = bronze_df.dropna(subset=["trade_id", "commodity", "quantity", "price", "trade_date"])

# Convert string dates if necessary (e.g., from ingestion errors)
clean_df = clean_df.withColumn("trade_date", to_date(col("trade_date")))

# Remove obviously invalid data (e.g., negative quantities or prices)
validated_df = clean_df.filter((col("quantity") > 0) & (col("price") > 0))

# --- Optional Enrichment ---
# Example: Create a value column for trade size
validated_df = validated_df.withColumn("trade_value", col("quantity") * col("price"))

# Preview cleaned data
validated_df.display()

# Write to Silver Delta table
silver_table = "silver.trades_clean"
validated_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

# Stakeholder note (simulated):
# "Removed nulls, fixed dates, and validated trade quantities/prices. Data ready for analysis."
print(f"✅ Cleaned data written to: {silver_table}")
