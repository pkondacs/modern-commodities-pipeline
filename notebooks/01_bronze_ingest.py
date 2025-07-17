# Databricks Notebook: 01_bronze_ingest.py
# Purpose: Ingest legacy trade CSV data into Delta Bronze table
# Author: Peter Kondacs (Showcase Project)
# Context: Simulated decommissioning of legacy commodities platform

from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schema explicitly for regulated environments
tarde_schema = StructType([
    StructField("trade_id", IntegerType(), True),
    StructField("commodity", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("counterparty", StringType(), True),
    StructField("trade_date", DateType(), True)
])

# Path to legacy CSVs (simulating SFTP dump or shared drive)
legacy_path = "/mnt/legacy_data/legacy_trades.csv"

# Read legacy data
legacy_df = spark.read \
    .option("header", True) \
    .schema(tarde_schema) \
    .csv(legacy_path) \
    .withColumn("source_file", input_file_name())

# Optional preview for stakeholder collaboration
legacy_df.display()

# Write to Bronze Delta table
bronze_table = "bronze.trades_raw"
legacy_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_table)

# Stakeholder note (simulated):
# "Ingested raw trade data with full lineage tracking. Schema enforced."
print(f"✅ Ingest complete: {bronze_table}")
