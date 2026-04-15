import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# INIT SPARK
spark = SparkSession.builder \
    .appName("Silver Audit - Azure Parquet") \
    .getOrCreate()

print("===== SILVER LAYER AUDIT =====")

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/cleaned_retail_data"

# READ DATA
df = spark.read.parquet(silver_path)

# AUDIT
row_count = df.count()

dup_count = df.groupBy(df.columns).count().filter("count > 1").count()

null_count = 0
if "Customer ID" in df.columns:
    null_count = df.filter(col("Customer ID").isNull()).count()

neg_count = 0
if "Quantity" in df.columns:
    neg_count = df.filter(col("Quantity") < 0).count()

# SAVE REPORT
report_path = "/opt/hadoop/audit/silver_report.txt"

with open(report_path, "w") as f:
    f.write("SILVER LAYER AUDIT REPORT \n")
    f.write("===========================\n")
    f.write(f"Total Rows: {row_count}\n")
    f.write(f"Duplicate Rows: {dup_count}\n")
    f.write(f"Null Rows (Customer ID): {null_count}\n")
    f.write(f"Negative Quantity Rows: {neg_count}\n")

print(f"Report saved to: {report_path}")
print("===== AUDIT COMPLETED =====")

spark.stop()
