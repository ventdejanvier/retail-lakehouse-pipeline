import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# 1. CẤU HÌNH AZURE
account_name = "nhom5storage"
azure_key = os.environ.get("AZURE_STORAGE_ACCESS_KEY")

# 2. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("Gold Layer Audit") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", azure_key) \
    .getOrCreate()

print("===== GOLD LAYER AUDIT =====")

# 3. PATH GOLD
gold_ai_path = f"abfss://gold@{account_name}.dfs.core.windows.net/gold_customers_ai"
gold_business_path = f"abfss://gold@{account_name}.dfs.core.windows.net/gold_business_performance"
gold_product_path = f"abfss://gold@{account_name}.dfs.core.windows.net/gold_products_catalog"

# 4. ĐỌC DỮ LIỆU
df_ai = spark.read.format("delta").load(gold_ai_path)
df_business = spark.read.format("delta").load(gold_business_path)
df_product = spark.read.format("delta").load(gold_product_path)

# 5. AUDIT

report = []

# AI TABLE
report.append("=== GOLD CUSTOMERS AI ===")
report.append(f"Row count: {df_ai.count()}")

null_ai = df_ai.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_ai.columns
]).collect()[0]

report.append("Null values:")
for c in df_ai.columns:
    report.append(f"{c}: {null_ai[c]}")

dup_ai = df_ai.count() - df_ai.dropDuplicates().count()
report.append(f"Duplicate rows: {dup_ai}")
report.append("")

# BUSINESS TABLE
report.append("=== GOLD BUSINESS PERFORMANCE ===")
report.append(f"Row count: {df_business.count()}")

null_business = df_business.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_business.columns
]).collect()[0]

report.append("Null values:")
for c in df_business.columns:
    report.append(f"{c}: {null_business[c]}")

dup_business = df_business.count() - df_business.dropDuplicates().count()
report.append(f"Duplicate rows: {dup_business}")

# Check logic AOV
invalid_aov = df_business.filter(col("AOV") <= 0).count()
report.append(f"AOV <= 0 rows: {invalid_aov}")
report.append("")

# PRODUCT TABLE
report.append("=== GOLD PRODUCT CATALOG ===")
report.append(f"Row count: {df_product.count()}")

null_product = df_product.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_product.columns
]).collect()[0]

report.append("Null values:")
for c in df_product.columns:
    report.append(f"{c}: {null_product[c]}")

dup_product = df_product.count() - df_product.dropDuplicates().count()
report.append(f"Duplicate rows: {dup_product}")

# 6. FILE REPORT
output_path = "/opt/hadoop/audit/gold_report.txt"

with open(output_path, "w") as f:
    for line in report:
        f.write(line + "\n")

print("Audit completed. Report saved at:", output_path)

spark.stop()