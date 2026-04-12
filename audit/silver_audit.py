from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# INIT SPARK
spark = SparkSession.builder \
    .appName("Silver Layer Audit") \
    .getOrCreate()

print("===== SILVER LAYER AUDIT STARTED =====")

# LOAD DATA
df = spark.read.csv("file:///opt/hadoop/silver", header=True, inferSchema=True)

# 1. ROW COUNT
row_count = df.count()
print(f"Total Rows: {row_count}")

# 2. DUPLICATE CHECK
dup_df = df.groupBy(df.columns).count().filter("count > 1")
dup_count = dup_df.count()

print(f"Duplicate Rows: {dup_count}")

if dup_count > 0:
    print("Example duplicate records:")
    dup_df.show(3, truncate=False)

# 3. NULL CHECK
null_count = 0
if "CustomerID" in df.columns:
    null_rows = df.filter(col("CustomerID").isNull())
    null_count = null_rows.count()

    if null_count > 0:
        print("Example NULL records:")
        null_rows.show(3, truncate=False)

print(f"Null Rows (CustomerID): {null_count}")

# 4. NEGATIVE CHECK
neg_count = 0
if "Quantity" in df.columns:
    neg_rows = df.filter(col("Quantity") < 0)
    neg_count = neg_rows.count()

    if neg_count > 0:
        print("Example negative records:")
        neg_rows.show(3, truncate=False)

print(f"Negative Quantity Rows: {neg_count}")

# SAVE REPORT
report_path = "/opt/hadoop/audit/silver_report.txt"

with open(report_path, "w") as f:
    f.write("SILVER LAYER AUDIT REPORT\n")
    f.write("===========================\n")
    f.write(f"Total Rows: {row_count}\n")
    f.write(f"Duplicate Rows: {dup_count}\n")
    f.write(f"Null Rows (CustomerID): {null_count}\n")
    f.write(f"Negative Quantity Rows: {neg_count}\n")

print(f"Report saved to: {report_path}")
print("===== AUDIT COMPLETED =====")

spark.stop()