from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, max, datediff, lit, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import year, month, quarter, dayofweek, countDistinct
import os

# Cấu hình xác thực Azure và tạo SparkSession
account_name = "nhom5storage"
azure_key = os.environ.get("AZURE_STORAGE_ACCESS_KEY") 

spark = SparkSession.builder \
    .appName("Gold_Layer_Final_Flattened") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", azure_key) \
    .getOrCreate()

# Đọc dữ liệu từ Silver 
df_silver = spark.read.parquet(f"abfss://silver@{account_name}.dfs.core.windows.net/cleaned_retail_data") \
    .withColumnRenamed("Customer ID", "CustomerID")

# Tính RFM
ref_date = df_silver.select(max("InvoiceDate")).collect()[0][0]
df_rfm = df_silver.groupBy("CustomerID").agg(
    datediff(lit(ref_date), max("InvoiceDate")).alias("Recency"),
    count("Invoice").alias("Frequency"),
    sum(col("Quantity") * col("Price")).alias("Monetary")
)

# Chuẩn hóa dữ liệu
assembler = VectorAssembler(inputCols=["Recency", "Frequency", "Monetary"], outputCol="features")
df_vector = assembler.transform(df_rfm)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
scaler_model = scaler.fit(df_vector)
df_scaled = scaler_model.transform(df_vector)

# Làm phẳng dữ liệu để dễ dàng sử dụng trong Power BI (tách cột Vector thành các cột riêng biệt)
def extract_from_vector(vector, index):
    return float(vector[index])

extract_udf = udf(extract_from_vector, DoubleType())

df_gold_final = df_scaled.withColumn("Recency_Scaled", extract_udf(col("scaled_features"), lit(0))) \
                         .withColumn("Frequency_Scaled", extract_udf(col("scaled_features"), lit(1))) \
                         .withColumn("Monetary_Scaled", extract_udf(col("scaled_features"), lit(2)))

# Ghi dữ liệu đã chuẩn hóa và làm phẳng vào Gold Layer
final_columns = ["CustomerID", "Recency", "Frequency", "Monetary", 
                 "Recency_Scaled", "Frequency_Scaled", "Monetary_Scaled"]

#Luu bảng phục vụ AI/ML
df_gold_final.select(*final_columns).write.format("delta").mode("overwrite") \
    .save(f"abfss://gold@{account_name}.dfs.core.windows.net/gold_customers_ai")

# Tính toán các chỉ số kinh doanh và lưu vào bảng phục vụ BI
df_gold_business = df_silver.groupBy("InvoiceDate", "Country").agg(
    sum(col("Quantity") * col("Price")).alias("Revenue"), # Tổng doanh thu
    sum("Quantity").alias("SalesVolume"),              # Tổng lượng hàng
    countDistinct("Invoice").alias("OrderCount"),      # Tổng số đơn hàng
    countDistinct("CustomerID").alias("CustomerCount") # Tổng số khách hàng
).withColumn("Year", year("InvoiceDate")) \
 .withColumn("Quarter", quarter("InvoiceDate")) \
 .withColumn("Month", month("InvoiceDate")) \
 .withColumn("DayOfWeek", dayofweek("InvoiceDate")) \
 .withColumn("AOV", col("Revenue") / col("OrderCount")) # Tính giá trị đơn hàng TB

df_gold_business.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"abfss://gold@{account_name}.dfs.core.windows.net/gold_business_performance")

spark.stop()