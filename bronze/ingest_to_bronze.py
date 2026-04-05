import os
from pyspark.sql import SparkSession

# Lấy Key từ biến môi trường 
azure_storage_key = os.environ.get("AZURE_STORAGE_ACCESS_KEY")
account_name = "nhom5storage"

# Khởi tạo Spark Session với cấu hình Key trực tiếp
spark = SparkSession.builder \
    .appName("Retail_Data_Ingestion_To_Cloud") \
    .config(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", azure_storage_key) \
    .config(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "SharedKey") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS Local
hdfs_path = "hdfs://nhom5-master:9000/user/retail/raw/online_retail_II.csv" 
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Ghi dữ liệu lên Azure Bronze
azure_path = f"abfss://bronze@{account_name}.dfs.core.windows.net/raw_retail_data.parquet" 

# Thực hiện ghi file
df.write.mode("overwrite").parquet(azure_path)

spark.stop()