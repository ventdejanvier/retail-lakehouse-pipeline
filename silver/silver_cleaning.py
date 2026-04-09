import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim

# =================================================================
# 1. THIẾT LẬP MÔI TRƯỜNG (FIX LỖI JAVA 21 & SCALA)
# =================================================================

os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-21'
os.environ['HADOOP_HOME'] = r'C:\hadoop-3.0.0'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['PATH']

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# =================================================================
# 2. CẤU HÌNH AZURE
# =================================================================
account_name = "nhom5storage"
azure_storage_key = os.getenv("AZURE_STORAGE_KEY")

# Đường dẫn ghi dữ liệu (Sử dụng Parquet để tránh lỗi Delta Class)
azure_silver_path = f"abfss://silver@{account_name}.dfs.core.windows.net/cleaned_retail_data"

# =================================================================
# 3. KHỞI TẠO SPARK SESSION (TỐI GIẢN ĐỂ CHẠY ỔN ĐỊNH)
# =================================================================
spark = SparkSession.builder \
    .appName("Retail_Data_Pipeline_Final") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", azure_storage_key) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# =================================================================
# 4. LUỒNG XỬ LÝ CHÍNH
# =================================================================
try:
    # 4.1: Đọc dữ liệu từ Bronze
    bronze_path = f"abfss://bronze@{account_name}.dfs.core.windows.net/raw_retail_data.parquet"
    print(f"\n--- Bước 1: Đang đọc dữ liệu từ Bronze: {bronze_path} ---")
    df_raw = spark.read.parquet(bronze_path)
    
    # 4.2: Silver Logic (Làm sạch và chuẩn hóa)
    print("--- Bước 2: Đang xử lý làm sạch và chuẩn hóa dữ liệu... ---")
    df_silver = df_raw \
        .dropDuplicates() \
        .withColumn("Description", trim(col("Description"))) \
        .dropna(subset=["Customer ID", "Invoice"]) \
        .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Quantity", col("Quantity").cast("int")) \
        .withColumn("Price", col("Price").cast("double")) \
        .filter(col("Quantity") > 0)

    # 4.3: Ghi dữ liệu (Dùng Parquet để Fabric Shortcut nhận diện nhanh nhất)
    print(f"--- Bước 3: Đang nạp dữ liệu vào Azure Silver tại: {azure_silver_path} ---")
    
    # Ghi đè (Overwrite) dữ liệu sạch lên Silver
    df_silver.write.mode("overwrite").parquet(azure_silver_path)

    print("\n--- THÀNH CÔNG ---")
    print("Dữ liệu đã nằm trên Azure Silver dưới dạng Parquet.")

except Exception as e:
    print(f"\n LỖI TRONG QUÁ TRÌNH XỬ LÝ: {str(e)}")

finally:
    spark.stop()
    print("--- Đã đóng kết nối Spark. ---")