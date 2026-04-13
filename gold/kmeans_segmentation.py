from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler # Bổ sung để sửa lỗi Vectorization
from pyspark.sql.functions import col, when
import os

# =============================================================================
# 1. CẤU HÌNH VÀ KHỞI TẠO SPARK SESSION
# =============================================================================
account_name = "nhom5storage"
azure_key = os.environ.get("AZURE_STORAGE_ACCESS_KEY") 

# Thiết lập Spark Session tích hợp cấu hình kết nối Azure ADLS Gen2
spark = SparkSession.builder \
    .appName("Gold_KMeans_Segmentation") \
    .config(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", azure_key) \
    .getOrCreate()

# =============================================================================
# 2. ĐỌC DỮ LIỆU TỪ TẦNG GOLD (ĐÃ QUA TIỀN XỬ LÝ)
# =============================================================================
# Truy xuất tập dữ liệu từ Delta Lake
path_input = f"abfss://gold@{account_name}.dfs.core.windows.net/gold_customers_ai"
df_gold = spark.read.format("delta").load(path_input)

# =============================================================================
# 3. CHUẨN BỊ VECTOR ĐẶC TRƯNG (FIX LỖI: scaled_features does not exist)
# =============================================================================
# Spark MLlib yêu cầu các cột input phải được gom lại thành 1 cột Vector duy nhất
feature_cols = ["Recency_Scaled", "Frequency_Scaled", "Monetary_Scaled"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="scaled_features")

# Chuyển đổi dữ liệu sang dạng Vector
df_gold_vector = assembler.transform(df_gold)

# =============================================================================
# 4. HUẤN LUYỆN MÔ HÌNH HỌC MÁY (K-MEANS)
# =============================================================================
# Sử dụng df_gold_vector làm đầu vào thay vì df_gold
kmeans = KMeans(featuresCol="scaled_features", predictionCol="Cluster", k=4, seed=42)
model = kmeans.fit(df_gold_vector)
df_result = model.transform(df_gold_vector)

# =============================================================================
# 5. LOGIC PHÂN ĐOẠN KHÁCH HÀNG (CUSTOMER SEGMENTATION)
# =============================================================================
df_final = df_result.withColumn("Customer_Segment", 
    when(col("Cluster") == 0, "VIP")
    .when(col("Cluster") == 1, "Loyal")
    .when(col("Cluster") == 2, "Potential")
    .otherwise("At Risk")
)

# =============================================================================
# 6. LƯU TRỮ KẾT QUẢ VÀ CUNG CẤP DỮ LIỆU CHO LỚP PHÂN TÍCH (BI)
# =============================================================================
output_path = f"abfss://gold@{account_name}.dfs.core.windows.net/customer_segmentation_results"
df_final.select("CustomerID", "Recency", "Frequency", "Monetary", "Customer_Segment") \
    .write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

print("--- [HỆ THỐNG]: Tiến trình phân cụm K-Means đã hoàn tất thành công ---")
spark.stop()