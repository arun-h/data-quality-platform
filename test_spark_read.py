
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataQualityTest") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("=== Reading single partition of orders===")
df = spark.read.parquet("data/orders_raw/dt=2024-10-01")
print(f"Rows: {df.count()}")
df.show(5)

print("\n=== Reading all partitions ===")
df_all = spark.read.parquet("data/orders_raw")
print(f"Total rows: {df_all.count()}")

print("\n=== Reading products ===")
products = spark.read.parquet("data/products")
print(f"Products: {products.count()}")
products.show(5)

spark.stop()