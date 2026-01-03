
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("GenerateRevenue").getOrCreate()

orders = spark.read.parquet("data/orders_raw")

# Aggregate by date and product
revenue = orders.groupBy("dt", "product_id").agg(
    F.sum("amount").alias("total_revenue"),
    F.count("order_id").alias("order_count")
)

# Write partitioned by date
revenue.write.partitionBy("dt").parquet("data/revenue_daily", mode="overwrite")

print(f"Generated revenue data with {revenue.count()} records")

spark.stop()