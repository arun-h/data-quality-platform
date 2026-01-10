from pyspark.sql import SparkSession
from quality_checks.checks import NullRateCheck, VolumeAnomalyCheck, ReferentialIntegrityCheck

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestChecks") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=== Testing Quality Checks ===\n")

# Load test data
orders = spark.read.parquet("data/orders_raw/dt=2024-10-01")
products = spark.read.parquet("data/products")

print(f"Test partition: {orders.count()} rows")
print(f"Products: {products.count()} items\n")

# Test 1: Null Rate Check
print("Test 1: Null Rate Check")
null_check = NullRateCheck('product_id', threshold=0.05)
result = null_check.run(orders)
print(f"  Check: {result['check_name']}")
print(f"  Passed: {result['passed']}")
print(f"  Null rate: {result['metric_value']:.4f}")
print(f"  Threshold: {result['threshold']}\n")

# Test 2: Volume Anomaly Check
print("Test 2: Volume Anomaly Check")
vol_check = VolumeAnomalyCheck(expected_min=10000, expected_max=20000)
result = vol_check.run(orders)
print(f"  Check: {result['check_name']}")
print(f"  Passed: {result['passed']}")
print(f"  Row count: {int(result['metric_value'])}")
print(f"  Expected range: {result['threshold']}\n")

# Test 3: Referential Integrity Check
print("Test 3: Referential Integrity Check")
ref_check = ReferentialIntegrityCheck('product_id', products, 'product_id')
result = ref_check.run(orders)
print(f"  Check: {result['check_name']}")
print(f"  Passed: {result['passed']}")
print(f"  Orphan rate: {result['metric_value']:.4f}")
print(f"  Threshold: {result['threshold']}\n")

print(" All checks executed successfully!")

spark.stop()