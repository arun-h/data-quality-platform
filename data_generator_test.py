import os
import pandas as pd

# Check partition structure
print("=== Partition Structure ===")
partitions = sorted([d for d in os.listdir('data/orders_raw') if d.startswith('dt=')])
print(f"Total partitions: {len(partitions)}")
print(f"First partition: {partitions[0]}")
print(f"Last partition: {partitions[-1]}")

# Check sample data
sample_partition = partitions[0]
df = pd.read_parquet(f"data/orders_raw/{sample_partition}/data.parquet")

print(f"\n=== Sample Data from {sample_partition} ===")
print(f"Rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print("\nFirst 3 rows:")
print(df.head(3))

print("\n=== Data Quality Spot Check ===")
print(f"Null counts:\n{df.isnull().sum()}")
print(f"\nProduct ID range: {df['product_id'].min()} to {df['product_id'].max()}")
print(f"Amount range: ${df['amount'].min():.2f} to ${df['amount'].max():.2f}")

# Total row count
total_rows = sum(len(pd.read_parquet(f"data/orders_raw/{p}/data.parquet")) for p in partitions)
print(f"\n=== Total Dataset ===")
print(f"Total rows across all partitions: {total_rows:,}")