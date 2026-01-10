"""
Inject controlled failures into data to demonstrate root cause diagnostics
"""
import pandas as pd
import os
import shutil

def backup_partition(dataset, partition):
    """Backup partition before injecting failures"""
    src = f"data/{dataset}/dt={partition}"
    backup = f"data/{dataset}/dt={partition}.backup"
    
    if os.path.exists(backup):
        print(f"  Backup already exists for {dataset}/{partition}")
        return
    
    shutil.copytree(src, backup)
    print(f"  ✓ Backed up {dataset}/{partition}")

def restore_partition(dataset, partition):
    """Restore partition from backup"""
    backup = f"data/{dataset}/dt={partition}.backup"
    target = f"data/{dataset}/dt={partition}"
    
    if not os.path.exists(backup):
        print(f"  No backup found for {dataset}/{partition}")
        return
    
    # Remove corrupted data
    shutil.rmtree(target)
    
    # Restore from backup
    shutil.copytree(backup, target)
    
    # Remove backup
    shutil.rmtree(backup)
    
    print(f"  ✓ Restored {dataset}/{partition}")

def inject_null_spike(dataset, partition, column, null_rate=0.30):
    """Inject null values into a specific column"""
    print(f"\n🔧 Injecting {null_rate:.0%} nulls in {dataset}/{partition}/{column}")
    
    # Backup first
    backup_partition(dataset, partition)
    
    # Load data
    path = f"data/{dataset}/dt={partition}/data.parquet"
    df = pd.read_parquet(path)
    
    original_count = len(df)
    original_nulls = df[column].isnull().sum()
    
    # Inject nulls
    mask = df.sample(frac=null_rate).index
    df.loc[mask, column] = None
    
    # Save
    df.to_parquet(path, index=False)
    
    new_nulls = df[column].isnull().sum()
    print(f"  Nulls in '{column}': {original_nulls} → {new_nulls} ({new_nulls/original_count:.2%})")

def inject_volume_drop(dataset, partition, keep_rate=0.30):
    """Reduce row count to simulate missing data"""
    print(f"\n🔧 Injecting volume drop in {dataset}/{partition} (keeping {keep_rate:.0%})")
    
    # Backup first
    backup_partition(dataset, partition)
    
    # Load data
    path = f"data/{dataset}/dt={partition}/data.parquet"
    df = pd.read_parquet(path)
    
    original_count = len(df)
    
    # Keep only a fraction
    df_small = df.sample(frac=keep_rate)
    
    # Save
    df_small.to_parquet(path, index=False)
    
    new_count = len(df_small)
    print(f"  Row count: {original_count} → {new_count} ({new_count/original_count:.2%})")

def inject_referential_break(dataset, partition, column, break_rate=0.15):
    """Add records with invalid foreign keys"""
    print(f"\n🔧 Injecting referential integrity break in {dataset}/{partition}/{column}")
    
    # Backup first
    backup_partition(dataset, partition)
    
    # Load data
    path = f"data/{dataset}/dt={partition}/data.parquet"
    df = pd.read_parquet(path)
    
    original_count = len(df)
    
    # Change some foreign keys to invalid values
    mask = df.sample(frac=break_rate).index
    df.loc[mask, column] = 99999  # Non-existent product
    
    # Save
    df.to_parquet(path, index=False)
    
    invalid_count = len(mask)
    print(f"  Invalid {column} references: {invalid_count} ({invalid_count/original_count:.2%})")

def restore_all():
    """Restore all backups"""
    print("\n🔄 Restoring all backups...")
    
    datasets = ['orders_raw', 'revenue_daily']
    
    for dataset in datasets:
        dataset_path = f"data/{dataset}"
        if not os.path.exists(dataset_path):
            continue
        
        for item in os.listdir(dataset_path):
            if item.endswith('.backup'):
                partition = item.replace('.backup', '').replace('dt=', '')
                restore_partition(dataset, partition)
    
    print("\n✓ All backups restored")

def main():
    print("=" * 60)
    print("Data Quality Failure Injection")
    print("=" * 60)
    
    # Scenario 1: Null spike in orders_raw causes downstream failure
    print("\n📋 Scenario 1: Upstream null spike")
    print("   orders_raw has null spike → revenue_daily fails")
    
    inject_null_spike('orders_raw', '2024-11-15', 'product_id', null_rate=0.35)
    
    # Scenario 2: Volume drop in orders_raw
    print("\n📋 Scenario 2: Upstream volume drop")
    print("   orders_raw has missing data → volume anomaly")
    
    inject_volume_drop('orders_raw', '2024-11-20', keep_rate=0.25)
    
    # Scenario 3: Referential integrity break
    print("\n📋 Scenario 3: Referential integrity break")
    print("   orders_raw references invalid products")
    
    inject_referential_break('orders_raw', '2024-11-25', 'product_id', break_rate=0.18)
    
    print("\n" + "=" * 60)
    print("✓ Failures injected successfully")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Delete metadata/quality.db to reset checkpoints")
    print("2. Run: python main.py")
    print("3. Watch root cause diagnostics in action!")
    print("\nTo restore clean data: python inject_failures.py --restore")

if __name__ == "__main__":
    import sys
    
    if '--restore' in sys.argv:
        restore_all()
    else:
        main()