from metadata.store import MetadataStore
import pandas as pd

def main():
    store = MetadataStore()
    
    print("=" * 60)
    print("Quality Check Results")
    print("=" * 60)
    print()
    
    # Overall summary
    print("=== Summary by Dataset ===")
    summary = store.get_summary()
    print(summary.to_string(index=False))
    print()
    
    # Check what's been processed
    checkpoints = store.conn.execute("""
        SELECT 
            dataset,
            last_checked_partition,
            last_check_time
        FROM partition_checkpoints
        ORDER BY dataset
    """).df()
    
    print("=== Last Checked Partitions ===")
    print(checkpoints.to_string(index=False))
    print()
    
    # Recent check results
    recent = store.conn.execute("""
        SELECT 
            dataset,
            partition,
            check_name,
            passed,
            metric_value,
            timestamp
        FROM quality_results
        ORDER BY timestamp DESC
        LIMIT 10
    """).df()
    
    print("=== Recent Check Results (Last 10) ===")
    print(recent.to_string(index=False))
    print()
    
    # Failed checks
    failed = store.get_failed_checks()
    if len(failed) > 0:
        print("=== Failed Checks ===")
        print(failed.to_string(index=False))
    else:
        print("✓ No failed checks found!")
    
    store.close()

if __name__ == "__main__":
    main()