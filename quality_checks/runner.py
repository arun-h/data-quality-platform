from pyspark.sql import SparkSession
from metadata.store import MetadataStore
from quality_checks.checks import *
import uuid
from datetime import datetime

class QualityCheckRunner:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataQuality") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        self.metadata = MetadataStore()
    
    def discover_new_partitions(self, dataset_path, dataset_name):
        """Find partitions that haven't been checked yet"""
        last_checked = self.metadata.get_last_checkpoint(dataset_name)
        
        # List all partitions in the dataset
        import os
        partitions = []
        for item in sorted(os.listdir(dataset_path)):
            if item.startswith('dt='):
                partition_value = item.split('=')[1]
                if last_checked is None or partition_value > last_checked:
                    partitions.append(partition_value)
        
        return partitions
    
    def run_checks_for_partition(self, dataset_name, dataset_path, 
                                  partition, checks):
        """Run all checks for a single partition"""
        run_id = str(uuid.uuid4())
        
        # Read partition data
        df = self.spark.read.parquet(f"{dataset_path}/dt={partition}")
        
        results = []
        for check in checks:
            try:
                result = check.run(df)
                result.update({
                    'run_id': run_id,
                    'dataset': dataset_name,
                    'partition': partition,
                    'timestamp': datetime.now()
                })
                results.append(result)
                
                status = "✓" if result['passed'] else "✗"
                print(f"{status} {dataset_name}/{partition}: {check.name} = {result['metric_value']}")
                
            except Exception as e:
                print(f"✗ Check failed: {check.name} - {e}")
        
        # Save results
        self.metadata.save_results(results)
        
        # Update checkpoint
        self.metadata.update_checkpoint(dataset_name, partition)
        
        return results
    
    def run(self, dataset_config):
        """Run checks for all datasets"""
        for dataset_name, config in dataset_config.items():
            print(f"\n=== Checking {dataset_name} ===")
            
            new_partitions = self.discover_new_partitions(
                config['path'], 
                dataset_name
            )
            
            if not new_partitions:
                print(f"No new partitions for {dataset_name}")
                continue
            
            print(f"Found {len(new_partitions)} new partitions")
            
            for partition in new_partitions:
                self.run_checks_for_partition(
                    dataset_name,
                    config['path'],
                    partition,
                    config['checks']
                )