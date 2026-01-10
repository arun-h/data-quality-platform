from pyspark.sql import SparkSession
from metadata.store import MetadataStore
from quality_checks.checks import QualityCheck
from lineage.parser import LineageGraph
from diagnostics.root_cause import RootCauseAnalyzer
import uuid
from datetime import datetime
import os
from colorama import Fore, Style, init
init(autoreset=True)

class QualityCheckRunner:
    """Orchestrates incremental quality checks with root cause analysis"""
    
    def __init__(self, enable_diagnostics=True):
        self.spark = SparkSession.builder \
            .appName("DataQuality") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.metadata = MetadataStore()
        
        # Initialize diagnostics
        self.enable_diagnostics = enable_diagnostics
        if enable_diagnostics:
            self.lineage = LineageGraph()
            self.root_cause_analyzer = RootCauseAnalyzer(self.lineage, self.metadata)
        
        print("✓ Quality Check Runner initialized")
        if enable_diagnostics:
            print("✓ Root cause diagnostics enabled\n")
        else:
            print()
    
    def discover_new_partitions(self, dataset_path, dataset_name):
        """Find partitions that haven't been checked yet"""
        last_checked = self.metadata.get_last_checkpoint(dataset_name)
        
        # List all partitions in the dataset
        partitions = []
        for item in sorted(os.listdir(dataset_path)):
            if item.startswith('dt='):
                partition_value = item.split('=')[1]
                # Only include partitions after last checkpoint
                if last_checked is None or partition_value > last_checked:
                    partitions.append(partition_value)
        
        return partitions
    
    def run_checks_for_partition(self, dataset_name, dataset_path, 
                                  partition, checks):
        """Run all checks for a single partition"""
        run_id = str(uuid.uuid4())
        
        # Read partition data
        partition_path = f"{dataset_path}/dt={partition}"
        df = self.spark.read.parquet(partition_path)
        
        results = []
        failed_checks = []
        
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
                
                # Track failures for diagnostics
                if not result['passed']:
                    failed_checks.append(result)
                
                # Visual feedback
                status = "✓" if result['passed'] else "✗"
                metric = result['metric_value']
                
                # Format metric based on check type
                if result['check_type'] == 'null_check':
                    metric_str = f"{metric:.4f}"
                elif result['check_type'] == 'volume':
                    metric_str = f"{int(metric)}"
                else:
                    metric_str = f"{metric:.4f}"
                
                if result['passed']:
                    status = f"{Fore.GREEN}✓{Style.RESET_ALL}"
                    print(f"  {status} {check.name}: {metric_str}")
                else:
                    status = f"{Fore.RED}✗{Style.RESET_ALL}"
                    print(f"  {status} {check.name}: {metric_str} {Fore.RED}⚠️  FAILED{Style.RESET_ALL}")

            except Exception as e:
                print(f"  ✗ {check.name}: ERROR - {e}")
                # Still record the failure
                failed_result = {
                    'run_id': run_id,
                    'dataset': dataset_name,
                    'partition': partition,
                    'check_name': check.name,
                    'check_type': check.check_type,
                    'passed': False,
                    'metric_value': -1.0,
                    'threshold': str(check.threshold) if hasattr(check, 'threshold') else 'N/A',
                    'timestamp': datetime.now()
                }
                results.append(failed_result)
                failed_checks.append(failed_result)
        
        # Save results to metadata store
        self.metadata.save_results(results)
        
        # Update checkpoint for this dataset
        self.metadata.update_checkpoint(dataset_name, partition)
        
        # Run root cause analysis if there are failures
        if failed_checks and self.enable_diagnostics:
            analysis = self.root_cause_analyzer.analyze_failure(
                dataset_name, partition, failed_checks
            )
            self.root_cause_analyzer.print_analysis(analysis, dataset_name, partition)
        
        return results
    
    def run(self, dataset_config):
        """Run checks for all configured datasets"""
        total_checks = 0
        total_passed = 0
        total_failed = 0
        
        for dataset_name, config in dataset_config.items():
            print(f"=== Checking {dataset_name} ===")
            
            # Discover new partitions
            new_partitions = self.discover_new_partitions(
                config['path'], 
                dataset_name
            )
            
            if not new_partitions:
                print(f"  No new partitions to check\n")
                continue
            
            print(f"  Found {len(new_partitions)} new partition(s)")
            print(f"  Running {len(config['checks'])} check(s) per partition\n")
            
            # Process each partition
            for i, partition in enumerate(new_partitions, 1):
                print(f"  [{i}/{len(new_partitions)}] Partition: {partition}")
                
                results = self.run_checks_for_partition(
                    dataset_name,
                    config['path'],
                    partition,
                    config['checks']
                )
                
                # Count results
                for result in results:
                    total_checks += 1
                    if result['passed']:
                        total_passed += 1
                    else:
                        total_failed += 1
                
                print()  # Blank line between partitions
            
            print()  # Blank line between datasets
        
        # Print summary
        print("=" * 60)
        print(f"{Fore.CYAN}=== Quality Check Summary ==={Style.RESET_ALL}")
        print(f"Total checks executed: {total_checks}")
        print(f"{Fore.GREEN}Passed: {total_passed}{Style.RESET_ALL}")
        if total_failed > 0:
            print(f"{Fore.RED}Failed: {total_failed}{Style.RESET_ALL}")
            print(f"\n{Fore.YELLOW}⚠️  {total_failed} failures detected - see diagnostics above{Style.RESET_ALL}")
        else:
            print(f"{Fore.GREEN}Failed: 0{Style.RESET_ALL}")
        print("=" * 60)
        
        return {
            'total_checks': total_checks,
            'passed': total_passed,
            'failed': total_failed
        }
    
    def close(self):
        """Clean up resources"""
        self.metadata.close()
        self.spark.stop()