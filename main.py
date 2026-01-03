
from quality_checks.runner import QualityCheckRunner
from quality_checks.checks import *
from pyspark.sql import SparkSession

def main():
    # Load reference data
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    products_df = spark.read.parquet("data/products")
    
    # Define checks for each dataset
    dataset_config = {
        'orders_raw': {
            'path': 'data/orders_raw',
            'checks': [
                NullRateCheck('order_id', threshold=0.0),
                NullRateCheck('product_id', threshold=0.05),
                NullRateCheck('amount', threshold=0.05),
                VolumeAnomalyCheck(expected_min=10000, expected_max=25000),
                ReferentialIntegrityCheck('product_id', products_df, 'product_id')
            ]
        },
        'revenue_daily': {
            'path': 'data/revenue_daily',
            'checks': [
                NullRateCheck('total_revenue', threshold=0.0),
                VolumeAnomalyCheck(expected_min=500, expected_max=1500),
            ]
        }
    }
    
    runner = QualityCheckRunner()
    runner.run(dataset_config)
    
    print("\n=== Quality Check Summary ===")
    # Query and display results
    runner.metadata.conn.execute("""
        SELECT 
            dataset,
            COUNT(*) as total_checks,
            SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failed
        FROM quality_results
        GROUP BY dataset
    """).df()

if __name__ == "__main__":
    main()