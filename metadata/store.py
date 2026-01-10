import duckdb
from datetime import datetime
import os

class MetadataStore:
    def __init__(self, db_path="metadata/quality.db"):
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.conn = duckdb.connect(db_path)
        self._init_schema()
    
    def _init_schema(self):
        """Create tables if they don't exist"""
        # Quality results table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_results (
                run_id VARCHAR,
                dataset VARCHAR,
                partition VARCHAR,
                check_name VARCHAR,
                check_type VARCHAR,
                passed BOOLEAN,
                metric_value DOUBLE,
                threshold VARCHAR,
                timestamp TIMESTAMP,
                PRIMARY KEY (run_id, dataset, partition, check_name)
            )
        """)
        
        # Checkpoint tracking table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS partition_checkpoints (
                dataset VARCHAR PRIMARY KEY,
                last_checked_partition VARCHAR,
                last_check_time TIMESTAMP
            )
        """)
        
        print("✓ Metadata store initialized")
    
    def save_results(self, results):
        """Save quality check results"""
        if not results:
            return
        
        for result in results:
            self.conn.execute("""
                INSERT OR REPLACE INTO quality_results 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                result['run_id'],
                result['dataset'],
                result['partition'],
                result['check_name'],
                result['check_type'],
                result['passed'],
                result['metric_value'],
                str(result['threshold']),
                result['timestamp']
            ])
        
        self.conn.commit()
    
    def get_last_checkpoint(self, dataset):
        """Get the last checked partition for a dataset"""
        result = self.conn.execute("""
            SELECT last_checked_partition 
            FROM partition_checkpoints 
            WHERE dataset = ?
        """, [dataset]).fetchone()
        
        return result[0] if result else None
    
    def update_checkpoint(self, dataset, partition):
        """Update the last checked partition"""
        self.conn.execute("""
            INSERT OR REPLACE INTO partition_checkpoints 
            VALUES (?, ?, ?)
        """, [dataset, partition, datetime.now()])
        
        self.conn.commit()
    
    def get_summary(self):
        """Get summary statistics"""
        return self.conn.execute("""
            SELECT 
                dataset,
                COUNT(*) as total_checks,
                SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failed
            FROM quality_results
            GROUP BY dataset
            ORDER BY dataset
        """).df()
    
    def get_failed_checks(self):
        """Get all failed checks"""
        return self.conn.execute("""
            SELECT 
                dataset,
                partition,
                check_name,
                metric_value,
                threshold,
                timestamp
            FROM quality_results
            WHERE NOT passed
            ORDER BY timestamp DESC
        """).df()
    
    def close(self):
        """Close database connection"""
        self.conn.close()