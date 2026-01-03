
import duckdb

class MetadataStore:
    def __init__(self, db_path="metadata/quality.db"):
        self.conn = duckdb.connect(db_path)
        self._init_schema()
    
    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_results (
                run_id VARCHAR,
                dataset VARCHAR,
                partition VARCHAR,
                check_name VARCHAR,
                check_type VARCHAR,
                passed BOOLEAN,
                metric_value DOUBLE,
                threshold DOUBLE,
                timestamp TIMESTAMP,
                PRIMARY KEY (run_id, dataset, partition, check_name)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS partition_checkpoints (
                dataset VARCHAR PRIMARY KEY,
                last_checked_partition VARCHAR,
                last_check_time TIMESTAMP
            )
        """)
    
    def save_results(self, results):
        """Insert quality check results"""
        # Implement insert logic
        pass
    
    def get_last_checkpoint(self, dataset):
        """Get last checked partition for a dataset"""
        result = self.conn.execute("""
            SELECT last_checked_partition 
            FROM partition_checkpoints 
            WHERE dataset = ?
        """, [dataset]).fetchone()
        return result[0] if result else None
    
    def update_checkpoint(self, dataset, partition):
        """Update last checked partition"""
        self.conn.execute("""
            INSERT OR REPLACE INTO partition_checkpoints 
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, [dataset, partition])