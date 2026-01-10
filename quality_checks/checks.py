from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class QualityCheck:
    """Base class for quality checks"""
    def __init__(self, name, check_type):
        self.name = name
        self.check_type = check_type
    
    def run(self, df: DataFrame) -> dict:
        """Execute the check and return results"""
        raise NotImplementedError("Subclasses must implement run()")


class NullRateCheck(QualityCheck):
    """Check for null values in a column"""
    
    def __init__(self, column, threshold=0.05):
        super().__init__(f"null_rate_{column}", "null_check")
        self.column = column
        self.threshold = threshold
    
    def run(self, df: DataFrame) -> dict:
        total = df.count()
        
        if total == 0:
            return {
                'check_name': self.name,
                'check_type': self.check_type,
                'passed': True,
                'metric_value': 0.0,
                'threshold': self.threshold
            }
        
        null_count = df.filter(F.col(self.column).isNull()).count()
        null_rate = null_count / total
        
        return {
            'check_name': self.name,
            'check_type': self.check_type,
            'passed': null_rate <= self.threshold,
            'metric_value': null_rate,
            'threshold': self.threshold
        }


class VolumeAnomalyCheck(QualityCheck):
    """Check if row count is within expected range"""
    
    def __init__(self, expected_min, expected_max):
        super().__init__("volume_check", "volume")
        self.expected_min = expected_min
        self.expected_max = expected_max
    
    def run(self, df: DataFrame) -> dict:
        count = df.count()
        passed = self.expected_min <= count <= self.expected_max
        
        return {
            'check_name': self.name,
            'check_type': self.check_type,
            'passed': passed,
            'metric_value': float(count),
            'threshold': f"{self.expected_min}-{self.expected_max}"
        }


class ReferentialIntegrityCheck(QualityCheck):
    """Check foreign key relationships"""
    
    def __init__(self, foreign_key, reference_df, reference_key):
        super().__init__(f"ref_integrity_{foreign_key}", "referential")
        self.foreign_key = foreign_key
        self.reference_df = reference_df
        self.reference_key = reference_key
    
    def run(self, df: DataFrame) -> dict:
        # Find orphaned records using left anti join
        orphaned = df.join(
            self.reference_df,
            df[self.foreign_key] == self.reference_df[self.reference_key],
            "left_anti"
        ).count()
        
        total = df.count()
        orphan_rate = orphaned / total if total > 0 else 0.0
        
        return {
            'check_name': self.name,
            'check_type': self.check_type,
            'passed': orphan_rate == 0.0,
            'metric_value': orphan_rate,
            'threshold': 0.0
        }