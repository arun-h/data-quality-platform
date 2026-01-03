from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class QualityCheck:
    def __init__(self, name, check_type):
        self.name = name
        self.check_type = check_type
    
    def run(self, df: DataFrame) -> dict:
        raise NotImplementedError

class NullRateCheck(QualityCheck):
    def __init__(self, column, threshold=0.05):
        super().__init__(f"null_rate_{column}", "null_check")
        self.column = column
        self.threshold = threshold
    
    def run(self, df: DataFrame) -> dict:
        total = df.count()
        null_count = df.filter(F.col(self.column).isNull()).count()
        null_rate = null_count / total if total > 0 else 0
        
        return {
            'check_name': self.name,
            'check_type': self.check_type,
            'passed': null_rate <= self.threshold,
            'metric_value': null_rate,
            'threshold': self.threshold
        }

class VolumeAnomalyCheck(QualityCheck):
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
            'metric_value': count,
            'threshold': f"{self.expected_min}-{self.expected_max}"
        }

class ReferentialIntegrityCheck(QualityCheck):
    def __init__(self, foreign_key, reference_df, reference_key):
        super().__init__(f"ref_integrity_{foreign_key}", "referential")
        self.foreign_key = foreign_key
        self.reference_df = reference_df
        self.reference_key = reference_key
    
    def run(self, df: DataFrame) -> dict:
        # Left anti join to find orphaned records
        orphaned = df.join(
            self.reference_df,
            df[self.foreign_key] == self.reference_df[self.reference_key],
            "left_anti"
        ).count()
        
        total = df.count()
        orphan_rate = orphaned / total if total > 0 else 0
        
        return {
            'check_name': self.name,
            'check_type': self.check_type,
            'passed': orphan_rate == 0,
            'metric_value': orphan_rate,
            'threshold': 0.0
        }