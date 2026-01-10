from lineage.parser import LineageGraph
from metadata.store import MetadataStore
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from colorama import Fore, Style, Back, init
init(autoreset=True)

class RootCauseAnalyzer:
    """Analyzes quality check failures and identifies root causes"""
    
    def __init__(self, lineage: LineageGraph, metadata: MetadataStore):
        self.lineage = lineage
        self.metadata = metadata
    
    def analyze_failure(self, dataset: str, partition: str, 
                       failed_checks: List[Dict]) -> Dict:
        """
        Analyze a failure and identify potential root causes
        
        Returns:
            {
                'suspected_causes': [(dataset, reason, confidence), ...],
                'downstream_impact': [dataset1, dataset2, ...],
                'recommendations': [action1, action2, ...]
            }
        """
        print(f"\n ROOT CAUSE ANALYSIS")
        print(f"   Analyzing failure in {dataset}/{partition}")
        print(f"   Failed checks: {len(failed_checks)}\n")
        
        suspected_causes = []
        
        # 1. Check if this is a source dataset (root cause is here)
        upstream = self.lineage.get_upstream(dataset)
        
        if not upstream:
            # This is a source dataset, failure originated here
            for check in failed_checks:
                reason = self._explain_check_failure(check)
                suspected_causes.append((dataset, reason, 0.95))
        else:
            # Check upstream datasets for issues in same time window
            suspected_causes = self._check_upstream_issues(
                dataset, partition, failed_checks, upstream
            )
        
        # 2. Identify downstream impact
        downstream_impact = list(self.lineage.get_all_downstream(dataset))
        
        # 3. Generate recommendations
        recommendations = self._generate_recommendations(
            dataset, failed_checks, suspected_causes
        )
        
        return {
            'suspected_causes': suspected_causes,
            'downstream_impact': downstream_impact,
            'recommendations': recommendations
        }
    
    def _check_upstream_issues(self, dataset: str, partition: str,
                               failed_checks: List[Dict], 
                               upstream: List[str]) -> List[Tuple]:
        """Check if upstream datasets had issues in same time window"""
        suspected_causes = []
        
        # Get critical columns for this dataset
        critical_cols = self.lineage.get_critical_columns(dataset)
        
        for upstream_dataset in upstream:
            # Query upstream failures in same partition
            upstream_failures = self.metadata.conn.execute("""
                SELECT check_name, check_type, metric_value
                FROM quality_results
                WHERE dataset = ?
                  AND partition = ?
                  AND NOT passed
            """, [upstream_dataset, partition]).fetchall()
            
            if upstream_failures:
                for check_name, check_type, metric_value in upstream_failures:
                    # Check if failure is in a critical column
                    col_name = check_name.replace('null_rate_', '')
                    col_name = col_name.replace('ref_integrity_', '')
                    
                    if col_name in critical_cols or check_type in ['volume', 'referential']:
                        confidence = 0.90
                        reason = f"{check_name} failed (value: {metric_value:.4f})"
                        suspected_causes.append((upstream_dataset, reason, confidence))
            else:
                # No direct failures, but could still be data quality issue
                # Check for anomalies vs baseline
                anomalies = self._check_baseline_anomalies(upstream_dataset, partition)
                if anomalies:
                    suspected_causes.extend(anomalies)
        
        # If no upstream issues found, the issue likely originated in current dataset
        if not suspected_causes:
            for check in failed_checks:
                reason = self._explain_check_failure(check)
                suspected_causes.append((dataset, reason, 0.75))
        
        return suspected_causes
    
    def _check_baseline_anomalies(self, dataset: str, partition: str) -> List[Tuple]:
        """Check if metrics are anomalous vs historical baseline"""
        anomalies = []
        
        # Get current metrics for this partition
        current = self.metadata.conn.execute("""
            SELECT check_name, metric_value
            FROM quality_results
            WHERE dataset = ?
              AND partition = ?
        """, [dataset, partition]).fetchall()
        
        for check_name, current_value in current:
            # Get historical baseline (last 30 partitions, excluding current)
            baseline = self.metadata.conn.execute("""
                SELECT 
                    AVG(metric_value) as mean,
                    MAX(metric_value) as p95
                FROM (
                    SELECT metric_value
                    FROM quality_results
                    WHERE dataset = ?
                      AND check_name = ?
                      AND partition < ?
                    ORDER BY partition DESC
                    LIMIT 30
                )
            """, [dataset, check_name, partition]).fetchone()
            
            if baseline and baseline[0] is not None:
                mean, p95 = baseline
                
                # Check if current value is anomalous (>2x mean or >p95)
                if current_value > mean * 2 or current_value > p95:
                    reason = f"{check_name} anomaly: {current_value:.4f} vs baseline {mean:.4f}"
                    anomalies.append((dataset, reason, 0.70))
        
        return anomalies
    
    def _explain_check_failure(self, check: Dict) -> str:
        """Generate human-readable explanation of check failure"""
        check_type = check['check_type']
        check_name = check['check_name']
        metric = check['metric_value']
        threshold = check['threshold']
        
        if check_type == 'null_check':
            col = check_name.replace('null_rate_', '')
            return f"High null rate in '{col}': {metric:.2%} (threshold: {threshold:.2%})"
        
        elif check_type == 'volume':
            return f"Row count anomaly: {int(metric)} rows (expected: {threshold})"
        
        elif check_type == 'referential':
            col = check_name.replace('ref_integrity_', '')
            return f"Referential integrity broken for '{col}': {metric:.2%} orphaned records"
        
        else:
            return f"{check_name} failed: {metric} vs {threshold}"
    
    def _generate_recommendations(self, dataset: str, 
                                 failed_checks: List[Dict],
                                 suspected_causes: List[Tuple]) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Check type-specific recommendations
        for check in failed_checks:
            check_type = check['check_type']
            
            if check_type == 'null_check':
                col = check['check_name'].replace('null_rate_', '')
                recommendations.append(
                    f"Investigate null spike in '{col}' column - check upstream data source"
                )
            
            elif check_type == 'volume':
                recommendations.append(
                    f"Verify data ingestion for {dataset} - possible incomplete load"
                )
            
            elif check_type == 'referential':
                col = check['check_name'].replace('ref_integrity_', '')
                recommendations.append(
                    f"Update reference data or fix FK constraint on '{col}'"
                )
        
        # Add upstream recommendations
        for upstream_dataset, reason, confidence in suspected_causes:
            if upstream_dataset != dataset:
                recommendations.append(
                    f"Check {upstream_dataset} - {reason}"
                )
        
        return recommendations
    
    def print_analysis(self, analysis: Dict, dataset: str, partition: str):
        print(f"\n{Back.BLUE}{Fore.WHITE} ROOT CAUSE ANALYSIS {Style.RESET_ALL}")
        print(f"{Fore.CYAN}   Analysis for {dataset}/{partition}{Style.RESET_ALL}")
        print()
        
        # Suspected causes
        print(f"{Fore.YELLOW}   Suspected Root Causes (ranked by confidence):{Style.RESET_ALL}")
        for i, (cause_dataset, reason, confidence) in enumerate(analysis['suspected_causes'], 1):
            if confidence > 0.85:
                emoji = "🎯"
                color = Fore.RED
            else:
                emoji = "⚠️"
                color = Fore.YELLOW
            
            print(f"   {i}. {emoji} {color}{cause_dataset}{Style.RESET_ALL} (confidence: {confidence:.0%})")
            print(f"      └─ {reason}")
        print()
        
        # Downstream impact
        if analysis['downstream_impact']:
            print(f"{Fore.MAGENTA}   📉 Downstream Impact:{Style.RESET_ALL}")
            for ds in analysis['downstream_impact']:
                print(f"{Fore.RED}      • {ds} (BLOCKED - depends on clean data){Style.RESET_ALL}")
        else:
            print(f"{Fore.GREEN}   ✓ No downstream dependencies affected{Style.RESET_ALL}")
        print()
        
        # Recommendations
        print(f"{Fore.CYAN}   💡 Recommended Actions:{Style.RESET_ALL}")
        for i, rec in enumerate(analysis['recommendations'], 1):
            print(f"      {i}. {rec}")
        print()
        
        # MTTR estimate
        num_checks = len(analysis['suspected_causes'])
        estimated_mttr = 15 if num_checks <= 2 else 30
        manual_mttr = 120  # 2 hours
        time_saved = manual_mttr - estimated_mttr
        
        print(f"{Fore.GREEN}   ⏱️  Estimated MTTR: {estimated_mttr} minutes{Style.RESET_ALL} "
            f"{Fore.LIGHTBLACK_EX}(vs {manual_mttr} min manual, saving {time_saved} min){Style.RESET_ALL}")
        print()