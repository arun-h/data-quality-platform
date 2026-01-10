import yaml
from typing import Dict, List, Set

class LineageGraph:
    """Represents data lineage relationships"""
    
    def __init__(self, config_path="lineage.yaml"):
        self.config_path = config_path
        self.datasets = {}
        self._load_config()
    
    def _load_config(self):
        """Load lineage configuration from YAML"""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.datasets = config.get('datasets', {})
        print(f"Loaded lineage for {len(self.datasets)} datasets")
    
    def get_upstream(self, dataset_name: str) -> List[str]:
        """Get immediate upstream dependencies"""
        if dataset_name not in self.datasets:
            return []
        
        return self.datasets[dataset_name].get('upstream', [])
    
    def get_all_upstream(self, dataset_name: str) -> Set[str]:
        """Get all upstream dependencies (recursive)"""
        upstream = set()
        to_process = [dataset_name]
        
        while to_process:
            current = to_process.pop()
            direct_upstream = self.get_upstream(current)
            
            for dep in direct_upstream:
                if dep not in upstream:
                    upstream.add(dep)
                    to_process.append(dep)
        
        return upstream
    
    def get_downstream(self, dataset_name: str) -> List[str]:
        """Get immediate downstream dependencies"""
        downstream = []
        
        for name, config in self.datasets.items():
            if dataset_name in config.get('upstream', []):
                downstream.append(name)
        
        return downstream
    
    def get_all_downstream(self, dataset_name: str) -> Set[str]:
        """Get all downstream dependencies (recursive)"""
        downstream = set()
        to_process = [dataset_name]
        
        while to_process:
            current = to_process.pop()
            direct_downstream = self.get_downstream(current)
            
            for dep in direct_downstream:
                if dep not in downstream:
                    downstream.add(dep)
                    to_process.append(dep)
        
        return downstream
    
    def get_critical_columns(self, dataset_name: str) -> List[str]:
        """Get columns critical for downstream processing"""
        if dataset_name not in self.datasets:
            return []
        
        return self.datasets[dataset_name].get('critical_columns', [])
    
    def visualize(self):
        """Print lineage graph"""
        print("\n=== Data Lineage Graph ===")
        for dataset, config in self.datasets.items():
            upstream = config.get('upstream', [])
            if upstream:
                print(f"{dataset}")
                print(f"  ⬆️  Depends on: {', '.join(upstream)}")
            else:
                print(f"{dataset} (source)")
            
            downstream = self.get_downstream(dataset)
            if downstream:
                print(f"  ⬇️  Feeds into: {', '.join(downstream)}")
            print()