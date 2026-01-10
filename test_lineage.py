from lineage.parser import LineageGraph

# Load lineage
lineage = LineageGraph()

print("=== Testing Lineage Graph ===\n")

# Test upstream traversal
print("1. Upstream dependencies of revenue_daily:")
upstream = lineage.get_all_upstream('revenue_daily')
print(f"   {upstream}\n")

# Test downstream traversal
print("2. Downstream dependencies of orders_raw:")
downstream = lineage.get_all_downstream('orders_raw')
print(f"   {downstream}\n")

# Test critical columns
print("3. Critical columns for revenue_daily:")
critical = lineage.get_critical_columns('revenue_daily')
print(f"   {critical}\n")

# Visualize full graph
lineage.visualize()