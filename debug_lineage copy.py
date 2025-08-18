from sqllineage.runner import LineageRunner

# Simple test to see what's available
test_sql = "SELECT accountno, amount FROM staging.transactions"

result = LineageRunner(test_sql, dialect="tsql")

print("Available attributes and methods:")
for attr in sorted(dir(result)):
    if not attr.startswith('_'):
        try:
            value = getattr(result, attr)
            if callable(value):
                print(f"  {attr}() - method")
            else:
                print(f"  {attr} = {type(value)} - {value}")
        except Exception as e:
            print(f"  {attr} - error: {e}")

print("\nTesting column lineage:")
try:
    lineage = result.get_column_lineage()
    print(f"Column lineage: {lineage}")
    if lineage:
        for i, item in enumerate(lineage):
            print(f"  {i}: {item} (type: {type(item)})")
except Exception as e:
    print(f"Error getting column lineage: {e}")

print("\nTesting table methods:")
try:
    source_tables = result.source_tables
    print(f"Source tables: {source_tables}")
    target_tables = result.target_tables
    print(f"Target tables: {target_tables}")
except Exception as e:
    print(f"Error getting tables: {e}")
