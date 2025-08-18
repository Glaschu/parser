import re
import sqlparse
from sqllineage.runner import LineageRunner

# Read the SQL file
with open("test.sql", "r") as f:
    sql = f.read()

# 1. Extract the core body of the procedure
sql_body = re.search(r"AS\s*BEGIN(.*)END\s*GO", sql, re.DOTALL | re.IGNORECASE)
if sql_body:
    sql = sql_body.group(1)

# 2. Deterministically find all CTE names
cte_names = set(re.findall(r"WITH\s+([a-zA-Z0-9_]+)\s+AS", sql, re.IGNORECASE))

# 3. Split into individual statements
statements = sqlparse.split(sql)

# 4. Filter for DML statements that are relevant to lineage
supported_types = ["INSERT", "UPDATE", "MERGE"]
dml_statements = []
for stmt_str in statements:
    if stmt_str.strip():
        parsed_stmt = sqlparse.parse(stmt_str)[0]
        first_token_norm = parsed_stmt.tokens[0].normalized
        if parsed_stmt.get_type() in supported_types or first_token_norm == 'WITH':
            dml_statements.append(stmt_str)

# 5. Analyze statement by statement and aggregate lineage
all_lineage_tuples = []
for stmt_group in dml_statements:
    with_clauses = [s for s in dml_statements if s.strip().upper().startswith('WITH')]
    if not stmt_group.strip().upper().startswith('WITH'):
        sql_to_parse = "\n".join(with_clauses) + "\n" + stmt_group
    else:
        sql_to_parse = stmt_group

    sql_to_parse = re.sub(r"@[a-zA-Z_]+", "'sample_value'", sql_to_parse)
    
    try:
        result = LineageRunner(sql_to_parse, dialect="tsql")
        all_lineage_tuples.extend(result.get_column_lineage())
    except Exception:
        continue

# 6. Build the lineage map from raw tuples
lineage_map = {}
for lineage_tuple in all_lineage_tuples:
    if lineage_tuple and len(lineage_tuple) > 1:
        target = lineage_tuple[-1]
        source = lineage_tuple[0]
        if source and target:
            lineage_map[target] = source

def is_intermediate(column, defined_cte_names):
    if not hasattr(column, 'parent') or not hasattr(column.parent, 'raw_name'):
        return True
    table_name = column.parent.raw_name
    return table_name.startswith("#") or table_name in defined_cte_names or table_name.endswith("work")

def find_original_source(column, defined_cte_names, visited=None):
    if visited is None:
        visited = set()
    if column in visited:
        return column
    visited.add(column)

    source = lineage_map.get(column)
    if source and is_intermediate(source, defined_cte_names):
        return find_original_source(source, defined_cte_names, visited)
    return source or column

final_lineage = {}
for target, source in lineage_map.items():
    if not is_intermediate(target, cte_names):
        original_source = find_original_source(source, cte_names)
        if original_source and not is_intermediate(original_source, cte_names):
            final_lineage[str(target)] = str(original_source)

# 7. Print the final results
print("--- End-to-End Column Lineage ---")
for target, source in sorted(final_lineage.items()):
    print(f"{target} <-- {source}")