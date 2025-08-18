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

# 2. Split into individual statements
statements = sqlparse.split(sql)

# 3. Filter for DML statements that are relevant to lineage
supported_types = ["INSERT", "UPDATE", "MERGE"]
dml_statements = []
for stmt_str in statements:
    if stmt_str.strip():
        parsed_stmt = sqlparse.parse(stmt_str)[0]
        first_token_norm = parsed_stmt.tokens[0].normalized
        if parsed_stmt.get_type() in supported_types or first_token_norm == 'WITH':
            dml_statements.append(stmt_str)

# 4. Analyze statement by statement and aggregate lineage
all_lineage_tuples = []
all_table_relations = []

# We need to collect CTE names for the is_intermediate function later
cte_names = set(re.findall(r"WITH\s+([a-zA-Z0-9_]+)\s+AS", sql, re.IGNORECASE))

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
        all_table_relations.extend(result.relations)
    except Exception:
        continue

# 5. Process and format source table information
source_table_info = {}

# Collect all unique source tables from column lineage
all_source_tables = set()
for lineage_tuple in all_lineage_tuples:
    if lineage_tuple and len(lineage_tuple) > 1 and lineage_tuple[0]:
        source_col = lineage_tuple[0]
        if hasattr(source_col, 'parent') and hasattr(source_col.parent, 'raw_name'):
            all_source_tables.add(source_col.parent)

for source_table_obj in all_source_tables:
    source_table_name = source_table_obj.raw_name
    if source_table_name.startswith("#") or source_table_name in cte_names:
        continue # Skip intermediate tables as primary sources for this output

    info = {
        "columns": set(),
        "destinations": set(),
        "transformation_summary": ""
    }

    # Find columns originating from this source table
    for lineage_tuple in all_lineage_tuples:
        if lineage_tuple and len(lineage_tuple) > 1 and lineage_tuple[0]:
            source_col = lineage_tuple[0]
            if hasattr(source_col, 'parent') and source_col.parent == source_table_obj:
                info["columns"].add(source_col.raw_name)

    # Find direct destinations for this source table
    for relation in all_table_relations:
        if relation.source == source_table_obj:
            info["destinations"].add(relation.target.raw_name)

    # Add a placeholder for transformation summary
    # This is highly complex and would require deeper AST analysis or heuristics
    # For now, we'll just indicate direct insert if applicable
    if len(info["destinations"]) == 1 and list(info["destinations"])[0].startswith("#"):
        info["transformation_summary"] = f"{list(info["destinations"])[0]} -- direct insert, no transformation"
    else:
        info["transformation_summary"] = "See column lineage for details"

    source_table_info[source_table_name] = info

# 6. Print in the desired format
print("--- Source Tables Analysis ---")
print(f"{ 'Table':<30}{ 'Columns':<40}{ 'Destination(s) / Transformation'}")
print(f"{ '='*30:<30}{ '='*40:<40}{ '='*40}")

for table_name in sorted(source_table_info.keys()):
    info = source_table_info[table_name]
    columns_str = ", ".join(sorted(list(info["columns"]))) if info["columns"] else "(all)"
    dest_str = ", ".join(sorted(list(info["destinations"]))) if info["destinations"] else "(unknown)"
    
    # Adjust transformation summary based on destinations
    if len(info["destinations"]) == 1 and list(info["destinations"])[0].startswith("#"):
        transformation_text = f"{list(info["destinations"])[0]} -- direct insert, no transformation"
    elif info["destinations"]:
        transformation_text = f"{dest_str} -- see column lineage for details"
    else:
        transformation_text = "(unknown transformation)"

    print(f"{table_name:<30}{columns_str:<40}{transformation_text}")
