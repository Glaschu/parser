import re
import sqlparse
from sqllineage.runner import LineageRunner

# Read the SQL file
with open("test.sql", "r") as f:
    sql = f.read()

print("=" * 80)
print("COMPREHENSIVE SQL LINEAGE ANALYSIS")
print("=" * 80)

# 1. Extract the core body of the procedure
sql_body = re.search(r"AS\s*BEGIN(.*)END\s*GO", sql, re.DOTALL | re.IGNORECASE)
if sql_body:
    sql = sql_body.group(1)
    print(f"✓ Extracted procedure body ({len(sql)} characters)")
else:
    print("✗ Could not extract procedure body - analyzing full SQL")

# 2. Deterministically find all CTE names and temp tables
cte_names = set(re.findall(r"WITH\s+([a-zA-Z0-9_]+)\s+AS", sql, re.IGNORECASE))
temp_tables = set(re.findall(r"#([a-zA-Z0-9_]+)", sql, re.IGNORECASE))

print(f"✓ Found {len(cte_names)} CTEs: {', '.join(sorted(cte_names)) if cte_names else 'None'}")
print(f"✓ Found {len(temp_tables)} temp tables: {', '.join(sorted(temp_tables)) if temp_tables else 'None'}")

# 3. Split into individual statements
statements = sqlparse.split(sql)
print(f"✓ Split into {len(statements)} SQL statements")

# 4. Filter for DML statements that are relevant to lineage
supported_types = ["INSERT", "UPDATE", "MERGE"]
dml_statements = []
for stmt_str in statements:
    if stmt_str.strip():
        parsed_stmt = sqlparse.parse(stmt_str)[0]
        first_token_norm = parsed_stmt.tokens[0].normalized if parsed_stmt.tokens else ""
        if parsed_stmt.get_type() in supported_types or first_token_norm == 'WITH':
            dml_statements.append(stmt_str)

print(f"✓ Found {len(dml_statements)} DML statements for lineage analysis")

# 5. Analyze statement by statement and aggregate lineage
all_lineage_tuples = []
all_table_relations = []
all_source_tables = set()  # Move this declaration here
processing_errors = []

# Collect WITH clauses to prepend to non-WITH statements
with_clauses = [s for s in dml_statements if s.strip().upper().startswith('WITH')]

for i, stmt_group in enumerate(dml_statements):
    # Prepare SQL for parsing
    if not stmt_group.strip().upper().startswith('WITH'):
        sql_to_parse = "\n".join(with_clauses) + "\n" + stmt_group
    else:
        sql_to_parse = stmt_group

    # Replace variables with sample values to help parser
    sql_to_parse = re.sub(r"@[a-zA-Z_]+", "'sample_value'", sql_to_parse)
    
    try:
        result = LineageRunner(sql_to_parse, dialect="tsql")
        stmt_lineage = result.get_column_lineage()
        
        # Get table relations using source_tables and target_tables
        stmt_relations = []
        source_tables = result.source_tables
        target_tables = result.target_tables
        intermediate_tables = result.intermediate_tables
        
        # Create relations: source -> target
        for source_table in source_tables:
            for target_table in target_tables:
                stmt_relations.append((source_table, target_table))
        
        # Also include intermediate table relationships
        for source_table in source_tables:
            for intermediate_table in intermediate_tables:
                stmt_relations.append((source_table, intermediate_table))
        
        for intermediate_table in intermediate_tables:
            for target_table in target_tables:
                stmt_relations.append((intermediate_table, target_table))
        
        all_lineage_tuples.extend(stmt_lineage)
        all_table_relations.extend(stmt_relations)
        
        # Add tables to source collection
        for table in source_tables + target_tables + intermediate_tables:
            all_source_tables.add(table)
        
        print(f"  Statement {i+1}: {len(stmt_lineage)} column lineages, {len(source_tables)} sources, {len(target_tables)} targets, {len(intermediate_tables)} intermediate")
    except Exception as e:
        error_msg = f"Statement {i+1}: {str(e)[:100]}..."
        processing_errors.append(error_msg)
        print(f"  ✗ {error_msg}")

print(f"✓ Total column lineages found: {len(all_lineage_tuples)}")
print(f"✓ Total table relations found: {len(all_table_relations)}")
if processing_errors:
    print(f"⚠ Processing errors: {len(processing_errors)}")

# 6. Build comprehensive lineage maps
def is_intermediate(column_or_table, defined_cte_names, temp_table_names):
    """Determine if a column/table is intermediate (temp table, CTE, or work table)"""
    if hasattr(column_or_table, 'parent') and hasattr(column_or_table.parent, 'raw_name'):
        table_name = column_or_table.parent.raw_name.lower()
    elif hasattr(column_or_table, 'raw_name'):
        table_name = column_or_table.raw_name.lower()
    else:
        return True
    
    # Check for various intermediate patterns
    return (table_name.startswith("#") or 
            any(cte.lower() in table_name for cte in defined_cte_names) or
            any(temp.lower() in table_name for temp in temp_table_names) or
            table_name.endswith("work") or
            "temp" in table_name or
            "stage" in table_name)

# Build column lineage map
lineage_map = {}
for lineage_tuple in all_lineage_tuples:
    if lineage_tuple and len(lineage_tuple) > 1:
        target = lineage_tuple[-1]
        source = lineage_tuple[0]
        if source and target:
            lineage_map[target] = source

def find_original_source(column, defined_cte_names, temp_table_names, visited=None):
    """Recursively find the original source column, skipping intermediate tables"""
    if visited is None:
        visited = set()
    if column in visited:
        return column
    visited.add(column)

    source = lineage_map.get(column)
    if source and is_intermediate(source, defined_cte_names, temp_table_names):
        return find_original_source(source, defined_cte_names, temp_table_names, visited)
    return source or column

# 7. Generate end-to-end column lineage (from lineage_parser.py approach)
print("\n" + "=" * 80)
print("END-TO-END COLUMN LINEAGE")
print("=" * 80)

final_lineage = {}
for target, source in lineage_map.items():
    if not is_intermediate(target, cte_names, temp_tables):
        original_source = find_original_source(source, cte_names, temp_tables)
        if original_source and not is_intermediate(original_source, cte_names, temp_tables):
            final_lineage[str(target)] = str(original_source)

if final_lineage:
    for target, source in sorted(final_lineage.items()):
        print(f"{target} <-- {source}")
else:
    print("No end-to-end column lineage found (may indicate complex transformations)")

# 8. Generate source table analysis (from lineage_parser2.py approach)
print("\n" + "=" * 80)
print("SOURCE TABLES ANALYSIS")
print("=" * 80)

source_table_info = {}

# Collect all unique source tables from column lineage - all_source_tables already populated above
for lineage_tuple in all_lineage_tuples:
    if lineage_tuple and len(lineage_tuple) > 1 and lineage_tuple[0]:
        source_col = lineage_tuple[0]
        if hasattr(source_col, 'parent') and hasattr(source_col.parent, 'raw_name'):
            all_source_tables.add(source_col.parent)

# Add tables from table relations
for relation in all_table_relations:
    if isinstance(relation, tuple) and len(relation) >= 2:
        # Handle tuple format (source_table, target_table)
        source_table = relation[0]
        if hasattr(source_table, 'raw_name'):
            all_source_tables.add(source_table)
    elif hasattr(relation, 'source') and hasattr(relation.source, 'raw_name'):
        all_source_tables.add(relation.source)

for source_table_obj in all_source_tables:
    source_table_name = source_table_obj.raw_name
    
    # Skip intermediate tables for source analysis
    if is_intermediate(source_table_obj, cte_names, temp_tables):
        continue

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
        if isinstance(relation, tuple) and len(relation) >= 2:
            # Handle tuple format (source_table, target_table)
            source_table, target_table = relation[0], relation[1]
            if source_table == source_table_obj:
                if hasattr(target_table, 'raw_name'):
                    dest_name = target_table.raw_name
                    # Only add non-intermediate destinations or mark as intermediate
                    if is_intermediate(target_table, cte_names, temp_tables):
                        info["destinations"].add(f"{dest_name} (intermediate)")
                    else:
                        info["destinations"].add(dest_name)
        elif hasattr(relation, 'source') and relation.source == source_table_obj:
            if hasattr(relation, 'target') and hasattr(relation.target, 'raw_name'):
                dest_name = relation.target.raw_name
                # Only add non-intermediate destinations or mark as intermediate
                if is_intermediate(relation.target, cte_names, temp_tables):
                    info["destinations"].add(f"{dest_name} (intermediate)")
                else:
                    info["destinations"].add(dest_name)

    # Determine transformation summary
    if not info["destinations"]:
        info["transformation_summary"] = "No direct destinations found"
    elif len(info["destinations"]) == 1:
        dest = list(info["destinations"])[0]
        if "(intermediate)" in dest:
            info["transformation_summary"] = f"Flows through {dest.replace(' (intermediate)', '')} - see column lineage"
        else:
            info["transformation_summary"] = f"Direct flow to {dest}"
    else:
        info["transformation_summary"] = "Multiple destinations - see column lineage for details"

    source_table_info[source_table_name] = info

# Display source table analysis
if source_table_info:
    print(f"{'Table':<25} {'Columns':<35} {'Destinations / Transformation'}")
    print(f"{'='*25} {'='*35} {'='*40}")
    
    for table_name in sorted(source_table_info.keys()):
        info = source_table_info[table_name]
        columns_str = ", ".join(sorted(list(info["columns"]))) if info["columns"] else "(all/*)"
        
        # Truncate long column lists
        if len(columns_str) > 32:
            columns_str = columns_str[:29] + "..."
        
        dest_summary = info["transformation_summary"]
        if len(dest_summary) > 37:
            dest_summary = dest_summary[:34] + "..."
        
        print(f"{table_name:<25} {columns_str:<35} {dest_summary}")
else:
    print("No source tables identified (all tables may be intermediate)")

# 9. Generate table relationship summary
print("\n" + "=" * 80)
print("TABLE RELATIONSHIP SUMMARY")
print("=" * 80)

if all_table_relations:
    print(f"{'Source Table':<25} {'Target Table':<25} {'Relationship Type'}")
    print(f"{'='*25} {'='*25} {'='*15}")
    
    unique_relations = set()
    for relation in all_table_relations:
        if isinstance(relation, tuple) and len(relation) >= 2:
            # Handle tuple format (source_table, target_table)
            source_table, target_table = relation[0], relation[1]
            source_name = source_table.raw_name if hasattr(source_table, 'raw_name') else str(source_table)
            target_name = target_table.raw_name if hasattr(target_table, 'raw_name') else str(target_table)
            
            # Determine relationship type
            if is_intermediate(target_table, cte_names, temp_tables):
                rel_type = "Staging"
            elif is_intermediate(source_table, cte_names, temp_tables):
                rel_type = "Final"
            else:
                rel_type = "Direct"
            
            unique_relations.add((source_name, target_name, rel_type))
        elif hasattr(relation, 'source') and hasattr(relation, 'target'):
            source_name = relation.source.raw_name if hasattr(relation.source, 'raw_name') else str(relation.source)
            target_name = relation.target.raw_name if hasattr(relation.target, 'raw_name') else str(relation.target)
            
            # Determine relationship type
            if is_intermediate(relation.target, cte_names, temp_tables):
                rel_type = "Staging"
            elif is_intermediate(relation.source, cte_names, temp_tables):
                rel_type = "Final"
            else:
                rel_type = "Direct"
            
            unique_relations.add((source_name, target_name, rel_type))
    
    for source_name, target_name, rel_type in sorted(unique_relations):
        print(f"{source_name:<25} {target_name:<25} {rel_type}")
else:
    print("No table relationships found")

# 10. Summary statistics
print("\n" + "=" * 80)
print("ANALYSIS SUMMARY")
print("=" * 80)
print(f"Total statements analyzed: {len(dml_statements)}")
print(f"Column lineages discovered: {len(all_lineage_tuples)}")
print(f"Table relationships discovered: {len(all_table_relations)}")
print(f"End-to-end column mappings: {len(final_lineage)}")
print(f"Source tables identified: {len(source_table_info)}")
print(f"Processing errors: {len(processing_errors)}")

if processing_errors:
    print(f"\nProcessing Errors:")
    for error in processing_errors[:5]:  # Show first 5 errors
        print(f"  • {error}")
    if len(processing_errors) > 5:
        print(f"  ... and {len(processing_errors) - 5} more")

print("\n" + "=" * 80)
