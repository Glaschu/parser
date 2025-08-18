import re
import sqlparse
from sqllineage.runner import LineageRunner
from collections import defaultdict, deque

def trace_end_to_end_lineage():
    """
    Advanced lineage tracer that follows column flows through intermediate tables
    to find complete source-to-target mappings
    """
    
    with open("test.sql", "r") as f:
        sql = f.read()

    print("🔬 " + "=" * 98)
    print("   ADVANCED END-TO-END COLUMN LINEAGE TRACER")
    print("   Tracing Through All Intermediate Processing Steps")
    print("=" * 100)

    # Extract procedure body
    sql_body = re.search(r"AS\s*BEGIN(.*)END\s*GO", sql, re.DOTALL | re.IGNORECASE)
    if sql_body:
        sql = sql_body.group(1)

    # Expected final mappings for verification
    expected_final_mappings = {
        "staging.transactions.srcid": "core.ledgerfinal.idempotencykey",
        "staging.transactions.txnexternalid": "audit.failedtxn.txnexternalid", 
        "staging.transactions.accountno": "core.ledgerfinal.accountid",
        "staging.transactions.txndate": "core.ledgerfinal.postingdate",
        "staging.transactions.valuedate": "core.ledgerfinal.postingdate",
        "staging.transactions.amount": "core.ledgerfinal.amountbase",
        "staging.transactions.currency": "core.ledgerfinal.amountbase",
        "staging.transactions.direction": "core.ledgerfinal.direction",
        "staging.transactions.txntype": "core.ledgerfinal.txntype",
        "staging.transactions.channel": "core.ledgerfinal.feeamount",
        "staging.transactions.narrative": "core.ledgerfinal.narrative",
        "staging.transactions.batchid": "core.ledgerfinal.batchid",
        "staging.transactions.batchdate": "core.ledgerfinal.createdat",
        "ref.account.accountid": "core.ledgerfinal.accountid",
        "ref.account.basecurrency": "core.ledgerfinal.amountbase",
        "ref.feeconfig.feeflat": "core.ledgerfinal.feeamount",
        "ref.feeconfig.feepct": "core.ledgerfinal.feeamount",
        "ref.glmap.glaccount": "core.glwork.glaccount",
        "ref.currencyrate.rate": "core.ledgerfinal.amountbase"
    }

    # Parse all statements and build a comprehensive lineage graph
    statements = sqlparse.split(sql)
    dml_statements = []
    
    for stmt_str in statements:
        if stmt_str.strip():
            parsed_stmt = sqlparse.parse(stmt_str)[0]
            stmt_type = parsed_stmt.get_type()
            first_token = parsed_stmt.tokens[0].normalized if parsed_stmt.tokens else ""
            
            if stmt_type in ["INSERT", "UPDATE", "MERGE"] or first_token == 'WITH':
                dml_statements.append(stmt_str)

    print(f"🔍 Building lineage graph from {len(dml_statements)} statements...")

    # Build complete lineage graph
    lineage_graph = defaultdict(list)  # source_column -> [(target_column, statement_num)]
    table_categories = {
        'source': set(),
        'intermediate': set(), 
        'target': set()
    }
    
    all_columns_seen = set()

    for i, stmt in enumerate(dml_statements):
        try:
            clean_stmt = re.sub(r"@[a-zA-Z_]+", "'sample_value'", stmt)
            result = LineageRunner(clean_stmt, dialect="tsql")
            column_lineage = result.get_column_lineage()
            
            # Track table categories
            for table in result.source_tables:
                table_name = str(table).lower()
                if any(x in table_name for x in ['staging.', 'ref.']):
                    table_categories['source'].add(table_name)
                elif any(x in table_name for x in ['#', 'temp', 'work']):
                    table_categories['intermediate'].add(table_name)
                    
            for table in result.target_tables:
                table_name = str(table).lower()
                if any(x in table_name for x in ['core.', 'audit.', 'ops.']):
                    table_categories['target'].add(table_name)
                elif any(x in table_name for x in ['#', 'temp', 'work']):
                    table_categories['intermediate'].add(table_name)
            
            # Build lineage connections
            for mapping in column_lineage:
                if mapping and len(mapping) >= 2:
                    source_col = str(mapping[0]).lower() if mapping[0] else None
                    target_col = str(mapping[-1]).lower() if mapping[-1] else None
                    
                    if source_col and target_col:
                        lineage_graph[source_col].append((target_col, i+1))
                        all_columns_seen.add(source_col)
                        all_columns_seen.add(target_col)
                        
        except Exception as e:
            print(f"   ⚠️ Statement {i+1}: {str(e)[:60]}...")

    print(f"✅ Built lineage graph with {len(lineage_graph)} source columns")
    print(f"✅ Found {len(all_columns_seen)} total columns in lineage")

    # Advanced path tracing function
    def trace_column_path(start_column, max_depth=10):
        """
        Trace a column through all intermediate transformations to find final destinations
        """
        final_destinations = []
        visited = set()
        
        def dfs_trace(current_col, path, depth):
            if depth > max_depth or current_col in visited:
                return
            visited.add(current_col)
            
            # Check if this is a final destination (target table)
            is_final = False
            for target_table in table_categories['target']:
                if target_table in current_col:
                    final_destinations.append((current_col, path + [current_col]))
                    is_final = True
                    break
            
            # If not final, continue tracing
            if not is_final and current_col in lineage_graph:
                for next_col, stmt_num in lineage_graph[current_col]:
                    dfs_trace(next_col, path + [current_col], depth + 1)
        
        dfs_trace(start_column, [], 0)
        return final_destinations

    print("\n🎯 TRACING END-TO-END COLUMN PATHS...")

    # Trace paths for each expected source column
    traced_mappings = []
    successful_traces = 0
    
    for expected_source, expected_target in expected_final_mappings.items():
        print(f"   Tracing: {expected_source}")
        
        # Find matching source columns in our graph
        matching_sources = []
        source_parts = expected_source.split('.')
        
        for col in all_columns_seen:
            # Match by table and column name patterns
            if len(source_parts) >= 2:
                table_part = source_parts[0].lower()
                column_part = source_parts[1].lower()
                
                if table_part in col and column_part in col:
                    matching_sources.append(col)
        
        if matching_sources:
            print(f"     Found {len(matching_sources)} potential sources")
            
            # Trace each matching source
            for source_col in matching_sources:
                destinations = trace_column_path(source_col)
                
                for dest_col, path in destinations:
                    # Check if destination matches expected target
                    target_parts = expected_target.split('.')
                    if len(target_parts) >= 2:
                        target_table = target_parts[0].lower()
                        target_column = target_parts[1].lower()
                        
                        if target_table in dest_col and target_column in dest_col:
                            traced_mappings.append({
                                'source': expected_source,
                                'target': expected_target,
                                'source_actual': source_col,
                                'target_actual': dest_col,
                                'path': path,
                                'status': '✅ TRACED'
                            })
                            successful_traces += 1
                            print(f"     ✅ Found path: {source_col} → {dest_col}")
                            break
        else:
            traced_mappings.append({
                'source': expected_source,
                'target': expected_target,
                'source_actual': None,
                'target_actual': None,
                'path': [],
                'status': '❌ NO SOURCE FOUND'
            })

    print(f"\n✅ Successfully traced {successful_traces} out of {len(expected_final_mappings)} expected mappings")

    # GENERATE COMPREHENSIVE REPORT
    print("\n📋 " + "=" * 98)
    print("   TRACED END-TO-END COLUMN LINEAGE RESULTS")
    print("=" * 100)

    print(f"{'Expected Source':<35} {'Expected Target':<35} {'Status':<15} {'Path Length'}")
    print("-" * 100)

    successful_mappings = []
    failed_mappings = []

    for mapping in traced_mappings:
        status = mapping['status']
        path_length = len(mapping['path']) if mapping['path'] else 0
        
        print(f"{mapping['source']:<35} {mapping['target']:<35} {status:<15} {path_length}")
        
        if mapping['status'] == '✅ TRACED':
            successful_mappings.append(mapping)
        else:
            failed_mappings.append(mapping)

    # Show detailed paths for successful traces
    if successful_mappings:
        print("\n🔍 " + "=" * 98)
        print("   DETAILED TRANSFORMATION PATHS")
        print("=" * 100)
        
        for i, mapping in enumerate(successful_mappings[:10]):  # Show first 10
            print(f"\n{i+1}. {mapping['source']} → {mapping['target']}")
            print(f"   Path: {' → '.join(mapping['path'])}")

    # Show additional discovered end-to-end mappings
    print("\n📊 " + "=" * 98)
    print("   ADDITIONAL END-TO-END MAPPINGS DISCOVERED")
    print("=" * 100)

    additional_mappings = []
    
    # Find all source columns from source tables
    source_columns = [col for col in all_columns_seen 
                     if any(src_table in col for src_table in table_categories['source'])]
    
    print(f"Tracing {len(source_columns)} source columns for additional mappings...")
    
    for source_col in source_columns[:50]:  # Limit to first 50 to avoid overwhelming output
        destinations = trace_column_path(source_col)
        for dest_col, path in destinations:
            # Check if this is not in our expected mappings
            is_expected = False
            for mapping in traced_mappings:
                if mapping['source_actual'] == source_col and mapping['target_actual'] == dest_col:
                    is_expected = True
                    break
            
            if not is_expected:
                additional_mappings.append((source_col, dest_col, len(path)))

    if additional_mappings:
        print(f"{'Source Column':<50} {'Target Column':<50} {'Hops'}")
        print("-" * 110)
        
        for source, target, hops in sorted(additional_mappings)[:15]:  # Show first 15
            print(f"{source:<50} {target:<50} {hops}")
    
    # Final summary
    print("\n📈 " + "=" * 98)
    print("   ADVANCED LINEAGE ANALYSIS SUMMARY")
    print("=" * 100)

    success_rate = (successful_traces / len(expected_final_mappings)) * 100 if expected_final_mappings else 0
    
    print(f"🎯 TRACING RESULTS:")
    print(f"   • Expected end-to-end mappings: {len(expected_final_mappings)}")
    print(f"   • Successfully traced: {successful_traces} ({success_rate:.1f}%)")
    print(f"   • Failed to trace: {len(failed_mappings)}")
    print(f"   • Additional mappings found: {len(additional_mappings)}")
    
    print(f"\n🔬 GRAPH ANALYSIS:")
    print(f"   • Total columns in lineage graph: {len(all_columns_seen)}")
    print(f"   • Source table columns: {len([c for c in all_columns_seen if any(s in c for s in table_categories['source'])])}")
    print(f"   • Target table columns: {len([c for c in all_columns_seen if any(t in c for t in table_categories['target'])])}")
    print(f"   • Intermediate transformations: {len(lineage_graph)}")

    if success_rate >= 70:
        print(f"\n✅ HIGH CONFIDENCE: Advanced tracing successfully mapped most expected lineage")
        print(f"✅ The stored procedure's end-to-end data flow is well understood")
    elif success_rate >= 40:
        print(f"\n⚠️ MEDIUM CONFIDENCE: Some complex transformations may need manual analysis")
        print(f"⚠️ Consider reviewing failed mappings for business logic complexity")
    else:
        print(f"\n❌ LOW CONFIDENCE: Many transformations are too complex for automated tracing")
        print(f"❌ Manual code review recommended for critical lineage mappings")

    print("\n" + "=" * 100)
    print("🔬 ADVANCED END-TO-END LINEAGE TRACING COMPLETE!")
    print("🎯 Multi-hop column transformation paths analyzed and documented")
    print("=" * 100)

    return {
        'successful_mappings': successful_mappings,
        'failed_mappings': failed_mappings,
        'additional_mappings': additional_mappings,
        'lineage_graph': dict(lineage_graph)
    }

if __name__ == "__main__":
    trace_end_to_end_lineage()
