import re
import sqlparse
from sqllineage.runner import LineageRunner
from collections import defaultdict

def analyze_expected_lineage():
    """
    Enhanced lineage analysis to match expected column-level mappings
    """
    
    with open("test.sql", "r") as f:
        sql = f.read()

    print("🎯 " + "=" * 98)
    print("   ENHANCED COLUMN LINEAGE ANALYSIS")
    print("   Matching Expected Source → Target Column Mappings")
    print("=" * 100)

    # Extract procedure body
    sql_body = re.search(r"AS\s*BEGIN(.*)END\s*GO", sql, re.DOTALL | re.IGNORECASE)
    if sql_body:
        sql = sql_body.group(1)

    # Define expected source tables with their columns
    expected_sources = {
        "staging.transactions": [
            "srcid", "txnexternalid", "accountno", "counterparty", "txndate", 
            "valuedate", "amount", "currency", "direction", "txntype", 
            "channel", "narrative", "batchid", "batchdate"
        ],
        "ref.currencyrate": ["fromcurrency", "tocurrency", "rate", "asof"],
        "ref.account": [
            "accountno", "accountid", "customerid", "branchcode", "status", 
            "basecurrency", "overdraftlimit", "productcode"
        ],
        "ref.feeconfig": [
            "productcode", "channel", "amounttierlow", "amounttierhigh", 
            "feeflat", "feepct", "feecode"
        ],
        "ref.glmap": ["txntype", "direction", "glaccount"]
    }

    # Define expected target tables with their columns
    expected_targets = {
        "core.ledgerfinal": [
            "idempotencykey", "accountid", "postingdate", "amountbase", 
            "direction", "txntype", "feeamount", "narrative", "batchid", "createdat"
        ],
        "audit.failedtxn": ["batchid", "txnexternalid", "reason"],
        "core.glwork": ["glaccount"]
    }

    # Expected column mappings based on your specification
    expected_mappings = [
        ("staging.transactions.srcid", "core.ledgerfinal.idempotencykey"),
        ("staging.transactions.txnexternalid", "audit.failedtxn.txnexternalid"),
        ("staging.transactions.accountno", "core.ledgerfinal.accountid"),
        ("staging.transactions.txndate", "core.ledgerfinal.postingdate"),
        ("staging.transactions.valuedate", "core.ledgerfinal.postingdate"),
        ("staging.transactions.amount", "core.ledgerfinal.amountbase"),
        ("staging.transactions.currency", "core.ledgerfinal.amountbase"),
        ("staging.transactions.direction", "core.ledgerfinal.direction"),
        ("staging.transactions.txntype", "core.ledgerfinal.txntype"),
        ("staging.transactions.channel", "core.ledgerfinal.feeamount"),
        ("staging.transactions.narrative", "core.ledgerfinal.narrative"),
        ("staging.transactions.batchid", "core.ledgerfinal.batchid"),
        ("staging.transactions.batchid", "audit.failedtxn.batchid"),
        ("staging.transactions.batchdate", "core.ledgerfinal.createdat"),
        ("ref.account.accountid", "core.ledgerfinal.accountid"),
        ("ref.account.basecurrency", "core.ledgerfinal.amountbase"),
        ("ref.feeconfig.feeflat", "core.ledgerfinal.feeamount"),
        ("ref.feeconfig.feepct", "core.ledgerfinal.feeamount"),
        ("ref.glmap.glaccount", "core.glwork.glaccount"),
        ("ref.currencyrate.rate", "core.ledgerfinal.amountbase"),
        ("staging.transactions.txnexternalid", "audit.failedtxn.txnexternalid"),
    ]

    print("🔍 ANALYZING SQL FOR EXPECTED LINEAGE PATTERNS...")

    # Parse SQL statements to find lineage
    statements = sqlparse.split(sql)
    dml_statements = []
    
    for stmt_str in statements:
        if stmt_str.strip():
            parsed_stmt = sqlparse.parse(stmt_str)[0]
            stmt_type = parsed_stmt.get_type()
            first_token = parsed_stmt.tokens[0].normalized if parsed_stmt.tokens else ""
            
            if stmt_type in ["INSERT", "UPDATE", "MERGE"] or first_token == 'WITH':
                dml_statements.append(stmt_str)

    print(f"✅ Found {len(dml_statements)} DML statements to analyze")

    # Analyze with sqllineage and collect all column mappings
    all_column_mappings = []
    verified_mappings = []
    missing_mappings = []

    print("📊 Running detailed column lineage analysis...")

    for i, stmt in enumerate(dml_statements):
        try:
            clean_stmt = re.sub(r"@[a-zA-Z_]+", "'sample_value'", stmt)
            result = LineageRunner(clean_stmt, dialect="tsql")
            column_lineage = result.get_column_lineage()
            
            for mapping in column_lineage:
                if mapping and len(mapping) >= 2:
                    source_col = str(mapping[0]).lower() if mapping[0] else "unknown"
                    target_col = str(mapping[-1]).lower() if mapping[-1] else "unknown"
                    all_column_mappings.append((source_col, target_col, i+1))
                    
        except Exception as e:
            print(f"   ⚠️ Statement {i+1}: {str(e)[:50]}...")

    print(f"✅ Collected {len(all_column_mappings)} column mappings from analysis")

    # Verify expected mappings against found mappings
    print("\n🎯 VERIFYING EXPECTED COLUMN LINEAGE...")

    found_mappings_normalized = set()
    for source, target, stmt in all_column_mappings:
        # Normalize the mapping format
        source_norm = source.replace(" ", "").lower()
        target_norm = target.replace(" ", "").lower()
        found_mappings_normalized.add((source_norm, target_norm))

    # Check each expected mapping
    for expected_source, expected_target in expected_mappings:
        source_norm = expected_source.replace(" ", "").lower()
        target_norm = expected_target.replace(" ", "").lower()
        
        # Look for exact or partial matches
        found = False
        for found_source, found_target in found_mappings_normalized:
            if (source_norm in found_source and target_norm in found_target) or \
               (found_source in source_norm and found_target in target_norm):
                verified_mappings.append((expected_source, expected_target, "✅ VERIFIED"))
                found = True
                break
        
        if not found:
            missing_mappings.append((expected_source, expected_target, "❌ NOT FOUND"))

    # GENERATE COMPREHENSIVE REPORT
    print("\n📋 " + "=" * 98)
    print("   SOURCE TABLES ANALYSIS")
    print("=" * 100)

    print(f"{'Table':<30} {'Columns'}")
    print("-" * 80)
    for table, columns in expected_sources.items():
        table_display = table.title()
        columns_str = ", ".join([col.title() for col in columns])
        if len(columns_str) > 45:
            columns_str = columns_str[:42] + "..."
        print(f"{table_display:<30} {columns_str}")

    print("\n🎯 " + "=" * 98)
    print("   EXPECTED vs FOUND COLUMN LINEAGE")
    print("=" * 100)

    print(f"{'Source Column':<40} {'Final Column':<35} {'Final Table':<20} {'Status'}")
    print("-" * 110)

    # Show verified mappings first
    for source, target, status in verified_mappings:
        # Parse source and target
        if '.' in target:
            target_parts = target.split('.')
            target_table = target_parts[0]
            target_col = target_parts[1]
        else:
            target_table = "unknown"
            target_col = target
            
        source_display = source.title()
        target_col_display = target_col.title()
        target_table_display = target_table.title()
        
        print(f"{source_display:<40} {target_col_display:<35} {target_table_display:<20} {status}")

    # Show missing mappings
    if missing_mappings:
        print("\n❌ MISSING EXPECTED MAPPINGS:")
        print(f"{'Source Column':<40} {'Final Column':<35} {'Final Table':<20} {'Status'}")
        print("-" * 110)
        
        for source, target, status in missing_mappings:
            if '.' in target:
                target_parts = target.split('.')
                target_table = target_parts[0]
                target_col = target_parts[1]
            else:
                target_table = "unknown"
                target_col = target
                
            source_display = source.title()
            target_col_display = target_col.title()
            target_table_display = target_table.title()
            
            print(f"{source_display:<40} {target_col_display:<35} {target_table_display:<20} {status}")

    print("\n📊 " + "=" * 98)
    print("   ADDITIONAL DISCOVERED MAPPINGS")
    print("=" * 100)

    # Show additional mappings found that weren't in expected list
    additional_mappings = []
    for source, target, stmt in all_column_mappings:
        source_norm = source.replace(" ", "").lower()
        target_norm = target.replace(" ", "").lower()
        
        # Check if this mapping is in our expected list
        is_expected = False
        for exp_source, exp_target in expected_mappings:
            exp_source_norm = exp_source.replace(" ", "").lower()
            exp_target_norm = exp_target.replace(" ", "").lower()
            
            if (source_norm in exp_source_norm or exp_source_norm in source_norm) and \
               (target_norm in exp_target_norm or exp_target_norm in target_norm):
                is_expected = True
                break
        
        if not is_expected and source != "unknown" and target != "unknown":
            additional_mappings.append((source, target, stmt))

    if additional_mappings:
        print(f"{'Source Column':<50} {'Target Column':<50} {'Statement'}")
        print("-" * 110)
        
        for source, target, stmt in sorted(set(additional_mappings))[:20]:  # Show first 20
            print(f"{source:<50} {target:<50} {stmt}")
        
        if len(additional_mappings) > 20:
            print(f"... and {len(additional_mappings) - 20} more additional mappings")
    else:
        print("No additional mappings found beyond expected ones.")

    print("\n📈 " + "=" * 98)
    print("   LINEAGE ANALYSIS SUMMARY")
    print("=" * 100)

    verification_rate = len(verified_mappings) / len(expected_mappings) * 100 if expected_mappings else 0
    
    print(f"📊 VERIFICATION RESULTS:")
    print(f"   • Expected column mappings: {len(expected_mappings)}")
    print(f"   • Successfully verified: {len(verified_mappings)} ({verification_rate:.1f}%)")
    print(f"   • Missing from analysis: {len(missing_mappings)}")
    print(f"   • Additional mappings found: {len(set(additional_mappings))}")
    print(f"   • Total column transformations: {len(all_column_mappings)}")

    print(f"\n🎯 BUSINESS IMPACT:")
    if verification_rate >= 80:
        print(f"   ✅ HIGH CONFIDENCE - Most expected lineage verified")
        print(f"   ✅ Ready for data governance and impact analysis")
    elif verification_rate >= 60:
        print(f"   ⚠️  MEDIUM CONFIDENCE - Some expected lineage missing")
        print(f"   ⚠️  Review missing mappings for complex transformations")
    else:
        print(f"   ❌ LOW CONFIDENCE - Many expected mappings not found")
        print(f"   ❌ May need manual analysis or enhanced parsing")

    print(f"\n💡 RECOMMENDATIONS:")
    if missing_mappings:
        print(f"   • Review {len(missing_mappings)} missing mappings for:")
        print(f"     - Complex transformations (CASE statements, functions)")
        print(f"     - Multi-step processing through temp tables")
        print(f"     - Derived/calculated columns")
    
    print(f"   • Use verified mappings for:")
    print(f"     - Data lineage documentation") 
    print(f"     - Impact analysis for schema changes")
    print(f"     - Compliance and audit reporting")

    print("\n" + "=" * 100)
    print("✅ ENHANCED COLUMN LINEAGE ANALYSIS COMPLETE!")
    print("🎯 Column-level mappings verified against expected lineage")
    print("=" * 100)

    return {
        'verified_mappings': verified_mappings,
        'missing_mappings': missing_mappings,
        'additional_mappings': additional_mappings,
        'total_mappings': all_column_mappings
    }

if __name__ == "__main__":
    analyze_expected_lineage()
