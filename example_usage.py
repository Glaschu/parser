#!/usr/bin/env python3
"""
Example usage of the Generic SQL Lineage Parser
"""

from generic_sql_lineage_parser import GenericSQLLineageParser

def example_basic_usage():
    """Example of basic parser usage"""
    print("🔍 Example 1: Basic Analysis")
    print("=" * 60)
    
    # Create parser instance for your SQL file
    parser = GenericSQLLineageParser("test.sql")
    
    # Run analysis
    results = parser.analyze()
    
    # Access results programmatically
    print(f"\n📊 Results Summary:")
    print(f"   Source tables: {len(results['source_tables'])}")
    print(f"   Target tables: {len(results['target_tables'])}")
    print(f"   Column mappings: {len(results['column_mappings'])}")
    
    return results

def example_export_results():
    """Example of exporting results to JSON"""
    print("\n🔍 Example 2: Export Results")
    print("=" * 60)
    
    parser = GenericSQLLineageParser("test.sql")
    parser.analyze()
    
    # Export to JSON file
    parser.export_results('json', 'my_lineage_analysis.json')
    
    print("✅ Results exported to 'my_lineage_analysis.json'")

def example_analyze_different_sql():
    """Example of analyzing a different SQL file"""
    
    # Create a simple test SQL file
    sample_sql = """
    -- Simple data transformation
    CREATE VIEW customer_summary AS
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(o.order_id) as total_orders,
        SUM(o.amount) as total_amount
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.status = 'ACTIVE'
    GROUP BY c.customer_id, c.first_name, c.last_name;
    
    INSERT INTO customer_analytics (customer_id, total_orders, total_amount, analysis_date)
    SELECT customer_id, total_orders, total_amount, CURRENT_DATE
    FROM customer_summary
    WHERE total_amount > 1000;
    """
    
    # Write to file
    with open("sample_analysis.sql", "w") as f:
        f.write(sample_sql)
    
    print("\n🔍 Example 3: Different SQL File")
    print("=" * 60)
    
    # Analyze the new file
    parser = GenericSQLLineageParser("sample_analysis.sql")
    parser.analyze()

def example_command_line_usage():
    """Show command line usage examples"""
    print("\n🔍 Example 4: Command Line Usage")
    print("=" * 60)
    print("You can also use the parser from command line:")
    print()
    print("# Analyze any SQL file:")
    print("python generic_sql_lineage_parser.py your_file.sql")
    print()
    print("# Analyze and export to JSON:")
    print("python generic_sql_lineage_parser.py your_file.sql --export json")
    print()
    print("# Analyze and save to specific file:")
    print("python generic_sql_lineage_parser.py your_file.sql --export json --output my_results.json")

if __name__ == "__main__":
    # Run all examples
    example_basic_usage()
    example_export_results() 
    example_analyze_different_sql()
    example_command_line_usage()
    
    print("\n" + "=" * 60)
    print("✅ All examples completed!")
    print("🎯 The parser is ready to analyze any SQL script!")
    print("=" * 60)
