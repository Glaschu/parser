#!/usr/bin/env python3
"""
Hybrid SQL Lineage Parser
Combines C# ScriptDom for table extraction with Python sqllineage for column lineage
"""

import subprocess
import re
import sys
from pathlib import Path
from generic_sql_lineage_parser import GenericSQLLineageParser

class HybridSQLLineageParser:
    """
    Hybrid parser that uses C# ScriptDom to extract accurate table information
    then uses Python sqllineage for detailed column lineage analysis
    """
    
    def __init__(self, sql_file_path):
        self.sql_file_path = sql_file_path
        self.csharp_metadata = None
        self.python_parser = None
        
    def run_csharp_parser(self):
        """Run the C# ScriptDom parser to extract table metadata"""
        try:
            # Run the C# parser
            result = subprocess.run(
                ['dotnet', 'run'],
                cwd='/Users/jamesglasgow/Projects/parser',
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"❌ C# parser error: {result.stderr}")
                return None
            
            # Read metadata from the JSON file created by C#
            metadata_path = '/Users/jamesglasgow/Projects/parser/csharp_metadata.json'
            if Path(metadata_path).exists():
                import json
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                print(f"🔍 DEBUG: Loaded C# metadata from file:")
                print(f"   Procedure: {metadata.get('procedure_name')}")
                print(f"   Source tables: {metadata.get('source_tables', [])}")
                print(f"   Target tables: {metadata.get('target_tables', [])}")
                
                return metadata
            else:
                print(f"❌ Metadata file not found: {metadata_path}")
                return None
                
        except subprocess.TimeoutExpired:
            print("❌ C# parser timed out")
            return None
        except Exception as e:
            print(f"❌ Error running C# parser: {e}")
            return None
    
    def _parse_csharp_output(self, output):
        """Parse the C# parser output to extract metadata"""
        metadata = {
            'procedure_name': None,
            'source_tables': [],
            'target_tables': []
        }
        
        lines = output.split('\n')
        in_metadata = False
        current_section = None
        
        for line in lines:
            line = line.strip()
            
            if line == "HYBRID_METADATA_START":
                in_metadata = True
                continue
            elif line == "HYBRID_METADATA_END":
                in_metadata = False
                break
            
            if not in_metadata:
                continue
                
            if line.startswith("PROCEDURE_NAME:"):
                metadata['procedure_name'] = line.split(":", 1)[1].strip()
            elif line == "SOURCE_TABLES:":
                current_section = 'source'
                continue
            elif line == "TARGET_TABLES:":
                current_section = 'target'
                continue
            elif line.startswith("  ") and current_section:
                table_name = line.strip()
                if table_name:  # Make sure it's not empty
                    if current_section == 'source':
                        metadata['source_tables'].append(table_name)
                    elif current_section == 'target':
                        metadata['target_tables'].append(table_name)
        
        print(f"🔍 DEBUG: Parsed C# metadata:")
        print(f"   Procedure: {metadata['procedure_name']}")
        print(f"   Source tables: {metadata['source_tables']}")
        print(f"   Target tables: {metadata['target_tables']}")
        
        return metadata
    
    def analyze(self):
        """Main analysis method combining C# and Python parsers"""
        print("🔧 " + "=" * 100)
        print("   HYBRID SQL LINEAGE ANALYSIS")
        print(f"   File: {self.sql_file_path}")
        print("=" * 102)
        
        # Step 1: Run C# ScriptDom parser
        print("🔍 Step 1: Running C# ScriptDom parser for table extraction...")
        self.csharp_metadata = self.run_csharp_parser()
        
        if not self.csharp_metadata:
            print("❌ Failed to get C# metadata, falling back to Python-only analysis")
            self.python_parser = GenericSQLLineageParser(self.sql_file_path)
            return self.python_parser.analyze()
        
        source_tables = self.csharp_metadata.get('source_tables', [])
        target_tables = self.csharp_metadata.get('target_tables', [])
        
        print(f"✅ C# parser found {len(source_tables)} source tables")
        print(f"✅ C# parser found {len(target_tables)} target tables")
        
        # Step 2: Run Python parser with C# metadata
        print("\n🐍 Step 2: Running Python parser for column lineage...")
        self.python_parser = EnhancedGenericSQLLineageParser(
            self.sql_file_path, 
            self.csharp_metadata
        )
        
        return self.python_parser.analyze()


class EnhancedGenericSQLLineageParser(GenericSQLLineageParser):
    """
    Enhanced version of the generic parser that uses C# ScriptDom metadata
    to improve table categorization and end-to-end lineage tracing
    """
    
    def __init__(self, sql_file_path, csharp_metadata):
        super().__init__(sql_file_path)
        self.csharp_metadata = csharp_metadata
        
        # Pre-populate table categories based on C# analysis
        self._initialize_from_csharp_metadata()
    
    def analyze(self):
        """Enhanced analyze method that includes hybrid end-to-end lineage"""
        # Run the base analysis first
        result = super().analyze()
        
        # Add hybrid end-to-end lineage tracing
        end_to_end_mappings = self._trace_end_to_end_lineage()
        
        # Display enhanced end-to-end lineage results
        self._display_hybrid_end_to_end_lineage(end_to_end_mappings)
        
        return result
    
    def _display_hybrid_end_to_end_lineage(self, end_to_end_mappings):
        """Display the hybrid end-to-end lineage results"""
        print("🎯 " + "=" * 100)
        print("   HYBRID END-TO-END COLUMN LINEAGE (C# + Python)")
        print("=" * 102)
        
        if end_to_end_mappings:
            print(f"🎉 Successfully traced {len(end_to_end_mappings)} complete end-to-end column lineages!")
            print("")
            
            # Group by source table for better organization
            by_source_table = {}
            for mapping in end_to_end_mappings:
                source_table = mapping['source_table']
                if source_table not in by_source_table:
                    by_source_table[source_table] = []
                by_source_table[source_table].append(mapping)
            
            for source_table in sorted(by_source_table.keys()):
                mappings = by_source_table[source_table]
                print(f"📋 FROM SOURCE TABLE: {source_table}")
                print("─" * 90)
                
                for mapping in mappings:
                    source_col = mapping['source_column']
                    target_table = mapping['target_table']
                    target_col = mapping['target_column']
                    steps = mapping['steps']
                    intermediate_tables = mapping.get('intermediate_tables', [])
                    
                    print(f"   {source_col:25} → {target_table}.{target_col}")
                    if intermediate_tables:
                        intermediate_str = ' → '.join(intermediate_tables)
                        print(f"   {'':25}   Via: {intermediate_str}")
                    print(f"   {'':25}   Steps: {steps}")
                    print("")
                
                print("")
            
            # Summary by target table
            by_target_table = {}
            for mapping in end_to_end_mappings:
                target_table = mapping['target_table']
                if target_table not in by_target_table:
                    by_target_table[target_table] = []
                by_target_table[target_table].append(mapping)
            
            print("📊 SUMMARY BY TARGET TABLE:")
            print("─" * 90)
            for target_table in sorted(by_target_table.keys()):
                mappings = by_target_table[target_table]
                source_tables = set(m['source_table'] for m in mappings)
                print(f"   {target_table:30} ← {len(mappings):2} columns from {len(source_tables)} source tables")
                source_list = ', '.join(sorted(source_tables))
                print(f"   {'':30}   Sources: {source_list}")
                print("")
        
        else:
            print("❌ No complete end-to-end column lineages found")
            print("   This might indicate:")
            print("   • Missing intermediate transformations (e.g., MERGE statements)")
            print("   • Complex CTEs or derived tables not fully parsed")
            print("   • Disconnected data flows between source and target systems")
        
        print("=" * 102)

    def _initialize_from_csharp_metadata(self):
        """Initialize table categorization using C# ScriptDom results"""
        # Mark source tables
        source_tables = self.csharp_metadata.get('source_tables', [])
        for table in source_tables:
            table_lower = table.lower()
            if table_lower not in self.source_tables:
                self.source_tables[table_lower] = {'columns': set(), 'usage_count': 1}
        
        # Mark target tables  
        target_tables = self.csharp_metadata.get('target_tables', [])
        for table in target_tables:
            table_lower = table.lower()
            if table_lower not in self.target_tables:
                self.target_tables[table_lower] = {'columns': set(), 'usage_count': 1}
    
    def _discover_dynamic_bridges(self, comprehensive_flows, comprehensive_column_to_table, source_tables, final_target_tables):
        """Dynamically discover bridge connections based on naming patterns and intermediate tables"""
        bridges = {}
        
        # Find intermediate temp tables that might need bridging
        intermediate_tables = set()
        for col, table in comprehensive_column_to_table.items():
            if (table.startswith('#') or table.startswith('<default>.#')) and table not in source_tables and table not in final_target_tables:
                intermediate_tables.add(table)
        
        print(f"🔍 Found {len(intermediate_tables)} intermediate temp tables: {sorted(intermediate_tables)}")
        
        # Strategy 1: Bridge columns with similar names between intermediate and target tables
        target_columns = {}
        for col, table in comprehensive_column_to_table.items():
            if table in final_target_tables:
                column_name = col.split('.')[-1]
                if column_name not in target_columns:
                    target_columns[column_name] = []
                target_columns[column_name].append(col)
        
        intermediate_columns = {}
        for col, table in comprehensive_column_to_table.items():
            if table in intermediate_tables:
                column_name = col.split('.')[-1]
                if column_name not in intermediate_columns:
                    intermediate_columns[column_name] = []
                intermediate_columns[column_name].append(col)
        
        # Bridge matching column names
        for column_name in intermediate_columns:
            if column_name in target_columns:
                for intermediate_col in intermediate_columns[column_name]:
                    for target_col in target_columns[column_name]:
                        # Only bridge if there's no existing direct path
                        if intermediate_col not in comprehensive_flows or target_col not in comprehensive_flows.get(intermediate_col, []):
                            bridges[intermediate_col] = target_col
                            print(f"   🔗 Bridge: {intermediate_col} → {target_col} (matching column name)")
        
        # Strategy 2: Bridge based on C# metadata MERGE patterns
        if self.csharp_metadata and 'merge_patterns' in self.csharp_metadata:
            for pattern in self.csharp_metadata['merge_patterns']:
                source_table = pattern.get('source_table', '').lower()
                target_table = pattern.get('target_table', '').lower()
                if source_table and target_table:
                    # Find columns in these tables and create bridges
                    source_cols = [col for col, table in comprehensive_column_to_table.items() if table == source_table]
                    target_cols = [col for col, table in comprehensive_column_to_table.items() if table == target_table]
                    
                    for source_col in source_cols:
                        column_name = source_col.split('.')[-1]
                        matching_targets = [tc for tc in target_cols if tc.split('.')[-1] == column_name]
                        for target_col in matching_targets:
                            bridges[source_col] = target_col
                            print(f"   🔗 Bridge: {source_col} → {target_col} (C# MERGE pattern)")
        
        # Strategy 3: Smart column name mapping for common transformation patterns
        common_mappings = {
            'hashid': 'idempotencykey',
            'srcid': 'batchid',
            'batchdate': 'postingdate',
            'txnexternalid': 'hashid',
            'fromccy': 'fromcurrency',
            'toccy': 'tocurrency',
            'fxrate': 'rate'
        }
        
        for intermediate_col, table in comprehensive_column_to_table.items():
            if table in intermediate_tables:
                column_name = intermediate_col.split('.')[-1]
                if column_name in common_mappings:
                    target_column_name = common_mappings[column_name]
                    # Find target columns with this name
                    matching_targets = [col for col in target_columns.get(target_column_name, [])]
                    for target_col in matching_targets:
                        if intermediate_col not in comprehensive_flows or target_col not in comprehensive_flows.get(intermediate_col, []):
                            bridges[intermediate_col] = target_col
                            print(f"   🔗 Bridge: {intermediate_col} → {target_col} (smart mapping: {column_name} → {target_column_name})")
        
        return bridges

    def _trace_end_to_end_lineage(self):
        """Enhanced end-to-end tracing using C# metadata"""
        print("\n🔍 Step 4: Tracing end-to-end column lineage (C# + Python hybrid)...")
        
        # Use C# identified tables as the authoritative source
        final_target_tables = set(table.lower() for table in self.csharp_metadata.get('target_tables', []))
        source_tables = set(table.lower() for table in self.csharp_metadata.get('source_tables', []))
        
        print(f"📊 C# identified source tables: {sorted(source_tables)}")
        print(f"📊 C# identified target tables: {sorted(final_target_tables)}")
        
        # Build flow graph from discovered mappings
        all_flows = {}
        column_to_table = {}
        
        for mapping in self.column_mappings:
            source_col = mapping['source_column'].lower()
            target_col = mapping['target_column'].lower()
            
            if source_col not in all_flows:
                all_flows[source_col] = set()
            all_flows[source_col].add(target_col)
            
            # Map columns to their tables
            if '.' in source_col:
                source_table = '.'.join(source_col.split('.')[:-1])
                column_to_table[source_col] = source_table
            if '.' in target_col:
                target_table = '.'.join(target_col.split('.')[:-1])
                column_to_table[target_col] = target_table
        
        def find_all_paths_to_finals(start_col, visited=None, path=None):
            """Recursively find all paths from a column to C# identified final target tables"""
            if visited is None:
                visited = set()
            if path is None:
                path = []
            
            if start_col in visited:
                return []
            
            visited.add(start_col)
            current_path = path + [start_col]
            all_paths = []
            
            # Check if this column is in a C# identified final target table
            if start_col in column_to_table:
                table = column_to_table[start_col]
                if table in final_target_tables:
                    return [current_path]
            
            # Continue tracing through all connections
            for next_col in all_flows.get(start_col, []):
                sub_paths = find_all_paths_to_finals(next_col, visited.copy(), current_path)
                all_paths.extend(sub_paths)
            
            return all_paths
        
        # Find end-to-end paths from C# identified source tables to target tables
        end_to_end_mappings = []
        
        print("🔍 Tracing paths from C# source tables to C# target tables...")
        print("🔗 Building comprehensive lineage map from both C# and Python data...")
        
        # Combine all mappings from both C# and Python
        comprehensive_flows = {}
        
        # Add Python column mappings
        for mapping in self.column_mappings:
            source_col = mapping['source_column'].lower()
            target_col = mapping['target_column'].lower()
            
            if source_col not in comprehensive_flows:
                comprehensive_flows[source_col] = []
            comprehensive_flows[source_col].append(target_col)
        
        # Add C# column lineages from JSON metadata
        if self.csharp_metadata and 'column_lineages' in self.csharp_metadata:
            for lineage in self.csharp_metadata['column_lineages']:
                source_col = f"{lineage['source_table']}.{lineage['source_column']}"
                target_col = f"{lineage['target_table']}.{lineage['target_column']}"
                
                if source_col not in comprehensive_flows:
                    comprehensive_flows[source_col] = []
                comprehensive_flows[source_col].append(target_col)
        
        # Add dynamic bridge connections based on discovered patterns
        comprehensive_column_to_table = {}
        for col in comprehensive_flows.keys():
            if '.' in col:
                table = '.'.join(col.split('.')[:-1])
                comprehensive_column_to_table[col] = table
        
        for col_list in comprehensive_flows.values():
            for col in col_list:
                if '.' in col:
                    table = '.'.join(col.split('.')[:-1])
                    comprehensive_column_to_table[col] = table
        
        dynamic_bridges = self._discover_dynamic_bridges(comprehensive_flows, comprehensive_column_to_table, source_tables, final_target_tables)
        
        if dynamic_bridges:
            print(f"🔗 Adding {len(dynamic_bridges)} dynamic bridge connections...")
            for source_col, target_col in dynamic_bridges.items():
                if source_col not in comprehensive_flows:
                    comprehensive_flows[source_col] = []
                comprehensive_flows[source_col].append(target_col)

        # Build complete column-to-table mapping
        comprehensive_column_to_table = {}
        for col in comprehensive_flows.keys():
            if '.' in col:
                table = '.'.join(col.split('.')[:-1])
                comprehensive_column_to_table[col] = table
        
        for col_list in comprehensive_flows.values():
            for col in col_list:
                if '.' in col:
                    table = '.'.join(col.split('.')[:-1])
                    comprehensive_column_to_table[col] = table
        
        print(f"🔍 Built comprehensive flow map with {len(comprehensive_flows)} source columns")
        print(f"🔍 Mapped {len(comprehensive_column_to_table)} columns to tables")
        
        def find_complete_paths_to_finals(start_col, visited=None, path=None):
            """Recursively find complete paths from a column to C# identified final target tables"""
            if visited is None:
                visited = set()
            if path is None:
                path = []
            
            if start_col in visited:
                return []
            
            visited.add(start_col)
            current_path = path + [start_col]
            all_complete_paths = []
            
            # Check if this column is in a C# identified final target table
            if start_col in comprehensive_column_to_table:
                table = comprehensive_column_to_table[start_col]
                if table in final_target_tables:
                    return [current_path]
            
            # Continue tracing through all connections
            for next_col in comprehensive_flows.get(start_col, []):
                sub_paths = find_complete_paths_to_finals(next_col, visited.copy(), current_path)
                all_complete_paths.extend(sub_paths)
            
            return all_complete_paths
        
        # Find end-to-end paths from original source tables to final target tables
        print(f"🔍 Scanning {len(comprehensive_flows)} source columns for original sources...")
        original_source_count = 0
        
        for source_col in comprehensive_flows.keys():
            if source_col in comprehensive_column_to_table:
                source_table = comprehensive_column_to_table[source_col]
                
                # Check if this is an original source table (not temp/intermediate)
                if (source_table in source_tables and 
                    not source_table.startswith('#') and 
                    not source_table.startswith('<default>.#') and
                    source_table not in ['x', 'r', 'a', 'j', 'joinmap']):
                    
                    print(f"🔍 Found original source column: {source_col} from table {source_table}")
                    original_source_count += 1
                    
                    complete_paths = find_complete_paths_to_finals(source_col)
                    print(f"   → Found {len(complete_paths)} complete paths")
                    
                    for complete_path in complete_paths:
                        if len(complete_path) >= 2:  # Must have at least source and target
                            source_full = complete_path[0]
                            target_full = complete_path[-1]
                            
                            # Parse source and target
                            source_parts = source_full.split('.')
                            target_parts = target_full.split('.')
                            
                            if len(source_parts) >= 2 and len(target_parts) >= 2:
                                source_table_name = '.'.join(source_parts[:-1])
                                source_column_name = source_parts[-1]
                                target_table_name = '.'.join(target_parts[:-1])
                                target_column_name = target_parts[-1]
                                
                                # Create intermediate steps summary
                                intermediate_tables = []
                                for step in complete_path[1:-1]:  # Skip source and target
                                    if '.' in step:
                                        step_table = '.'.join(step.split('.')[:-1])
                                        if step_table not in intermediate_tables:
                                            intermediate_tables.append(step_table)
                                
                                end_to_end_mappings.append({
                                    'source_table': source_table_name,
                                    'source_column': source_column_name,
                                    'target_table': target_table_name,
                                    'target_column': target_column_name,
                                    'steps': len(complete_path),
                                    'path': ' → '.join(complete_path),
                                    'intermediate_tables': intermediate_tables
                                })
                                
                                print(f"✅ Complete lineage: {source_full} → {target_full} (via {len(intermediate_tables)} intermediate tables)")
        
        print(f"🔍 Found {original_source_count} original source columns to trace")
        
        print(f"✅ C# + Python hybrid traced {len(end_to_end_mappings)} unique end-to-end column lineages")
        
        # Remove duplicates
        seen_mappings = set()
        unique_mappings = []
        
        for mapping in end_to_end_mappings:
            key = (mapping['source_table'], mapping['source_column'], 
                   mapping['target_table'], mapping['target_column'])
            if key not in seen_mappings:
                seen_mappings.add(key)
                unique_mappings.append(mapping)
        
        self.end_to_end_mappings = unique_mappings
        return self.end_to_end_mappings


def main():
    """Command line interface for the hybrid parser"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Hybrid SQL Lineage Parser (C# + Python)')
    parser.add_argument('sql_file', help='Path to SQL file to analyze')
    parser.add_argument('--export', '-e', choices=['json'], default=None,
                       help='Export results to file format')
    parser.add_argument('--output', '-o', help='Output file path')
    
    args = parser.parse_args()
    
    try:
        # Create hybrid parser and analyze
        hybrid_parser = HybridSQLLineageParser(args.sql_file)
        results = hybrid_parser.analyze()
        
        # Export if requested
        if args.export and hybrid_parser.python_parser:
            hybrid_parser.python_parser.export_results(args.export, args.output)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    # If called directly without command line args, use the test file
    import sys
    if len(sys.argv) == 1:
        hybrid_parser = HybridSQLLineageParser("test.sql")
        hybrid_parser.analyze()
    else:
        exit(main())
