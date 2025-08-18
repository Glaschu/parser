#!/usr/bin/env python3
"""
End-to-End SQL Column Lineage Tracer
=====================================

This script traces complete end-to-end column lineage from staging sources 
to final target tables by combining C# ScriptDom results with Python sqllineage
and implementing graph-based path finding algorithms.

Focus: Find true business lineage like "staging.transactions.srcid → core.ledgerfinal.idempotencykey"
"""

import json
import sys
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque

class EndToEndLineageTracer:
    def __init__(self, csharp_metadata_file: str = "csharp_metadata.json", schema_file: str = "schema.json"):
        """Initialize the end-to-end lineage tracer"""
        print("🚀 Initializing End-to-End SQL Column Lineage Tracer")
        print("=" * 80)
        
        # Load C# metadata
        try:
            with open(csharp_metadata_file, 'r') as f:
                self.csharp_metadata = json.load(f)
            print(f"✅ Loaded C# metadata from {csharp_metadata_file}")
        except Exception as e:
            print(f"❌ Error loading C# metadata: {e}")
            sys.exit(1)
        
        # Load schema
        try:
            with open(schema_file, 'r') as f:
                self.schema = json.load(f)
            print(f"✅ Loaded schema from {schema_file}")
        except Exception as e:
            print(f"❌ Error loading schema: {e}")
            sys.exit(1)
        
        # Initialize lineage graph
        self.lineage_graph = defaultdict(list)  # source -> [targets]
        self.reverse_graph = defaultdict(list)  # target -> [sources]
        self.column_metadata = {}  # column -> metadata
        
        print(f"📊 C# Analysis found:")
        real_to_real = self.csharp_metadata.get('column_lineages', {}).get('real_to_real', [])
        temp_involved = self.csharp_metadata.get('column_lineages', {}).get('temp_involved', [])
        print(f"   • {len(real_to_real)} real-to-real lineages")
        print(f"   • {len(temp_involved)} temp-involved lineages")
        print(f"   • {len(self.schema)} tables in schema")
        print("")
    
    def build_lineage_graph(self):
        """Build a comprehensive lineage graph from all available sources"""
        print("🔧 Building comprehensive lineage graph...")
        
        # Add real-to-real lineages
        real_lineages = self.csharp_metadata.get('column_lineages', {}).get('real_to_real', [])
        for lineage in real_lineages:
            source = f"{lineage['source_table']}.{lineage['source_column']}"
            target = f"{lineage['target_table']}.{lineage['target_column']}"
            
            self.lineage_graph[source].append(target)
            self.reverse_graph[target].append(source)
            
            # Store metadata
            self.column_metadata[source] = {
                'table': lineage['source_table'],
                'column': lineage['source_column'],
                'type': 'real_source'
            }
            self.column_metadata[target] = {
                'table': lineage['target_table'],
                'column': lineage['target_column'],
                'type': 'real_target'
            }
        
        print(f"   ✅ Added {len(real_lineages)} real-to-real lineages")
        
        # Add temp-involved lineages (these are crucial for end-to-end tracing)
        temp_lineages = self.csharp_metadata.get('column_lineages', {}).get('temp_involved', [])
        for lineage in temp_lineages:
            source = f"{lineage['source_table']}.{lineage['source_column']}"
            target = f"{lineage['target_table']}.{lineage['target_column']}"
            
            self.lineage_graph[source].append(target)
            self.reverse_graph[target].append(source)
            
            # Determine if source/target are real or temp
            source_is_real = lineage['source_table'] in self._get_real_tables()
            target_is_real = lineage['target_table'] in self._get_real_tables()
            
            self.column_metadata[source] = {
                'table': lineage['source_table'],
                'column': lineage['source_column'],
                'type': 'real_source' if source_is_real else 'temp_source'
            }
            self.column_metadata[target] = {
                'table': lineage['target_table'],
                'column': lineage['target_column'],
                'type': 'real_target' if target_is_real else 'temp_target'
            }
        
        print(f"   ✅ Added {len(temp_lineages)} temp-involved lineages")
        print(f"   📈 Total graph nodes: {len(self.column_metadata)}")
        print(f"   🔗 Total graph edges: {sum(len(targets) for targets in self.lineage_graph.values())}")
        print("")
    
    def _get_real_tables(self) -> Set[str]:
        """Get all real table names from C# metadata"""
        real_tables = set()
        
        # Add real source tables
        real_sources = self.csharp_metadata.get('source_tables', {}).get('real_tables', [])
        real_tables.update(real_sources)
        
        # Add real target tables
        real_targets = self.csharp_metadata.get('target_tables', {}).get('real_tables', [])
        real_tables.update(real_targets)
        
        return real_tables
    
    def find_staging_sources(self) -> List[str]:
        """Find all columns from staging tables"""
        staging_columns = []
        
        for column, metadata in self.column_metadata.items():
            table = metadata['table']
            if table.lower().startswith('staging.'):
                staging_columns.append(column)
        
        return staging_columns
    
    def find_final_targets(self) -> List[str]:
        """Find all columns in final target tables (not temp/intermediate)"""
        final_columns = []
        
        for column, metadata in self.column_metadata.items():
            if metadata['type'] == 'real_target':
                table = metadata['table']
                # Consider core.* and other non-staging, non-temp tables as final
                if not table.lower().startswith('staging.') and not table.startswith('#'):
                    final_columns.append(column)
        
        return final_columns
    
    def trace_end_to_end_path(self, source_column: str, target_column: str, max_depth: int = 10) -> Optional[List[str]]:
        """
        Find a path from source column to target column using BFS
        Returns the path as a list of columns, or None if no path exists
        Enhanced to search deeper for complex transformations
        """
        if source_column == target_column:
            return [source_column]
        
        # BFS to find shortest path with depth limit
        queue = deque([(source_column, [source_column], 0)])
        visited = {source_column}
        
        while queue:
            current_column, path, depth = queue.popleft()
            
            # Skip if we've gone too deep
            if depth >= max_depth:
                continue
            
            # Explore all targets of current column
            for next_column in self.lineage_graph.get(current_column, []):
                if next_column == target_column:
                    return path + [next_column]
                
                if next_column not in visited:
                    visited.add(next_column)
                    queue.append((next_column, path + [next_column], depth + 1))
        
        return None  # No path found
    
    def find_sample_paths(self, max_samples: int = 5) -> List[Dict]:
        """Find some sample paths to understand the data flow patterns"""
        print("🔍 Searching for sample paths to understand data flow...")
        
        staging_sources = self.find_staging_sources()
        final_targets = self.find_final_targets()
        
        sample_paths = []
        attempts = 0
        max_attempts = min(50, len(staging_sources) * len(final_targets))
        
        # Try some combinations
        import itertools
        for source_col, target_col in itertools.islice(
            itertools.product(staging_sources, final_targets), max_attempts
        ):
            attempts += 1
            path = self.trace_end_to_end_path(source_col, target_col, max_depth=15)
            if path and len(path) > 1:
                sample_paths.append({
                    'source': source_col,
                    'target': target_col,
                    'path': path,
                    'length': len(path)
                })
                
                if len(sample_paths) >= max_samples:
                    break
        
        print(f"   Attempted {attempts} combinations, found {len(sample_paths)} paths")
        return sample_paths
    
    def display_sample_paths(self, sample_paths: List[Dict]):
        """Display sample paths to show the transformation patterns"""
        if not sample_paths:
            print("   No sample paths found with current search depth")
            return
        
        print(f"\n🎯 SAMPLE END-TO-END PATHS FOUND ({len(sample_paths)} examples):")
        print("─" * 80)
        
        for i, path_info in enumerate(sample_paths[:3], 1):  # Show first 3
            path = path_info['path']
            print(f"\n{i}. {path_info['source']} → {path_info['target']}")
            print(f"   Length: {path_info['length']} steps")
            print("   Path:")
            for j, step in enumerate(path):
                table = self.column_metadata[step]['table']
                column = self.column_metadata[step]['column']
                indent = "     " if j > 0 else "   → "
                arrow = " → " if j < len(path) - 1 else ""
                print(f"{indent}{table}.{column}{arrow}")
        
        if len(sample_paths) > 3:
            print(f"\n   ... and {len(sample_paths) - 3} more paths found")
        print("")
        """Find all complete end-to-end lineages from staging sources to final targets"""
        print("🎯 Tracing complete end-to-end column lineages...")
        
        staging_sources = self.find_staging_sources()
        final_targets = self.find_final_targets()
        
        print(f"   🏁 Found {len(staging_sources)} staging source columns")
        print(f"   🎯 Found {len(final_targets)} final target columns")
        print("")
        
        end_to_end_lineages = []
        paths_found = 0
        
        # Try to find paths from each staging source to each final target
        for source_col in staging_sources:
            for target_col in final_targets:
                path = self.trace_end_to_end_path(source_col, target_col)
                if path and len(path) > 1:  # Must have at least 2 nodes (source and target)
                    lineage = {
                        'source_column': source_col,
                        'target_column': target_col,
                        'source_table': self.column_metadata[source_col]['table'],
                        'target_table': self.column_metadata[target_col]['table'],
                        'path': path,
                        'steps': len(path) - 1,
                        'intermediate_steps': path[1:-1] if len(path) > 2 else []
                    }
                    end_to_end_lineages.append(lineage)
                    paths_found += 1
        
        print(f"   ✅ Found {paths_found} complete end-to-end lineage paths!")
        print("")
        
        return end_to_end_lineages
    
    def display_end_to_end_lineages(self, lineages: List[Dict]):
        """Display the end-to-end lineages in a clear format"""
        print("🎯 " + "=" * 90)
        print("   COMPLETE END-TO-END COLUMN LINEAGE (Staging → Final)")
        print("=" * 92)
        
        if not lineages:
            print("❌ No complete end-to-end lineages found")
            print("   This might indicate:")
            print("   • Complex transformations through multiple temp tables")
            print("   • Missing intermediate connections in the SQL parsing")
            print("   • Business logic that requires manual mapping")
            print("   • Indirect relationships through reference data")
            print("")
            print("💡 Recommendation: Check temp-involved lineages for intermediate steps")
            return
        
        print(f"🎉 Found {len(lineages)} complete end-to-end column lineages!")
        print("")
        
        # Sort by source table and column for better organization
        lineages_sorted = sorted(lineages, key=lambda x: (x['source_table'], x['source_column']))
        
        # Group by target table
        by_target_table = defaultdict(list)
        for lineage in lineages_sorted:
            by_target_table[lineage['target_table']].append(lineage)
        
        for target_table in sorted(by_target_table.keys()):
            table_lineages = by_target_table[target_table]
            print(f"📋 TARGET TABLE: {target_table}")
            print("─" * 60)
            
            for lineage in table_lineages:
                source_col = lineage['source_column']
                target_col = lineage['target_column']
                steps = lineage['steps']
                intermediate_steps = lineage['intermediate_steps']
                
                print(f"   🎯 {source_col} → {target_col}")
                
                if intermediate_steps:
                    intermediate_str = ' → '.join([col.split('.')[-1] for col in intermediate_steps])
                    print(f"       via: {intermediate_str}")
                
                print(f"       steps: {steps}")
                print("")
        
        # Summary
        print("📊 " + "=" * 90)
        print("   END-TO-END LINEAGE SUMMARY")
        print("=" * 92)
        print(f"   Total end-to-end lineages: {len(lineages)}")
        print(f"   Unique source tables: {len(set(l['source_table'] for l in lineages))}")
        print(f"   Unique target tables: {len(set(l['target_table'] for l in lineages))}")
        
        if lineages:
            avg_steps = sum(l['steps'] for l in lineages) / len(lineages)
            print(f"   Average transformation steps: {avg_steps:.1f}")
            
            complex_lineages = [l for l in lineages if l['steps'] > 3]
            if complex_lineages:
                print(f"   Complex transformations (>3 steps): {len(complex_lineages)}")
        
        print("=" * 92)
    
    def display_diagnostic_info(self):
        """Display diagnostic information to help understand the data flow"""
        print("🔍 " + "=" * 90)
        print("   DIAGNOSTIC INFORMATION")
        print("=" * 92)
        
        # Show staging sources
        staging_sources = self.find_staging_sources()
        print(f"📥 STAGING SOURCES ({len(staging_sources)} columns):")
        by_table = defaultdict(list)
        for col in staging_sources:
            table = self.column_metadata[col]['table']
            column_name = self.column_metadata[col]['column']
            by_table[table].append(column_name)
        
        for table in sorted(by_table.keys()):
            columns = sorted(by_table[table])
            print(f"   • {table}: {', '.join(columns)}")
        print("")
        
        # Show what staging sources connect to (first hops)
        print("🔗 STAGING CONNECTIONS (First Hops):")
        staging_connections = 0
        for staging_col in staging_sources[:3]:  # Show first 3 for brevity
            targets = self.lineage_graph.get(staging_col, [])
            if targets:
                staging_connections += len(targets)
                target_tables = set(self.column_metadata[t]['table'] for t in targets)
                print(f"   • {staging_col} → {len(targets)} targets in tables: {', '.join(sorted(target_tables))}")
        
        total_staging_connections = sum(len(self.lineage_graph.get(col, [])) for col in staging_sources)
        print(f"   📊 Total staging outbound connections: {total_staging_connections}")
        print("")
        
        # Show final targets
        final_targets = self.find_final_targets()
        print(f"🎯 FINAL TARGETS ({len(final_targets)} columns):")
        by_table = defaultdict(list)
        for col in final_targets:
            table = self.column_metadata[col]['table']
            column_name = self.column_metadata[col]['column']
            by_table[table].append(column_name)
        
        for table in sorted(by_table.keys()):
            columns = sorted(by_table[table])
            print(f"   • {table}: {', '.join(columns)}")
        print("")
        
        # Show what connects to final targets (reverse hops)
        print("🔙 FINAL TARGET CONNECTIONS (Reverse Hops):")
        for final_col in list(final_targets)[:3]:  # Show first 3 for brevity
            sources = self.reverse_graph.get(final_col, [])
            if sources:
                source_tables = set(self.column_metadata[s]['table'] for s in sources)
                print(f"   • {final_col} ← {len(sources)} sources from tables: {', '.join(sorted(source_tables))}")
        
        total_final_connections = sum(len(self.reverse_graph.get(col, [])) for col in final_targets)
        print(f"   📊 Total final target inbound connections: {total_final_connections}")
        print("")
        
        # Show intermediate temp tables
        temp_tables = set()
        for col, metadata in self.column_metadata.items():
            if metadata['type'] in ['temp_source', 'temp_target']:
                temp_tables.add(metadata['table'])
        
        print(f"⚙️  INTERMEDIATE TEMP TABLES ({len(temp_tables)} tables):")
        for table in sorted(temp_tables):
            print(f"   • {table}")
        print("")
        
        # Show some potential bridge connections
        print("🌉 POTENTIAL BRIDGE ANALYSIS:")
        print("   Looking for temp tables that might bridge staging to final...")
        
        # Find temp tables that receive from staging
        staging_to_temp = set()
        for staging_col in staging_sources:
            for target in self.lineage_graph.get(staging_col, []):
                target_table = self.column_metadata[target]['table']
                if target_table.startswith('#') or target_table in ['src', 'x', 'a']:
                    staging_to_temp.add(target_table)
        
        # Find temp tables that feed to final
        temp_to_final = set()
        for final_col in final_targets:
            for source in self.reverse_graph.get(final_col, []):
                source_table = self.column_metadata[source]['table']
                if source_table.startswith('#') or source_table in ['src', 'x', 'a']:
                    temp_to_final.add(source_table)
        
        bridge_tables = staging_to_temp.intersection(temp_to_final)
        
        print(f"   • Temp tables receiving from staging: {len(staging_to_temp)}")
        print(f"     {sorted(staging_to_temp)}")
        print(f"   • Temp tables feeding to final: {len(temp_to_final)}")
        print(f"     {sorted(temp_to_final)}")
        print(f"   • Bridge temp tables (both): {len(bridge_tables)}")
        print(f"     {sorted(bridge_tables)}")
        print("")
        
        print("=" * 92)
    
    def run_analysis(self):
        """Run the complete end-to-end lineage analysis"""
        # Step 1: Build the lineage graph
        self.build_lineage_graph()
        
        # Step 2: Display diagnostic information
        self.display_diagnostic_info()
        
        # Step 3: Find sample paths to understand flow patterns
        sample_paths = self.find_sample_paths(max_samples=10)
        self.display_sample_paths(sample_paths)
        
        # Step 4: Find all end-to-end lineages
        end_to_end_lineages = self.find_all_end_to_end_lineages()
        
        # Step 5: Display the results
        self.display_end_to_end_lineages(end_to_end_lineages)
        
        return end_to_end_lineages

def main():
    """Main execution function"""
    print("🎯 End-to-End SQL Column Lineage Tracer")
    print("========================================")
    print("Tracing complete data flow from staging sources to final targets...")
    print("")
    
    # Initialize and run the tracer
    tracer = EndToEndLineageTracer()
    lineages = tracer.run_analysis()
    
    print(f"\n✅ Analysis complete! Found {len(lineages)} end-to-end lineage paths.")
    
    if lineages:
        print("\n🎉 SUCCESS: End-to-end lineage tracing completed successfully!")
        print("   The lineages above show complete data flow from staging sources")
        print("   through all intermediate transformations to final target tables.")
    else:
        print("\n⚠️  No direct end-to-end paths found, but this is normal for complex SQL.")
        print("   The diagnostic information above shows all the components that")
        print("   could be connected through additional analysis or business rules.")

if __name__ == "__main__":
    main()
