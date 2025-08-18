#!/usr/bin/env python3
"""
Lineage Analyzer - End-to-End Column Lineage from C# Metadata
============================================================

This script analyzes the C# metadata JSON to generate complete end-to-end 
lineage paths showing both direct and indirect column dependencies.

Features:
- Traces ultimate sources to ultimate targets
- Shows complete transformation chains
- Categorizes lineages (real-to-real, temp-involved)
- Finds shortest and alternative paths
- Provides comprehensive lineage statistics
"""

import json
import sys
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple, Optional


class LineageAnalyzer:
    def __init__(self, metadata_file: str):
        """Initialize the lineage analyzer with C# metadata."""
        print("🔍 Initializing Lineage Analyzer")
        print("=" * 50)
        
        try:
            with open(metadata_file, 'r') as f:
                self.metadata = json.load(f)
            print(f"✅ Loaded metadata from {metadata_file}")
        except Exception as e:
            print(f"❌ Error loading metadata: {e}")
            sys.exit(1)
        
        # Initialize data structures
        self.forward_graph = defaultdict(set)  # source -> targets
        self.reverse_graph = defaultdict(set)  # target -> sources
        self.all_columns = set()
        
        # Build the lineage graph
        self._build_lineage_graph()
        
        print(f"📊 Built lineage graph with {len(self.all_columns)} columns")
        print(f"🔗 Forward edges: {sum(len(targets) for targets in self.forward_graph.values())}")
        print("")

    def _build_lineage_graph(self):
        """Build forward and reverse lineage graphs from metadata."""
        lineages = []
        
        # Collect all lineages
        column_lineages = self.metadata.get('column_lineages', {})
        
        # Add real-to-real lineages
        if 'real_to_real' in column_lineages:
            lineages.extend(column_lineages['real_to_real'])
        
        # Add temp-involved lineages (filter out incomplete ones)
        if 'temp_involved' in column_lineages:
            for lineage in column_lineages['temp_involved']:
                if (lineage.get('source_table') and lineage.get('source_column') and 
                    lineage.get('target_table') and lineage.get('target_column')):
                    lineages.append(lineage)
        
        # Build graphs
        for lineage in lineages:
            source = f"{lineage['source_table']}.{lineage['source_column']}"
            target = f"{lineage['target_table']}.{lineage['target_column']}"
            
            self.forward_graph[source].add(target)
            self.reverse_graph[target].add(source)
            self.all_columns.add(source)
            self.all_columns.add(target)

    def _is_temp_table(self, table_name: str) -> bool:
        """Check if a table is temporary or CTE."""
        temp_indicators = [
            '#', 'x', 'j', 'a', 'r', 'scores', 'feerule', 'feecalc', 'bal',
            'needcheck', 'slice', 'map', 'src', 'joinmap', 'net'
        ]
        
        return (table_name.startswith('#') or 
                table_name.lower() in [t.lower() for t in temp_indicators])

    def _categorize_lineage(self, source: str, target: str) -> str:
        """Categorize the lineage type."""
        source_table = source.split('.')[0]
        target_table = target.split('.')[0]
        
        source_is_temp = self._is_temp_table(source_table)
        target_is_temp = self._is_temp_table(target_table)
        
        if not source_is_temp and not target_is_temp:
            return "real_to_real"
        elif not source_is_temp and target_is_temp:
            return "real_to_temp"
        elif source_is_temp and not target_is_temp:
            return "temp_to_real"
        else:
            return "temp_to_temp"

    def find_ultimate_sources(self, column: str, visited: Optional[Set[str]] = None) -> Set[str]:
        """Find all ultimate source columns (recursively)."""
        if visited is None:
            visited = set()
        
        if column in visited:
            return set()  # Cycle detection
        
        visited.add(column)
        
        # If no incoming edges, this is an ultimate source
        if column not in self.reverse_graph:
            return {column}
        
        ultimate_sources = set()
        for source in self.reverse_graph[column]:
            ultimate_sources.update(self.find_ultimate_sources(source, visited.copy()))
        
        return ultimate_sources

    def find_ultimate_targets(self, column: str, visited: Optional[Set[str]] = None) -> Set[str]:
        """Find all ultimate target columns (recursively)."""
        if visited is None:
            visited = set()
        
        if column in visited:
            return set()  # Cycle detection
        
        visited.add(column)
        
        # If no outgoing edges, this is an ultimate target
        if column not in self.forward_graph:
            return {column}
        
        ultimate_targets = set()
        for target in self.forward_graph[column]:
            ultimate_targets.update(self.find_ultimate_targets(target, visited.copy()))
        
        return ultimate_targets

    def find_paths(self, source: str, target: str, max_depth: int = 15) -> List[List[str]]:
        """Find all paths from source to target using BFS."""
        if source == target:
            return [[source]]
        
        paths = []
        queue = deque([(source, [source])])
        
        while queue:
            current, path = queue.popleft()
            
            if len(path) > max_depth:
                continue
            
            if current in self.forward_graph:
                for next_col in self.forward_graph[current]:
                    if next_col in path:  # Avoid cycles
                        continue
                    
                    new_path = path + [next_col]
                    
                    if next_col == target:
                        paths.append(new_path)
                    else:
                        queue.append((next_col, new_path))
        
        return paths

    def generate_end_to_end_lineages(self) -> Dict[str, List[Dict]]:
        """Generate comprehensive end-to-end lineages."""
        print("🎯 Generating End-to-End Lineages")
        print("=" * 40)
        
        results = {
            "direct_lineages": [],
            "indirect_lineages": [],
            "real_to_real": [],
            "temp_involved": [],
            "statistics": {}
        }
        
        # Find ultimate sources and targets
        ultimate_sources = set()
        ultimate_targets = set()
        
        for column in self.all_columns:
            table = column.split('.')[0]
            
            # Ultimate sources (no incoming edges)
            if column not in self.reverse_graph:
                ultimate_sources.add(column)
            
            # Ultimate targets (no outgoing edges)
            if column not in self.forward_graph:
                ultimate_targets.add(column)
        
        print(f"📍 Found {len(ultimate_sources)} ultimate sources")
        print(f"🎯 Found {len(ultimate_targets)} ultimate targets")
        print("")
        
        # Generate lineages
        total_lineages = 0
        direct_count = 0
        indirect_count = 0
        
        for source in ultimate_sources:
            for target in ultimate_targets:
                paths = self.find_paths(source, target)
                
                if paths:
                    # Get the shortest path
                    shortest_path = min(paths, key=len)
                    is_direct = len(shortest_path) == 2
                    
                    lineage = {
                        "source": source,
                        "target": target,
                        "source_table": source.split('.')[0],
                        "source_column": source.split('.', 1)[1],
                        "target_table": target.split('.')[0],
                        "target_column": target.split('.', 1)[1],
                        "category": self._categorize_lineage(source, target),
                        "is_direct": is_direct,
                        "shortest_path": shortest_path,
                        "path_length": len(shortest_path) - 1,
                        "all_paths": paths[:5],  # Limit to first 5 paths
                        "path_count": len(paths)
                    }
                    
                    # Categorize
                    if is_direct:
                        results["direct_lineages"].append(lineage)
                        direct_count += 1
                    else:
                        results["indirect_lineages"].append(lineage)
                        indirect_count += 1
                    
                    if lineage["category"] == "real_to_real":
                        results["real_to_real"].append(lineage)
                    else:
                        results["temp_involved"].append(lineage)
                    
                    total_lineages += 1
        
        # Generate statistics
        results["statistics"] = {
            "total_lineages": total_lineages,
            "direct_lineages": direct_count,
            "indirect_lineages": indirect_count,
            "real_to_real_count": len(results["real_to_real"]),
            "temp_involved_count": len(results["temp_involved"]),
            "ultimate_sources": len(ultimate_sources),
            "ultimate_targets": len(ultimate_targets),
            "total_columns": len(self.all_columns)
        }
        
        print(f"✅ Generated {total_lineages} end-to-end lineages")
        print(f"   • Direct: {direct_count}")
        print(f"   • Indirect: {indirect_count}")
        print(f"   • Real-to-Real: {len(results['real_to_real'])}")
        print(f"   • Temp-Involved: {len(results['temp_involved'])}")
        print("")
        
        return results

    def display_lineages(self, results: Dict, max_display: int = 10):
        """Display lineages in a formatted way."""
        print("📋 END-TO-END LINEAGE RESULTS")
        print("=" * 60)
        
        categories = [
            ("Real-to-Real Direct", [l for l in results["real_to_real"] if l["is_direct"]]),
            ("Real-to-Real Indirect", [l for l in results["real_to_real"] if not l["is_direct"]]),
            ("Temp-Involved Direct", [l for l in results["temp_involved"] if l["is_direct"]]),
            ("Temp-Involved Indirect", [l for l in results["temp_involved"] if not l["is_direct"]])
        ]
        
        for category_name, lineages in categories:
            if not lineages:
                continue
                
            print(f"\n🔗 {category_name.upper()} ({len(lineages)} total)")
            print("-" * 50)
            
            for i, lineage in enumerate(lineages[:max_display]):
                print(f"\n{i+1}. {lineage['source']} → {lineage['target']}")
                print(f"   Path Length: {lineage['path_length']} steps")
                print(f"   Path: {' → '.join(lineage['shortest_path'])}")
                
                if lineage['path_count'] > 1:
                    print(f"   Alternative Paths: {lineage['path_count'] - 1} more")
            
            if len(lineages) > max_display:
                print(f"\n   ... and {len(lineages) - max_display} more lineages")

    def save_results(self, results: Dict, output_file: str):
        """Save results to JSON file."""
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"💾 Results saved to {output_file}")

    def display_summary(self, results: Dict):
        """Display a comprehensive summary."""
        stats = results["statistics"]
        
        print("\n" + "=" * 70)
        print("📊 LINEAGE ANALYSIS SUMMARY")
        print("=" * 70)
        print(f"Procedure: {self.metadata.get('procedure_name', 'Unknown')}")
        print(f"Analysis Date: {self.metadata.get('analysis_timestamp', 'Unknown')}")
        print("")
        print(f"📈 GRAPH STATISTICS:")
        print(f"   Total Columns: {stats['total_columns']}")
        print(f"   Ultimate Sources: {stats['ultimate_sources']}")
        print(f"   Ultimate Targets: {stats['ultimate_targets']}")
        print("")
        print(f"🔗 LINEAGE STATISTICS:")
        print(f"   Total End-to-End Lineages: {stats['total_lineages']}")
        print(f"   Direct Lineages: {stats['direct_lineages']}")
        print(f"   Indirect Lineages: {stats['indirect_lineages']}")
        print("")
        print(f"📋 CATEGORY BREAKDOWN:")
        print(f"   Real-to-Real: {stats['real_to_real_count']}")
        print(f"   Temp-Involved: {stats['temp_involved_count']}")
        print("=" * 70)


def main():
    """Main execution function."""
    print("🚀 Lineage Analyzer - End-to-End Column Lineage Generator")
    print("=" * 65)
    print("Analyzing C# metadata to generate comprehensive lineage chains...")
    print("")
    
    # Initialize analyzer
    metadata_file = "/Users/jamesglasgow/Projects/parser/csharp_metadata.json"
    analyzer = LineageAnalyzer(metadata_file)
    
    # Generate lineages
    results = analyzer.generate_end_to_end_lineages()
    
    # Display results
    analyzer.display_lineages(results, max_display=5)
    analyzer.display_summary(results)
    
    # Save results
    output_file = "/Users/jamesglasgow/Projects/parser/end_to_end_lineages_analysis.json"
    analyzer.save_results(results, output_file)
    
    print(f"\n🎉 Analysis Complete!")
    print(f"Found {results['statistics']['total_lineages']} end-to-end lineage relationships")
    

if __name__ == "__main__":
    main()
