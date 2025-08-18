import re
import sqlparse
from sqllineage.runner import LineageRunner
from collections import defaultdict
import argparse
import json
from pathlib import Path

class GenericSQLLineageParser:
    """
    Generic SQL Lineage Parser that works with any SQL script
    No hardcoded mappings - dynamically discovers all lineage relationships
    """
    
    def __init__(self, sql_file_path):
        self.sql_file_path = sql_file_path
        self.sql_content = self._read_sql_file()
        self.procedure_body = self._extract_procedure_body()
        
        # Dynamic discovery containers
        self.source_tables = {}
        self.target_tables = {}
        self.intermediate_tables = {}
        self.column_mappings = []
        self.table_relationships = defaultdict(set)
        self.processing_stages = []
        
    def _read_sql_file(self):
        """Read the SQL file content"""
        try:
            with open(self.sql_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Error reading SQL file: {e}")
    
    def _extract_procedure_body(self):
        """Extract procedure body if it's a stored procedure, otherwise return full content"""
        # For stored procedures, we need to match balanced BEGIN/END blocks
        
        # First, find the start of the procedure body
        proc_start_match = re.search(r'CREATE\s+PROCEDURE\s+[^\s]+.*?AS\s*BEGIN', self.sql_content, re.DOTALL | re.IGNORECASE)
        if not proc_start_match:
            print(f"ℹ️  Processing as general SQL script ({len(self.sql_content):,} characters)")
            return self.sql_content
        
        # Find the position after "AS BEGIN"
        start_pos = proc_start_match.end()
        
        # Find the matching END GO by counting nested BEGIN/END blocks
        begin_count = 1  # We already have one BEGIN from "AS BEGIN"
        pos = start_pos
        end_pos = None
        
        while pos < len(self.sql_content) and begin_count > 0:
            # Look for the next BEGIN or END
            begin_match = re.search(r'\bBEGIN\b', self.sql_content[pos:], re.IGNORECASE)
            end_match = re.search(r'\bEND\b', self.sql_content[pos:], re.IGNORECASE)
            
            if begin_match and (not end_match or begin_match.start() < end_match.start()):
                # Found BEGIN before END
                begin_count += 1
                pos += begin_match.end()
            elif end_match:
                # Found END
                begin_count -= 1
                pos += end_match.end()
                if begin_count == 0:
                    end_pos = start_pos + end_match.start()
                    break
            else:
                # No more BEGIN or END found
                break
        
        if end_pos:
            body = self.sql_content[start_pos:end_pos].strip()
            if len(body) > 100:
                print(f"✅ Detected stored procedure - extracted body ({len(body):,} characters)")
                return body
        
        print(f"ℹ️  Processing as general SQL script ({len(self.sql_content):,} characters)")
        return self.sql_content
    
    def _categorize_table(self, table_name):
        """Dynamically categorize tables based on naming patterns and usage"""
        table_lower = table_name.lower()
        
        # Common source table patterns
        source_patterns = [
            'staging', 'stage', 'src', 'source', 'raw', 'input', 'import',
            'ext', 'external', 'ref', 'reference', 'lookup', 'dim', 'fact'
        ]
        
        # Common target table patterns
        target_patterns = [
            'core', 'final', 'output', 'dest', 'destination', 'prod', 'production',
            'audit', 'log', 'history', 'archive', 'summary', 'agg', 'aggregate'
        ]
        
        # Intermediate/temporary table patterns
        intermediate_patterns = [
            '#', 'temp', 'tmp', 'work', 'staging', 'buffer', 'cache',
            'intermediate', 'process', 'transform', 'enrich', 'clean'
        ]
        
        # Check for intermediate first (most specific)
        if any(pattern in table_lower for pattern in intermediate_patterns):
            return 'intermediate'
        
        # Check for source patterns
        if any(pattern in table_lower for pattern in source_patterns):
            return 'source'
        
        # Check for target patterns
        if any(pattern in table_lower for pattern in target_patterns):
            return 'target'
        
        # Default categorization based on context will be done later
        return 'unknown'
    
    def _extract_table_references(self):
        """Extract all table references from SQL using regex patterns"""
        tables = {
            'from_tables': set(),
            'insert_tables': set(),
            'update_tables': set(),
            'join_tables': set(),
            'with_tables': set()
        }
        
        # Extract different types of table references
        patterns = {
            'from_tables': r'FROM\s+([#\w\.\[\]]+)',
            'insert_tables': r'INSERT\s+(?:INTO\s+)?([#\w\.\[\]]+)',
            'update_tables': r'UPDATE\s+([#\w\.\[\]]+)',
            'join_tables': r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+([#\w\.\[\]]+)',
            'with_tables': r'WITH\s+([#\w\.\[\]]+)\s+AS'
        }
        
        for pattern_type, pattern in patterns.items():
            matches = re.findall(pattern, self.procedure_body, re.IGNORECASE)
            for match in matches:
                # Clean up table name
                table_name = match.strip().replace('[', '').replace(']', '')
                # Remove common suffixes
                table_name = re.sub(r'\s+(?:WITH\s*\(NOLOCK\)|NOLOCK)', '', table_name, flags=re.IGNORECASE)
                tables[pattern_type].add(table_name.lower())
        
        return tables
    
    def _analyze_statements(self):
        """Analyze individual SQL statements for detailed lineage"""
        # Split by common SQL statement terminators
        statement_patterns = [
            r';',  # Standard SQL terminator
            r'\bGO\b',  # T-SQL batch separator
            r'(?<=\))\s*(?=\bWITH\b)',  # Before WITH clauses
            r'(?<=\))\s*(?=\bINSERT\b)',  # Before INSERT statements
            r'(?<=\))\s*(?=\bUPDATE\b)',  # Before UPDATE statements
            r'(?<=\))\s*(?=\bSELECT\b)',  # Before SELECT statements
        ]
        
        # Split the SQL content more intelligently
        statements = []
        
        # First try sqlparse
        try:
            statements = sqlparse.split(self.procedure_body)
        except:
            # Fallback to manual splitting
            statements = [self.procedure_body]
        
        dml_statements = []
        
        # Filter for relevant statements
        for stmt_str in statements:
            if stmt_str.strip():
                stmt_clean = stmt_str.strip()
                
                # Check if it's a DML statement by looking at keywords
                if re.search(r'\b(INSERT|UPDATE|MERGE|DELETE|WITH)\b', stmt_clean, re.IGNORECASE):
                    dml_statements.append(stmt_str)
                # Also include SELECT statements that seem to be part of INSERT/CREATE
                elif re.search(r'\b(SELECT)\b.*\b(INTO|FROM)\b', stmt_clean, re.IGNORECASE):
                    dml_statements.append(stmt_str)
        
        # If we still don't have enough statements, try to extract them differently
        if len(dml_statements) < 5:  # Heuristic: complex procedures should have more statements
            # Look for patterns that indicate statement boundaries
            pattern = r'((?:INSERT|UPDATE|MERGE|DELETE|WITH)\s+(?:[^;]|;(?!\s*(?:INSERT|UPDATE|MERGE|DELETE|WITH|$)))*)'
            additional_statements = re.findall(pattern, self.procedure_body, re.IGNORECASE | re.DOTALL)
            dml_statements.extend(additional_statements)
        
        print(f"🔍 Found {len(dml_statements)} DML statements to analyze")
        
        # If we found very few statements, show a sample for debugging
        if len(dml_statements) < 3:
            print("⚠️  Few DML statements detected. Showing first 500 chars of content:")
            print(f"   {self.procedure_body[:500]}...")
        
        # Analyze each statement
        for i, stmt in enumerate(dml_statements):
            try:
                # Clean statement for better parsing
                clean_stmt = re.sub(r'@[a-zA-Z_]\w*', "'placeholder_value'", stmt)
                clean_stmt = re.sub(r'--.*?$', '', clean_stmt, flags=re.MULTILINE)  # Remove comments
                clean_stmt = re.sub(r'/\*.*?\*/', '', clean_stmt, flags=re.DOTALL)  # Remove block comments
                
                # Skip very short statements
                if len(clean_stmt.strip()) < 20:
                    continue
                
                # Use sqllineage to get detailed lineage
                result = LineageRunner(clean_stmt, dialect="tsql")
                column_lineage = result.get_column_lineage()
                source_tables = result.source_tables
                target_tables = result.target_tables
                intermediate_tables = getattr(result, 'intermediate_tables', [])
                
                # Record processing stage
                stage_info = {
                    'statement_num': i + 1,
                    'statement_type': self._get_statement_type(stmt),
                    'source_tables': [str(t) for t in source_tables],
                    'target_tables': [str(t) for t in target_tables],
                    'intermediate_tables': [str(t) for t in intermediate_tables],
                    'column_mappings_count': len(column_lineage)
                }
                self.processing_stages.append(stage_info)
                
                # Record table relationships
                for source in source_tables:
                    for target in target_tables:
                        self.table_relationships[str(source)].add(str(target))
                
                # Record column mappings
                for mapping in column_lineage:
                    if mapping and len(mapping) >= 2:
                        self.column_mappings.append({
                            'source_column': str(mapping[0]) if mapping[0] else 'unknown',
                            'target_column': str(mapping[-1]) if mapping[-1] else 'unknown',
                            'full_path': [str(m) for m in mapping if m],
                            'statement_num': i + 1,
                            'transformation_steps': len(mapping) - 1
                        })
                
                # Categorize tables dynamically
                all_tables = list(source_tables) + list(target_tables) + list(intermediate_tables)
                for table in all_tables:
                    table_name = str(table)
                    category = self._categorize_table(table_name)
                    
                    if category == 'source':
                        if table_name not in self.source_tables:
                            self.source_tables[table_name] = {'columns': set(), 'usage_count': 0}
                        self.source_tables[table_name]['usage_count'] += 1
                    elif category == 'target':
                        if table_name not in self.target_tables:
                            self.target_tables[table_name] = {'columns': set(), 'usage_count': 0}
                        self.target_tables[table_name]['usage_count'] += 1
                    elif category == 'intermediate':
                        if table_name not in self.intermediate_tables:
                            self.intermediate_tables[table_name] = {'columns': set(), 'usage_count': 0}
                        self.intermediate_tables[table_name]['usage_count'] += 1
                
                # Extract column information
                for mapping in column_lineage:
                    if mapping and len(mapping) >= 2:
                        source_col = mapping[0]
                        target_col = mapping[-1]
                        
                        if source_col and hasattr(source_col, 'parent'):
                            parent_table = str(source_col.parent)
                            col_name = str(source_col).split('.')[-1] if '.' in str(source_col) else str(source_col)
                            
                            for table_dict in [self.source_tables, self.target_tables, self.intermediate_tables]:
                                if parent_table in table_dict:
                                    table_dict[parent_table]['columns'].add(col_name)
                        
                        if target_col and hasattr(target_col, 'parent'):
                            parent_table = str(target_col.parent)
                            col_name = str(target_col).split('.')[-1] if '.' in str(target_col) else str(target_col)
                            
                            for table_dict in [self.source_tables, self.target_tables, self.intermediate_tables]:
                                if parent_table in table_dict:
                                    table_dict[parent_table]['columns'].add(col_name)
                
            except Exception as e:
                print(f"   ⚠️ Error processing statement {i+1}: {str(e)[:80]}...")
                continue
    
    def _get_statement_type(self, stmt):
        """Determine the type of SQL statement"""
        stmt_upper = stmt.strip().upper()
        if stmt_upper.startswith('INSERT'):
            return 'INSERT'
        elif stmt_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif stmt_upper.startswith('MERGE'):
            return 'MERGE'
        elif stmt_upper.startswith('DELETE'):
            return 'DELETE'
        elif stmt_upper.startswith('WITH'):
            return 'CTE'
        else:
            return 'OTHER'
    
    def _refine_table_categorization(self):
        """Refine table categorization based on usage patterns"""
        table_refs = self._extract_table_references()
        
        # Tables that only appear in FROM/JOIN are likely sources
        for table in table_refs['from_tables'].union(table_refs['join_tables']):
            if table not in table_refs['insert_tables'] and table not in table_refs['update_tables']:
                if table not in self.source_tables and not table.startswith('#'):
                    self.source_tables[table] = {'columns': set(), 'usage_count': 1}
        
        # Tables that appear in INSERT/UPDATE are likely targets
        for table in table_refs['insert_tables'].union(table_refs['update_tables']):
            if not table.startswith('#') and 'temp' not in table.lower():
                if table not in self.target_tables:
                    self.target_tables[table] = {'columns': set(), 'usage_count': 1}
        
        # WITH clause tables are intermediate
        for table in table_refs['with_tables']:
            if table not in self.intermediate_tables:
                self.intermediate_tables[table] = {'columns': set(), 'usage_count': 1}
    
    def analyze(self):
        """Main analysis method"""
        print("🔍 " + "=" * 100)
        print("   GENERIC SQL LINEAGE ANALYSIS")
        print(f"   File: {self.sql_file_path}")
        print("=" * 102)
        
        # Step 1: Extract table references
        print("📋 Step 1: Extracting table references...")
        table_refs = self._extract_table_references()
        total_tables = len(set().union(*table_refs.values()))
        print(f"✅ Found {total_tables} unique table references")
        
        # Step 2: Analyze statements for detailed lineage
        print("\n📊 Step 2: Analyzing statements for column lineage...")
        self._analyze_statements()
        
        # Step 3: Refine categorization
        print("\n🎯 Step 3: Refining table categorization...")
        self._refine_table_categorization()
        
        print(f"✅ Categorized {len(self.source_tables)} source tables")
        print(f"✅ Categorized {len(self.target_tables)} target tables")
        print(f"✅ Categorized {len(self.intermediate_tables)} intermediate tables")
        print(f"✅ Found {len(self.column_mappings)} column mappings")
        
        return self.generate_report()
    
    def generate_report(self):
        """Generate comprehensive lineage report"""
        print("\n📋 " + "=" * 100)
        print("   SOURCE TABLES DISCOVERED")
        print("=" * 102)
        
        if self.source_tables:
            print(f"{'Table Name':<40} {'Columns Found':<30} {'Usage Count'}")
            print("-" * 102)
            
            for table, info in sorted(self.source_tables.items()):
                columns_str = ", ".join(sorted(list(info['columns']))[:8])
                if len(info['columns']) > 8:
                    columns_str += f" (+{len(info['columns'])-8} more)"
                if not columns_str:
                    columns_str = "(columns not detected)"
                
                print(f"{table:<40} {columns_str:<30} {info['usage_count']}")
        else:
            print("No source tables detected")
        
        print("\n🎯 " + "=" * 100)
        print("   TARGET TABLES DISCOVERED")
        print("=" * 102)
        
        if self.target_tables:
            print(f"{'Table Name':<40} {'Columns Found':<30} {'Usage Count'}")
            print("-" * 102)
            
            for table, info in sorted(self.target_tables.items()):
                columns_str = ", ".join(sorted(list(info['columns']))[:8])
                if len(info['columns']) > 8:
                    columns_str += f" (+{len(info['columns'])-8} more)"
                if not columns_str:
                    columns_str = "(columns not detected)"
                
                print(f"{table:<40} {columns_str:<30} {info['usage_count']}")
        else:
            print("No target tables detected")
        
        print("\n🔄 " + "=" * 100)
        print("   INTERMEDIATE/PROCESSING TABLES")
        print("=" * 102)
        
        if self.intermediate_tables:
            print(f"{'Table Name':<40} {'Columns Found':<30} {'Usage Count'}")
            print("-" * 102)
            
            for table, info in sorted(self.intermediate_tables.items()):
                columns_str = ", ".join(sorted(list(info['columns']))[:8])
                if len(info['columns']) > 8:
                    columns_str += f" (+{len(info['columns'])-8} more)"
                if not columns_str:
                    columns_str = "(columns not detected)"
                
                print(f"{table:<40} {columns_str:<30} {info['usage_count']}")
        else:
            print("No intermediate tables detected")
        
        print("\n📊 " + "=" * 100)
        print("   COLUMN LINEAGE MAPPINGS")
        print("=" * 102)
        
        if self.column_mappings:
            print(f"{'Source Column':<50} {'Target Column':<50} {'Steps'}")
            print("-" * 102)
            
            # Group mappings by transformation complexity
            simple_mappings = [m for m in self.column_mappings if m['transformation_steps'] <= 1]
            complex_mappings = [m for m in self.column_mappings if m['transformation_steps'] > 1]
            
            print(f"\n📈 DIRECT MAPPINGS ({len(simple_mappings)}):")
            for mapping in sorted(simple_mappings, key=lambda x: x['source_column'])[:20]:
                source = mapping['source_column']
                target = mapping['target_column']
                steps = mapping['transformation_steps']
                
                if len(source) > 48:
                    source = source[:45] + "..."
                if len(target) > 48:
                    target = target[:45] + "..."
                
                print(f"{source:<50} {target:<50} {steps}")
            
            if len(simple_mappings) > 20:
                print(f"... and {len(simple_mappings) - 20} more direct mappings")
            
            if complex_mappings:
                print(f"\n🔄 COMPLEX TRANSFORMATIONS ({len(complex_mappings)}):")
                for mapping in sorted(complex_mappings, key=lambda x: x['transformation_steps'], reverse=True)[:10]:
                    source = mapping['source_column']
                    target = mapping['target_column']
                    steps = mapping['transformation_steps']
                    
                    if len(source) > 48:
                        source = source[:45] + "..."
                    if len(target) > 48:
                        target = target[:45] + "..."
                    
                    print(f"{source:<50} {target:<50} {steps}")
                
                if len(complex_mappings) > 10:
                    print(f"... and {len(complex_mappings) - 10} more complex transformations")
        else:
            print("No column mappings detected")
        
        print("\n🔗 " + "=" * 100)
        print("   TABLE RELATIONSHIPS")
        print("=" * 102)
        
        if self.table_relationships:
            print(f"{'Source Table':<50} {'Target Table(s)'}")
            print("-" * 102)
            
            for source, targets in sorted(self.table_relationships.items())[:20]:
                targets_str = ", ".join(sorted(list(targets))[:3])
                if len(targets) > 3:
                    targets_str += f" (+{len(targets)-3} more)"
                
                if len(source) > 48:
                    source = source[:45] + "..."
                
                print(f"{source:<50} {targets_str}")
            
            if len(self.table_relationships) > 20:
                print(f"... and {len(self.table_relationships) - 20} more relationships")
        else:
            print("No table relationships detected")
        
        print("\n📈 " + "=" * 100)
        print("   ANALYSIS SUMMARY")
        print("=" * 102)
        
        total_tables = len(self.source_tables) + len(self.target_tables) + len(self.intermediate_tables)
        
        print(f"📊 TABLES DISCOVERED:")
        print(f"   • Total tables: {total_tables}")
        print(f"   • Source tables: {len(self.source_tables)}")
        print(f"   • Target tables: {len(self.target_tables)}")
        print(f"   • Intermediate/processing tables: {len(self.intermediate_tables)}")
        
        print(f"\n🔗 LINEAGE MAPPINGS:")
        print(f"   • Column mappings: {len(self.column_mappings)}")
        print(f"   • Table relationships: {len(self.table_relationships)}")
        print(f"   • Processing stages: {len(self.processing_stages)}")
        
        # Calculate complexity score
        complexity_score = 0
        if self.column_mappings:
            avg_transformation_steps = sum(m['transformation_steps'] for m in self.column_mappings) / len(self.column_mappings)
            complexity_score = min(100, avg_transformation_steps * 20)
        
        print(f"\n🎯 COMPLEXITY ANALYSIS:")
        print(f"   • Transformation complexity: {complexity_score:.1f}%")
        
        if complexity_score < 30:
            print("   • Assessment: Simple data flow with mostly direct mappings")
        elif complexity_score < 60:
            print("   • Assessment: Moderate complexity with some multi-step transformations")
        else:
            print("   • Assessment: Complex data processing with extensive transformations")
        
        print("\n" + "=" * 102)
        print("✅ GENERIC SQL LINEAGE ANALYSIS COMPLETE!")
        print("🎯 Ready for data governance, impact analysis, and documentation")
        print("=" * 102)
        
        return {
            'source_tables': dict(self.source_tables),
            'target_tables': dict(self.target_tables),
            'intermediate_tables': dict(self.intermediate_tables),
            'column_mappings': self.column_mappings,
            'table_relationships': dict(self.table_relationships),
            'processing_stages': self.processing_stages,
            'complexity_score': complexity_score
        }
    
    def export_results(self, output_format='json', output_file=None):
        """Export analysis results in various formats"""
        results = {
            'sql_file': self.sql_file_path,
            'analysis_timestamp': self._get_timestamp(),
            'source_tables': {k: {'columns': list(v['columns']), 'usage_count': v['usage_count']} 
                            for k, v in self.source_tables.items()},
            'target_tables': {k: {'columns': list(v['columns']), 'usage_count': v['usage_count']} 
                            for k, v in self.target_tables.items()},
            'intermediate_tables': {k: {'columns': list(v['columns']), 'usage_count': v['usage_count']} 
                                  for k, v in self.intermediate_tables.items()},
            'column_mappings': self.column_mappings,
            'table_relationships': {k: list(v) for k, v in self.table_relationships.items()},
            'processing_stages': self.processing_stages
        }
        
        if output_file is None:
            output_file = f"{Path(self.sql_file_path).stem}_lineage.{output_format}"
        
        if output_format.lower() == 'json':
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"📄 Results exported to {output_file}")
        
        return results
    
    def _get_timestamp(self):
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()


def main():
    """Command line interface for the generic lineage parser"""
    parser = argparse.ArgumentParser(description='Generic SQL Lineage Parser')
    parser.add_argument('sql_file', help='Path to SQL file to analyze')
    parser.add_argument('--export', '-e', choices=['json'], default=None,
                       help='Export results to file format')
    parser.add_argument('--output', '-o', help='Output file path')
    
    args = parser.parse_args()
    
    try:
        # Create parser instance and analyze
        lineage_parser = GenericSQLLineageParser(args.sql_file)
        results = lineage_parser.analyze()
        
        # Export if requested
        if args.export:
            lineage_parser.export_results(args.export, args.output)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    # If called directly without command line args, use the test file
    import sys
    if len(sys.argv) == 1:
        lineage_parser = GenericSQLLineageParser("test.sql")
        lineage_parser.analyze()
    else:
        exit(main())
