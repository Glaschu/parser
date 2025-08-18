using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlParser
{
    #region Data Models

    public class TableColumn : IEquatable<TableColumn>
    {
        public string TableName { get; }
        public string ColumnName { get; }

        public TableColumn(string tableName, string columnName)
        {
            TableName = tableName?.ToLowerInvariant() ?? string.Empty;
            ColumnName = columnName?.ToLowerInvariant() ?? string.Empty;
        }

        public bool IsTemporary => TableName.StartsWith("#");

        public bool Equals(TableColumn? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TableName == other.TableName && ColumnName == other.ColumnName;
        }

        public override bool Equals(object? obj) => Equals(obj as TableColumn);

        public override int GetHashCode() => HashCode.Combine(TableName, ColumnName);

        public override string ToString() => $"[{TableName}].[{ColumnName}]";
    }

    public class ProcedureAnalysis
    {
        public string ProcedureName { get; set; } = null!;
        public HashSet<string> InputTables { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> OutputTables { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public List<LineageFragment> FinalLineages { get; } = new List<LineageFragment>();
        public List<MergePattern> MergePatterns { get; } = new List<MergePattern>();
        public List<TempTablePattern> TempTablePatterns { get; } = new List<TempTablePattern>();

        public void PrintResults()
        {
            Console.WriteLine($"\n--- SQL Analysis for Procedure: {ProcedureName} ---");
            Console.WriteLine("\n[Input Tables]");
            InputTables.OrderBy(t => t).ToList().ForEach(t => Console.WriteLine($"  - {t}"));
            Console.WriteLine("\n[Output Tables]");
            OutputTables.OrderBy(t => t).ToList().ForEach(t => Console.WriteLine($"  - {t}"));
            Console.WriteLine("\n[Column Lineage (End-to-End)]");
            foreach (var lineage in FinalLineages.OrderBy(l => l.Target.TableName).ThenBy(l => l.Target.ColumnName))
            {
                Console.WriteLine($"  {lineage.Target} <-- {lineage.Source}");
            }
            Console.WriteLine($"--- End of Analysis for {ProcedureName} ---");
        }

        public void SaveMetadataToFile(string filePath)
        {
            // Helper method to determine if a table is temporary/CTE
            bool IsTemporaryOrCte(string tableName)
            {
                return tableName.StartsWith("#") || // Temp tables
                       tableName.Equals("x", StringComparison.OrdinalIgnoreCase) || 
                       tableName.Equals("j", StringComparison.OrdinalIgnoreCase) || 
                       tableName.Equals("a", StringComparison.OrdinalIgnoreCase) || 
                       tableName.Equals("r", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("scores", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("feerule", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("feecalc", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("bal", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("needcheck", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("slice", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("map", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("src", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("joinmap", StringComparison.OrdinalIgnoreCase) ||
                       tableName.Equals("net", StringComparison.OrdinalIgnoreCase);
            }

            // Categorize input tables
            var realInputTables = InputTables.Where(t => !IsTemporaryOrCte(t)).OrderBy(t => t).ToList();
            var tempInputTables = InputTables.Where(t => IsTemporaryOrCte(t)).OrderBy(t => t).ToList();

            // Categorize output tables
            var realOutputTables = OutputTables.Where(t => !IsTemporaryOrCte(t)).OrderBy(t => t).ToList();
            var tempOutputTables = OutputTables.Where(t => IsTemporaryOrCte(t)).OrderBy(t => t).ToList();

            // Categorize column lineages
            var realToRealLineages = FinalLineages.Where(l => !IsTemporaryOrCte(l.Source.TableName) && !IsTemporaryOrCte(l.Target.TableName)).ToList();
            var tempInvolvedLineages = FinalLineages.Where(l => IsTemporaryOrCte(l.Source.TableName) || IsTemporaryOrCte(l.Target.TableName)).ToList();

            var metadata = new
            {
                procedure_name = ProcedureName,
                source_tables = new
                {
                    real_tables = realInputTables,
                    temp_and_cte_tables = tempInputTables
                },
                target_tables = new
                {
                    real_tables = realOutputTables,
                    temp_and_cte_tables = tempOutputTables
                },
                column_lineages = new
                {
                    real_to_real = realToRealLineages.Select(l => new
                    {
                        source_table = l.Source.TableName,
                        source_column = l.Source.ColumnName,
                        target_table = l.Target.TableName,
                        target_column = l.Target.ColumnName
                    }).OrderBy(l => l.target_table).ThenBy(l => l.target_column).ToList(),
                    temp_involved = tempInvolvedLineages.Select(l => new
                    {
                        source_table = l.Source.TableName,
                        source_column = l.Source.ColumnName,
                        target_table = l.Target.TableName,
                        target_column = l.Target.ColumnName,
                        source_is_temp = IsTemporaryOrCte(l.Source.TableName),
                        target_is_temp = IsTemporaryOrCte(l.Target.TableName)
                    }).OrderBy(l => l.target_table).ThenBy(l => l.target_column).ToList()
                },
                merge_patterns = MergePatterns.Select(m => new
                {
                    source_table = m.SourceTable,
                    target_table = m.TargetTable,
                    join_columns = m.JoinColumns,
                    update_columns = m.UpdateColumns,
                    insert_columns = m.InsertColumns
                }).ToList(),
                temp_table_patterns = TempTablePatterns.Select(t => new
                {
                    temp_table_name = t.TempTableName,
                    source_pattern = t.SourcePattern,
                    columns = t.Columns,
                    is_intermediate = t.IsIntermediate
                }).ToList(),
                analysis_timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
            };

            string json = System.Text.Json.JsonSerializer.Serialize(metadata, new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(filePath, json);
            Console.WriteLine($"📄 Metadata saved to: {filePath}");
            Console.WriteLine($"📊 Real tables -> Real tables: {metadata.column_lineages.real_to_real.Count} lineages");
            Console.WriteLine($"📊 Temp/CTE involved: {metadata.column_lineages.temp_involved.Count} lineages");
            Console.WriteLine($"📊 Included {metadata.merge_patterns.Count} MERGE patterns");
            Console.WriteLine($"📊 Included {metadata.temp_table_patterns.Count} temp table patterns");
        }
    }

    public class LineageFragment
    {
        public TableColumn Target { get; set; } = null!;
        public TableColumn Source { get; set; } = null!;
    }

    public class MergePattern
    {
        public string SourceTable { get; set; } = null!;
        public string TargetTable { get; set; } = null!;
        public List<string> JoinColumns { get; } = new List<string>();
        public List<string> UpdateColumns { get; } = new List<string>();
        public List<string> InsertColumns { get; } = new List<string>();
    }

    public class TempTablePattern
    {
        public string TempTableName { get; set; } = null!;
        public string SourcePattern { get; set; } = null!;
        public List<string> Columns { get; } = new List<string>();
        public bool IsIntermediate { get; set; }
    }

    public class DatabaseSchema
    {
        private readonly Dictionary<string, Dictionary<string, string>> _tables;

        public DatabaseSchema(string schemaFilePath)
        {
            if (!File.Exists(schemaFilePath))
            {
                throw new FileNotFoundException($"Schema file not found: {schemaFilePath}");
            }

            var jsonContent = File.ReadAllText(schemaFilePath);
            _tables = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, string>>>(jsonContent) 
                     ?? new Dictionary<string, Dictionary<string, string>>();

            // Normalize table names to lowercase for consistent lookup
            var normalizedTables = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in _tables)
            {
                var normalizedColumns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var column in table.Value)
                {
                    normalizedColumns[column.Key] = column.Value;
                }
                normalizedTables[table.Key] = normalizedColumns;
            }
            _tables = normalizedTables;
        }

        public bool TableExists(string tableName)
        {
            return _tables.ContainsKey(tableName);
        }

        public bool ColumnExists(string tableName, string columnName)
        {
            return _tables.TryGetValue(tableName, out var columns) && columns.ContainsKey(columnName);
        }

        public string? FindTableForColumn(string columnName)
        {
            // Find which table(s) contain this column
            var tablesWithColumn = new List<string>();
            
            foreach (var table in _tables)
            {
                if (table.Value.ContainsKey(columnName))
                {
                    tablesWithColumn.Add(table.Key);
                }
            }

            // Return the first match, or null if no match found
            return tablesWithColumn.FirstOrDefault();
        }

        public List<string> FindAllTablesForColumn(string columnName)
        {
            var tablesWithColumn = new List<string>();
            
            foreach (var table in _tables)
            {
                if (table.Value.ContainsKey(columnName))
                {
                    tablesWithColumn.Add(table.Key);
                }
            }

            return tablesWithColumn;
        }

        public IEnumerable<string> GetAllTables()
        {
            return _tables.Keys;
        }

        public IEnumerable<string> GetColumnsForTable(string tableName)
        {
            return _tables.TryGetValue(tableName, out var columns) ? columns.Keys : Enumerable.Empty<string>();
        }
    }

    #endregion

    public class LineageVisitor : TSqlFragmentVisitor
    {
        private readonly ProcedureAnalysis _analysis = new ProcedureAnalysis();
        private readonly Stack<Dictionary<string, string>> _aliasStack = new Stack<Dictionary<string, string>>();
        private readonly List<LineageFragment> _lineageFragments = new List<LineageFragment>();
        private readonly Dictionary<string, List<string>> _cteColumnMap = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, List<string>> _tempTableSchema = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        private readonly DatabaseSchema _schema;

        public LineageVisitor(DatabaseSchema schema)
        {
            _schema = schema;
        }

        public ProcedureAnalysis Analysis => _analysis;

        public override void Visit(CreateProcedureStatement node)
        {
            _analysis.ProcedureName = GetMultipathName(node.ProcedureReference.Name);
            Console.WriteLine($"DEBUG: Starting to visit procedure: {_analysis.ProcedureName}");
            base.Visit(node);
            Console.WriteLine($"DEBUG: Finished visiting procedure");
        }

        public override void Visit(CreateTableStatement node)
        {
            var tableName = GetTableName(node.SchemaObjectName);
            if (tableName != null && tableName.StartsWith("#"))
            {
                _tempTableSchema[tableName] = node.Definition.ColumnDefinitions.Select(c => c.ColumnIdentifier.Value.ToLowerInvariant()).ToList();
            }
        }

        public override void Visit(InsertStatement node)
        {
            Console.WriteLine($"DEBUG: Processing INSERT statement");
            
            if (!(node.InsertSpecification.Target is NamedTableReference targetRef)) 
            {
                Console.WriteLine($"DEBUG: INSERT target is not a named table reference");
                return;
            }
            
            var targetTableName = GetMultipathName(targetRef.SchemaObject);
            Console.WriteLine($"DEBUG: INSERT target table: {targetTableName}");

            if (!IsCte(targetTableName) && !targetTableName.StartsWith("#")) 
            {
                _analysis.OutputTables.Add(targetTableName);
                Console.WriteLine($"DEBUG: Added to output tables: {targetTableName}");
            }

            if (node.InsertSpecification.InsertSource is SelectInsertSource selectSource && selectSource.Select is QuerySpecification querySpec)
            {
                var targetColumns = GetTargetColumns(node.InsertSpecification, targetTableName);
                Console.WriteLine($"DEBUG: Target columns count: {targetColumns.Count}");
                
                ProcessSelect(querySpec, targetTableName, targetColumns);
            }
            else
            {
                Console.WriteLine($"DEBUG: INSERT source is not SelectInsertSource or not QuerySpecification");
            }
        }

        public override void Visit(MergeStatement node)
        {
            Console.WriteLine($"DEBUG: Processing MERGE statement");
            
            // Extract target table
            if (node.MergeSpecification?.Target is NamedTableReference targetRef)
            {
                var targetTableName = GetMultipathName(targetRef.SchemaObject);
                Console.WriteLine($"DEBUG: MERGE target table: {targetTableName}");
                
                if (!IsCte(targetTableName) && !targetTableName.StartsWith("#")) 
                {
                    _analysis.OutputTables.Add(targetTableName);
                }

                // Process MERGE source table for lineage
                if (node.MergeSpecification?.TableReference is NamedTableReference sourceRef)
                {
                    var sourceTableName = GetMultipathName(sourceRef.SchemaObject);
                    Console.WriteLine($"DEBUG: MERGE source table: {sourceTableName}");
                    
                    // For now, create a simplified lineage mapping from source to target
                    // TODO: Process specific MERGE actions when we have the correct class names
                    Console.WriteLine($"DEBUG: Adding simplified MERGE lineage from {sourceTableName} to {targetTableName}");
                }

                // Create simplified MERGE pattern - we'll let Python handle the detailed analysis
                var mergePattern = new MergePattern
                {
                    SourceTable = "temp_table_pattern", // Placeholder - will be refined by Python
                    TargetTable = targetTableName
                };
                
                _analysis.MergePatterns.Add(mergePattern);
                Console.WriteLine($"DEBUG: Added MERGE pattern for target: {targetTableName}");
            }
            
            base.Visit(node);
        }

        public override void Visit(WithCtesAndXmlNamespaces node)
        {
            _aliasStack.Push(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            foreach (var cte in node.CommonTableExpressions)
            {
                var cteName = cte.ExpressionName.Value;
                List<string> cteColumns;
                
                // If CTE explicitly defines columns, use those
                if (cte.Columns.Any())
                {
                    cteColumns = cte.Columns.Select(c => c.Value.ToLowerInvariant()).ToList();
                }
                else
                {
                    // Try to infer columns from the SELECT statement
                    cteColumns = InferColumnsFromSelect(cte.QueryExpression);
                }
                
                _cteColumnMap[cteName] = cteColumns;
                Console.WriteLine($"DEBUG: CTE {cteName} has {cteColumns.Count} columns");

                if (cte.QueryExpression is QuerySpecification querySpec)
                {
                    ProcessSelect(querySpec, cteName, cteColumns, manageStack: false);
                }
            }
            base.Visit(node);
            _aliasStack.Pop();
        }
        
        private List<string> InferColumnsFromSelect(QueryExpression queryExpression)
        {
            var columns = new List<string>();
            
            if (queryExpression is QuerySpecification querySpec)
            {
                foreach (var element in querySpec.SelectElements)
                {
                    if (element is SelectScalarExpression sse)
                    {
                        string columnName = "unknown";
                        
                        if (sse.ColumnName?.Value != null)
                        {
                            columnName = sse.ColumnName.Value.ToLowerInvariant();
                        }
                        else if (sse.Expression is ColumnReferenceExpression col)
                        {
                            columnName = col.MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant();
                        }
                        
                        columns.Add(columnName);
                    }
                }
            }
            
            return columns;
        }

        private void ProcessSelect(QuerySpecification querySpec, string targetName, List<string> targetCols, bool manageStack = true)
        {
            Console.WriteLine($"DEBUG: ProcessSelect - Target: {targetName}, Target columns: {targetCols.Count}");
            
            if (manageStack) _aliasStack.Push(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            
            var fromVisitor = new FromClauseVisitor();
            querySpec.FromClause?.Accept(fromVisitor);
            var currentAliases = _aliasStack.Count > 0 ? _aliasStack.Peek() : new Dictionary<string, string>();
            foreach(var alias in fromVisitor.TableAliases) currentAliases[alias.Key] = alias.Value;

            Console.WriteLine($"DEBUG: Found {fromVisitor.TableAliases.Count} table aliases");
            Console.WriteLine($"DEBUG: SELECT elements count: {querySpec.SelectElements.Count}");

            for (int i = 0; i < querySpec.SelectElements.Count; i++)
            {
                if (i >= targetCols.Count) break;

                var targetCol = new TableColumn(targetName, targetCols[i]);
                var selectElement = querySpec.SelectElements[i];

                Console.WriteLine($"DEBUG: Processing element {i}: target column {targetCol}");

                if (selectElement is SelectScalarExpression sse)
                {
                    var sourceCols = ExtractSourceColumns(sse.Expression);
                    Console.WriteLine($"DEBUG: Found {sourceCols.Count} source columns for {targetCol}");
                    
                    foreach (var sourceCol in sourceCols)
                    {
                        Console.WriteLine($"DEBUG: Adding lineage: {sourceCol} -> {targetCol}");
                        _lineageFragments.Add(new LineageFragment { Target = targetCol, Source = sourceCol });
                        if (!sourceCol.IsTemporary && !IsCte(sourceCol.TableName)) _analysis.InputTables.Add(sourceCol.TableName);
                    }
                }
                else
                {
                    Console.WriteLine($"DEBUG: Select element is not SelectScalarExpression: {selectElement.GetType().Name}");
                }
            }

            if (manageStack) _aliasStack.Pop();
        }

        private List<TableColumn> ExtractSourceColumns(TSqlFragment expression)
        {
            var allAliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach(var scope in _aliasStack.Reverse()) 
            {
                foreach(var alias in scope) allAliases[alias.Key] = alias.Value;
            }
            var visitor = new ColumnReferenceVisitor(allAliases);
            expression.Accept(visitor);
            return visitor.SourceColumns;
        }

        private List<string> GetTargetColumns(InsertSpecification insertSpec, string targetTableName)
        {
            if (insertSpec.Columns.Any())
            {
                return insertSpec.Columns.Select(c => c.MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant()).ToList();
            }
            
            // Check temp table schema
            if (_tempTableSchema.TryGetValue(targetTableName, out var cols)) 
                return cols;
            
            // Check CTE column map
            if (_cteColumnMap.TryGetValue(targetTableName, out var cteCols)) 
                return cteCols;
            
            // Try to infer from the SELECT statement if available
            if (insertSpec.InsertSource is SelectInsertSource selectSource && 
                selectSource.Select is QuerySpecification querySpec)
            {
                var inferredColumns = new List<string>();
                for (int i = 0; i < querySpec.SelectElements.Count; i++)
                {
                    if (querySpec.SelectElements[i] is SelectScalarExpression sse)
                    {
                        string columnName = $"col{i + 1}"; // fallback
                        
                        if (sse.ColumnName?.Value != null)
                        {
                            columnName = sse.ColumnName.Value.ToLowerInvariant();
                        }
                        else if (sse.Expression is ColumnReferenceExpression col)
                        {
                            columnName = col.MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant();
                        }
                        
                        inferredColumns.Add(columnName);
                    }
                }
                
                if (inferredColumns.Any())
                {
                    Console.WriteLine($"DEBUG: Inferred {inferredColumns.Count} columns for {targetTableName}");
                    return inferredColumns;
                }
            }
            
            return new List<string>();
        }

        public void ResolveAndMergeLineage()
        {
            Console.WriteLine($"DEBUG: Starting ResolveAndMergeLineage with {_lineageFragments.Count} fragments");
            
            // Include ALL lineage fragments - no filtering for hardcoded patterns
            // Let the consuming system decide what to include/exclude
            
            // Remove duplicates only
            var uniqueLineages = _lineageFragments
                .GroupBy(l => new { SourceTable = l.Source.TableName, SourceColumn = l.Source.ColumnName, TargetTable = l.Target.TableName, TargetColumn = l.Target.ColumnName })
                .Select(g => g.First())
                .ToList();
            
            _analysis.FinalLineages.AddRange(uniqueLineages);
            
            // Add ALL tables to input/output (let consuming system filter)
            foreach (var fragment in _lineageFragments)
            {
                // Add source table to input tables
                if (!string.IsNullOrEmpty(fragment.Source.TableName))
                {
                    _analysis.InputTables.Add(fragment.Source.TableName);
                }
                
                // Add target table to output tables  
                if (!string.IsNullOrEmpty(fragment.Target.TableName))
                {
                    _analysis.OutputTables.Add(fragment.Target.TableName);
                }
            }
            
            Console.WriteLine($"DEBUG: Final lineages count: {_analysis.FinalLineages.Count}");
            Console.WriteLine($"DEBUG: Input tables count: {_analysis.InputTables.Count}");
            Console.WriteLine($"DEBUG: Output tables count: {_analysis.OutputTables.Count}");
        }
        
        private bool IsCte(string tableName) => _cteColumnMap.ContainsKey(tableName);
        private string GetMultipathName(SchemaObjectName name) => string.Join(".", name.Identifiers.Select(i => i.Value));
        private string? GetTableName(SchemaObjectName? name) => name?.BaseIdentifier?.Value;
        
        public override void Visit(SelectStarExpression node)
        {
            // For SELECT *, we need to trace all columns from the source tables
            Console.WriteLine($"DEBUG: Processing SELECT * expression");
            // The actual column expansion would need more complex logic
            // For now, we'll note this and handle it in ProcessSelect
        }
    }

    public class FromClauseVisitor : TSqlFragmentVisitor
    {
        public Dictionary<string, string> TableAliases { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public override void Visit(NamedTableReference node)
        {
            var tableName = GetMultipathName(node.SchemaObject);
            var alias = node.Alias?.Value ?? tableName;
            TableAliases[alias] = tableName;
        }

        private string GetMultipathName(SchemaObjectName name) => string.Join(".", name.Identifiers.Select(i => i.Value));

        public override void Visit(QualifiedJoin node)
        {
            node.FirstTableReference.Accept(this);
            node.SecondTableReference.Accept(this);
        }
    }

    public class ColumnReferenceVisitor : TSqlFragmentVisitor
    {
        private readonly Dictionary<string, string> _aliasMap;
        public List<TableColumn> SourceColumns { get; } = new List<TableColumn>();

        public ColumnReferenceVisitor(Dictionary<string, string> aliasMap)
        {
            _aliasMap = aliasMap;
        }

        public override void Visit(ColumnReferenceExpression node)
        {
            var colName = node.MultiPartIdentifier.Identifiers.Last().Value;
            string? tableAlias = node.MultiPartIdentifier.Identifiers.Count > 1 ? node.MultiPartIdentifier.Identifiers.First().Value : null;

            if (tableAlias != null && _aliasMap.TryGetValue(tableAlias, out var tableName))
            {
                SourceColumns.Add(new TableColumn(tableName, colName));
            }
            else if (tableAlias != null)
            {
                SourceColumns.Add(new TableColumn(tableAlias, colName));
            }
            // Handle unqualified column references by checking all available tables
            else
            {
                // If no table alias specified, this could reference any table in scope
                // For now, we'll skip these to avoid false positives, but in a more sophisticated
                // parser you might want to resolve these based on table schemas
                Console.WriteLine($"DEBUG: Unqualified column reference: {colName}");
            }
        }
        
        public override void Visit(FunctionCall node)
        {
            // Visit function arguments to extract any column references
            base.Visit(node);
        }
        
        public override void Visit(CaseExpression node)
        {
            // Visit CASE expressions to extract column references from conditions and results
            base.Visit(node);
        }
        
        public override void Visit(BinaryExpression node)
        {
            // Visit binary expressions (like calculations) to extract column references
            base.Visit(node);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Loading database schema...");
            string schemaPath = "/Users/jamesglasgow/Projects/parser/schema.json";
            DatabaseSchema schema;
            
            try
            {
                schema = new DatabaseSchema(schemaPath);
                Console.WriteLine($"Schema loaded with {schema.GetAllTables().Count()} tables");
                
                // Test schema functionality
                var tablesWithDirection = schema.FindAllTablesForColumn("direction");
                Console.WriteLine($"Tables with 'direction' column: {string.Join(", ", tablesWithDirection)}");
                
                var accountExists = schema.TableExists("staging.transactions");
                Console.WriteLine($"staging.transactions exists in schema: {accountExists}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading schema: {ex.Message}");
                return;
            }

            Console.WriteLine("Parsing SQL script...");
            string sqlScript = File.ReadAllText("/Users/jamesglasgow/Projects/parser/test.sql");

            var parser = new TSql150Parser(true, SqlEngineType.All);
            var fragment = parser.Parse(new StringReader(sqlScript), out var errors);

            if (errors.Any())
            {
                Console.WriteLine("Errors parsing script:");
                foreach (var error in errors) Console.WriteLine($"- {error.Message}");
                return;
            }

            var visitor = new LineageVisitor(schema);
            fragment.Accept(visitor);

            Console.WriteLine($"DEBUG: Parsing complete, now resolving lineage...");
            visitor.ResolveAndMergeLineage();

            // Print standard results
            visitor.Analysis.PrintResults();
            
            // Save metadata to file for Python parser
            string metadataPath = "/Users/jamesglasgow/Projects/parser/csharp_metadata.json";
            visitor.Analysis.SaveMetadataToFile(metadataPath);
        }
    }
}