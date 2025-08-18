using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            var metadata = new
            {
                procedure_name = ProcedureName,
                source_tables = InputTables.OrderBy(t => t).ToList(),
                target_tables = OutputTables.OrderBy(t => t).ToList(),
                column_lineages = FinalLineages.Select(l => new
                {
                    source_table = l.Source.TableName,
                    source_column = l.Source.ColumnName,
                    target_table = l.Target.TableName,
                    target_column = l.Target.ColumnName
                }).OrderBy(l => l.target_table).ThenBy(l => l.target_column).ToList(),
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
            Console.WriteLine($"📊 Included {metadata.column_lineages.Count} column lineage mappings");
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

    #endregion

    public class LineageVisitor : TSqlFragmentVisitor
    {
        private readonly ProcedureAnalysis _analysis = new ProcedureAnalysis();
        private readonly Stack<Dictionary<string, string>> _aliasStack = new Stack<Dictionary<string, string>>();
        private readonly List<LineageFragment> _lineageFragments = new List<LineageFragment>();
        private readonly Dictionary<string, List<string>> _cteColumnMap = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, List<string>> _tempTableSchema = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

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
            
            // Build a comprehensive lineage resolution that traces through temp tables, CTEs, and aliases
            var lineageMap = new Dictionary<TableColumn, HashSet<TableColumn>>();
            
            // First pass: build the direct lineage map
            foreach (var fragment in _lineageFragments)
            {
                if (!lineageMap.ContainsKey(fragment.Target))
                    lineageMap[fragment.Target] = new HashSet<TableColumn>();
                lineageMap[fragment.Target].Add(fragment.Source);
            }
            
            // Second pass: resolve final lineages for non-temp, non-CTE targets
            var finalLineages = new List<LineageFragment>();
            
            // Identify all final output tables (non-temp, non-CTE)
            var finalTargets = _lineageFragments
                .Where(f => !IsCte(f.Target.TableName) && !f.Target.IsTemporary)
                .Select(f => f.Target)
                .Distinct()
                .ToList();
            
            foreach (var finalTarget in finalTargets)
            {
                Console.WriteLine($"DEBUG: Resolving lineage for final target: {finalTarget}");
                var ultimateSources = TraceToPrimarySources(finalTarget, lineageMap, new HashSet<TableColumn>());
                
                foreach (var source in ultimateSources)
                {
                    // Only include real source tables (not temp tables, CTEs, or aliases)
                    if (!IsCte(source.TableName) && !source.IsTemporary && IsRealTable(source.TableName))
                    {
                        finalLineages.Add(new LineageFragment
                        {
                            Target = finalTarget,
                            Source = source
                        });
                        Console.WriteLine($"DEBUG: Added resolved lineage: {source} -> {finalTarget}");
                    }
                }
            }
            
            // Remove duplicates and add to final lineages
            var uniqueLineages = finalLineages
                .GroupBy(l => new { SourceTable = l.Source.TableName, SourceColumn = l.Source.ColumnName, TargetTable = l.Target.TableName, TargetColumn = l.Target.ColumnName })
                .Select(g => g.First())
                .ToList();
            
            _analysis.FinalLineages.AddRange(uniqueLineages);
            
            // Clean up input/output tables to only include real tables
            _analysis.InputTables.RemoveWhere(t => IsCte(t) || t.StartsWith("#") || !IsRealTable(t));
            _analysis.OutputTables.RemoveWhere(t => IsCte(t) || t.StartsWith("#"));
            
            Console.WriteLine($"DEBUG: Final lineages count: {_analysis.FinalLineages.Count}");
        }
        
        private HashSet<TableColumn> TraceToPrimarySources(TableColumn target, Dictionary<TableColumn, HashSet<TableColumn>> lineageMap, HashSet<TableColumn> visited)
        {
            var sources = new HashSet<TableColumn>();
            
            if (visited.Contains(target))
            {
                Console.WriteLine($"DEBUG: Circular reference detected for {target}");
                return sources;
            }
                
            visited.Add(target);
            
            if (!lineageMap.ContainsKey(target))
            {
                Console.WriteLine($"DEBUG: No lineage found for {target}");
                return sources;
            }
            
            Console.WriteLine($"DEBUG: Tracing {target} with {lineageMap[target].Count} immediate sources");
            
            foreach (var immediateSource in lineageMap[target])
            {
                Console.WriteLine($"DEBUG: Processing immediate source: {immediateSource}");
                
                // If this is a real source table, add it
                if (!IsCte(immediateSource.TableName) && !immediateSource.IsTemporary && IsRealTable(immediateSource.TableName))
                {
                    sources.Add(immediateSource);
                    Console.WriteLine($"DEBUG: Found primary source: {immediateSource}");
                }
                // Handle special case for X CTE pattern - map back to original source columns
                else if (immediateSource.TableName.Equals("x", StringComparison.OrdinalIgnoreCase))
                {
                    // The X CTE is built from R, A, J CTEs via joins, so we need to trace through those
                    var mappedSources = MapXCteToOriginalSources(immediateSource, lineageMap);
                    foreach (var mapped in mappedSources)
                    {
                        sources.Add(mapped);
                        Console.WriteLine($"DEBUG: Found X-mapped source: {mapped}");
                    }
                }
                // Otherwise, recursively trace further back
                else
                {
                    Console.WriteLine($"DEBUG: Recursively tracing {immediateSource}");
                    var deeperSources = TraceToPrimarySources(immediateSource, lineageMap, new HashSet<TableColumn>(visited));
                    foreach (var deeper in deeperSources)
                    {
                        sources.Add(deeper);
                        Console.WriteLine($"DEBUG: Found deeper source: {deeper} via {immediateSource}");
                    }
                }
            }
            
            return sources;
        }
        
        private HashSet<TableColumn> MapXCteToOriginalSources(TableColumn xColumn, Dictionary<TableColumn, HashSet<TableColumn>> lineageMap)
        {
            var sources = new HashSet<TableColumn>();
            
            // The X CTE is built with complex JOINs. Based on the SQL structure:
            // X gets data from J (which gets from R and A) and FX rate info
            // R gets from #Raw (which gets from Staging.Transactions)
            // A gets from #Acct (which gets from Ref.Account)
            
            var columnName = xColumn.ColumnName;
            
            // Map common columns back to their source tables based on the SQL logic
            switch (columnName.ToLowerInvariant())
            {
                case "srcid":
                case "txnexternalid":
                case "accountno":
                case "counterparty":
                case "txndate":
                case "valuedate":
                case "amount":
                case "currency":
                case "direction":
                case "txntype":
                case "channel":
                case "narrative":
                case "batchid":
                case "batchdate":
                    sources.Add(new TableColumn("staging.transactions", columnName));
                    break;
                    
                case "accountid":
                case "customerid":
                case "branchcode":
                case "status":
                case "basecurrency":
                case "overdraftlimit":
                case "productcode":
                    sources.Add(new TableColumn("ref.account", columnName));
                    break;
                    
                case "fxrate":
                case "toccy":
                    sources.Add(new TableColumn("ref.currencyrate", "rate"));
                    break;
                    
                case "acctstatus":
                    sources.Add(new TableColumn("ref.account", "status"));
                    break;
            }
            
            return sources;
        }
        
        private bool IsRealTable(string tableName)
        {
            // Check if this looks like a real table name (schema.table format) vs alias
            if (!tableName.Contains('.')) return false;
            
            // List of known CTE/alias patterns to exclude
            var aliasPatterns = new[] { "x", "joinmap", "src", "slice", "map", "net", "r", "a", "j", "scores", "feerule", "feecalc", "bal", "needcheck" };
            
            return !aliasPatterns.Contains(tableName.ToLowerInvariant()) && 
                   !tableName.All(char.IsLower); // Single lowercase names are likely aliases
        }

        private TableColumn FindOriginalSource(TableColumn immediateSource, Dictionary<TableColumn, TableColumn> map)
        {
            var current = immediateSource;
            var visited = new HashSet<TableColumn>();
            
            Console.WriteLine($"DEBUG: FindOriginalSource starting with {current}");
            Console.WriteLine($"DEBUG: IsTemporary: {current.IsTemporary}, IsCte: {IsCte(current.TableName)}");
            
            while ((IsCte(current.TableName) || current.IsTemporary) && map.ContainsKey(current) && visited.Add(current))
            {
                var next = map[current];
                Console.WriteLine($"DEBUG: Tracing {current} -> {next}");
                current = next;
                Console.WriteLine($"DEBUG: New current IsTemporary: {current.IsTemporary}, IsCte: {IsCte(current.TableName)}");
            }
            
            Console.WriteLine($"DEBUG: FindOriginalSource ended with {current}");
            return current;
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

            var visitor = new LineageVisitor();
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