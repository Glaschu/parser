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
                var cteColumns = cte.Columns.Select(c => c.Value.ToLowerInvariant()).ToList();
                _cteColumnMap[cteName] = cteColumns;

                if (cte.QueryExpression is QuerySpecification querySpec)
                {
                    ProcessSelect(querySpec, cteName, cteColumns, manageStack: false);
                }
            }
            base.Visit(node);
            _aliasStack.Pop();
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
            if (_tempTableSchema.TryGetValue(targetTableName, out var cols)) return cols;
            if (_cteColumnMap.TryGetValue(targetTableName, out var cteCols)) return cteCols;
            return new List<string>();
        }

        public void ResolveAndMergeLineage()
        {
            Console.WriteLine($"DEBUG: Starting ResolveAndMergeLineage with {_lineageFragments.Count} fragments");
            
            // Instead of complex merging, just record direct lineages that we can identify clearly
            // This reduces complexity and avoids over-merging that might create incorrect mappings
            
            // Group fragments by target to handle multiple sources per target
            var directLineages = new List<LineageFragment>();
            
            foreach (var fragment in _lineageFragments)
            {
                Console.WriteLine($"DEBUG: Processing fragment: {fragment.Source} -> {fragment.Target}");
                
                // Only include lineages where we have clear direct mappings
                // Avoid complex intermediate resolution for now to prevent incorrect mappings
                if (!IsCte(fragment.Target.TableName) && !fragment.Target.IsTemporary)
                {
                    // For direct source-to-target mappings (non-temp to final tables)
                    if (!IsCte(fragment.Source.TableName) && !fragment.Source.IsTemporary)
                    {
                        directLineages.Add(new LineageFragment
                        {
                            Target = fragment.Target,
                            Source = fragment.Source
                        });
                        Console.WriteLine($"DEBUG: Added direct lineage: {fragment.Source} -> {fragment.Target}");
                    }
                    // For temp table to final table mappings (one-step resolution)
                    else if (fragment.Source.IsTemporary)
                    {
                        // Find direct sources to this temp table
                        var tempSources = _lineageFragments.Where(f => 
                            f.Target.Equals(fragment.Source) && 
                            !f.Source.IsTemporary && 
                            !IsCte(f.Source.TableName)).ToList();
                        
                        foreach (var tempSource in tempSources)
                        {
                            directLineages.Add(new LineageFragment
                            {
                                Target = fragment.Target,
                                Source = tempSource.Source
                            });
                            Console.WriteLine($"DEBUG: Added temp-resolved lineage: {tempSource.Source} -> {fragment.Target}");
                        }
                    }
                }
            }
            
            // Remove duplicates and add to final lineages
            var uniqueLineages = directLineages
                .GroupBy(l => new { SourceTable = l.Source.TableName, SourceColumn = l.Source.ColumnName, TargetTable = l.Target.TableName, TargetColumn = l.Target.ColumnName })
                .Select(g => g.First())
                .ToList();
            
            _analysis.FinalLineages.AddRange(uniqueLineages);
            
            Console.WriteLine($"DEBUG: Final lineages count: {_analysis.FinalLineages.Count}");
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