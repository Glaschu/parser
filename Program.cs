// To run this code, you need to add the following NuGet package to your project:
// Microsoft.SqlServer.TransactSql.ScriptDom

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlParser
{
    #region Data Models

    /// <summary>
    /// Represents a column with its table context, used for lineage mapping.
    /// Using a class for reference equality and easier dictionary keys.
    /// </summary>
    public class TableColumn : IEquatable<TableColumn>
    {
        public string TableName { get; }
        public string ColumnName { get; }

        public TableColumn(string tableName, string columnName)
        {
            TableName = tableName?.ToLowerInvariant() ?? string.Empty;
            ColumnName = columnName?.ToLowerInvariant() ?? string.Empty;
        }

        public bool IsTemporary => TableName.StartsWith("#") || TableName.StartsWith("@");

        public bool Equals(TableColumn other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TableName == other.TableName && ColumnName == other.ColumnName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((TableColumn)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(TableName, ColumnName);
        }

        public override string ToString() => $"[{TableName}].[{ColumnName}]";
    }

    /// <summary>
    /// Represents an in-memory model of a temporary table's schema.
    /// </summary>
    public class InMemoryTable
    {
        public string TableName { get; set; }
        public List<string> Columns { get; } = new List<string>();
    }

    /// <summary>
    /// Represents a single step in the data lineage graph.
    /// </summary>
    public class LineageFragment
    {
        public TableColumn Target { get; set; }
        public TableColumn Source { get; set; }
        public string Expression { get; set; }
    }

    /// <summary>
    /// Represents the final, end-to-end parsed information from a stored procedure.
    /// </summary>
    public class ProcedureAnalysis
    {
        public string ProcedureName { get; set; }
        public HashSet<string> InputTables { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> OutputTables { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public List<LineageFragment> FinalLineages { get; } = new List<LineageFragment>();
        public List<string> Warnings { get; } = new List<string>();

        public void PrintResults()
        {
            Console.WriteLine($"\n--- SQL Analysis for Procedure: {ProcedureName} ---");
            if (Warnings.Any())
            {
                Console.WriteLine("\n[Warnings]");
                Warnings.ForEach(w => Console.WriteLine($"  - {w}"));
            }
            Console.WriteLine("\n[Input Tables]");
            InputTables.OrderBy(t => t).ToList().ForEach(Console.WriteLine);
            Console.WriteLine("\n[Output Tables]");
            OutputTables.OrderBy(t => t).ToList().ForEach(Console.WriteLine);
            Console.WriteLine("\n[Column Lineage (End-to-End)]");
            foreach (var lineage in FinalLineages.OrderBy(l => l.Target.TableName).ThenBy(l => l.Target.ColumnName))
            {
                Console.WriteLine($"{lineage.Target} <-- {lineage.Source} (Expression: {lineage.Expression})");
            }
            Console.WriteLine($"--- End of Analysis for {ProcedureName} ---");
        }
    }

    #endregion

    /// <summary>
    /// The main stateful visitor class that traverses the SQL Abstract Syntax Tree (AST).
    /// </summary>
    public class ProcedureVisitor : TSqlFragmentVisitor
    {
        private ProcedureAnalysis _analysis;
        private readonly Stack<Dictionary<string, string>> _aliasScope = new Stack<Dictionary<string, string>>();
        private readonly Stack<Dictionary<string, List<string>>> _cteScope = new Stack<Dictionary<string, List<string>>>();
        private readonly Stack<Dictionary<string, StringLiteral>> _variableScope = new Stack<Dictionary<string, StringLiteral>>();

        // Stateful tracking for temporary objects
        private readonly Dictionary<string, InMemoryTable> _tempTables = new Dictionary<string, InMemoryTable>(StringComparer.OrdinalIgnoreCase);
        private readonly List<LineageFragment> _lineageFragments = new List<LineageFragment>();

        public ProcedureAnalysis CurrentAnalysis => _analysis;
        
        #region Entry Point and State Management

        public override void Visit(CreateProcedureStatement node)
        {
            _analysis = new ProcedureAnalysis { ProcedureName = GetTableName(node.ProcedureReference.Name) };
            _variableScope.Push(new Dictionary<string, StringLiteral>(StringComparer.OrdinalIgnoreCase));
            
            base.Visit(node); // This will traverse the entire procedure body

            ResolveAndMergeLineage(); // Perform final lineage resolution after traversal
            
            _variableScope.Pop();
        }

        #endregion

        #region Temp Table Schema Tracking

        public override void Visit(CreateTableStatement node)
        {
            var tableName = GetTableName(node.SchemaObjectName);
            if (IsTempTable(tableName))
            {
                var tableModel = new InMemoryTable { TableName = tableName };
                foreach (var colDef in node.Definition.ColumnDefinitions)
                {
                    tableModel.Columns.Add(colDef.ColumnIdentifier.Value);
                }
                _tempTables[tableName] = tableModel;
            }
            base.Visit(node);
        }

        public override void Visit(SelectStatement node)
        {
            // Handle SELECT ... INTO #TempTable
            if (node.Into != null && IsTempTable(GetTableName(node.Into)))
            {
                var tableName = GetTableName(node.Into);
                var tableModel = new InMemoryTable { TableName = tableName };
                if (node.QueryExpression is QuerySpecification querySpec)
                {
                    int unnamedColCount = 0;
                    foreach (var selectElement in querySpec.SelectElements)
                    {
                        if (selectElement is SelectScalarExpression scalarExpr)
                        {
                            var colName = scalarExpr.ColumnName?.Value ?? $"_unnamed_{++unnamedColCount}";
                            tableModel.Columns.Add(colName);
                        }
                        // Note: SELECT * INTO is complex as it requires schema lookup of the source.
                        // This implementation focuses on explicitly defined columns.
                    }
                }
                _tempTables[tableName] = tableModel;
            }
            
            ProcessSelectStatement(node, node.Into != null ? GetTableName(node.Into) : null);
        }

        #endregion

        #region Data Modification and Lineage Fragment Creation

        public override void Visit(InsertStatement node)
        {
            if (_analysis == null) return;
            var targetTable = GetTableName(node.InsertSpecification.Target as SchemaObjectName);
            if (targetTable == null) return;
            
            if (!IsTempTable(targetTable)) _analysis.OutputTables.Add(targetTable);

            if (node.InsertSpecification.InsertSource is SelectInsertSource selectSource)
            {
                ProcessSelectStatement(selectSource.Select, targetTable, node.InsertSpecification.Columns);
            }
        }

        public override void Visit(UpdateStatement node)
        {
            // This logic remains largely the same, as updates typically target permanent tables.
            // If updating a temp table, fragments would be created, but that's an edge case.
            if (_analysis == null) return;
            var targetTable = GetTableName(node.UpdateSpecification.Target as SchemaObjectName);
            if (targetTable == null) return;
            
            if (!IsTempTable(targetTable)) _analysis.OutputTables.Add(targetTable);

            _aliasScope.Push(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            if (node.UpdateSpecification.FromClause != null) ProcessFromClause(node.UpdateSpecification.FromClause);
            else _analysis.InputTables.Add(targetTable);

            foreach (var setClause in node.UpdateSpecification.SetClauses.OfType<AssignmentSetClause>())
            {
                var targetColumn = setClause.Column.MultiPartIdentifier.Identifiers.Last().Value;
                ProcessColumnExpression(new TableColumn(targetTable, targetColumn), setClause.NewValue);
            }
            _aliasScope.Pop();
        }

        public override void Visit(MergeStatement node)
        {
            if (_analysis == null) return;
            var targetTable = GetTableName(node.MergeSpecification.Target as SchemaObjectName);
            if (targetTable == null) return;

            if (!IsTempTable(targetTable)) _analysis.OutputTables.Add(targetTable);
            ProcessTableReference(node.MergeSpecification.TableReference);

            foreach (var action in node.MergeSpecification.ActionClauses)
            {
                if (action.Action is UpdateMergeAction updateAction)
                {
                    foreach (var setClause in updateAction.SetClauses.OfType<AssignmentSetClause>())
                    {
                        var targetColumn = setClause.Column.MultiPartIdentifier.Identifiers.Last().Value;
                        ProcessColumnExpression(new TableColumn(targetTable, targetColumn), setClause.NewValue);
                    }
                }
                else if (action.Action is InsertMergeAction insertAction)
                {
                    for (int i = 0; i < insertAction.Source.RowValues.Count; i++)
                    {
                        var valueExpr = insertAction.Source.RowValues[i];
                        var targetColumnName = (insertAction.Columns.Count > i) ? insertAction.Columns[i].Value : _tempTables[targetTable].Columns[i];
                        ProcessColumnExpression(new TableColumn(targetTable, targetColumnName), valueExpr);
                    }
                }
            }
        }

        #endregion

        #region Core Processing Logic

        private void ProcessSelectStatement(SelectStatement selectStatement, string targetTable, IList<ColumnReferenceExpression> targetColumns = null)
        {
            _aliasScope.Push(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));

            if (selectStatement.QueryExpression is QuerySpecification querySpec)
            {
                if (querySpec.FromClause != null) ProcessFromClause(querySpec.FromClause);

                if (targetTable != null)
                {
                    for (int i = 0; i < querySpec.SelectElements.Count; i++)
                    {
                        var selectElement = querySpec.SelectElements[i];
                        if (selectElement is SelectScalarExpression scalarExpr)
                        {
                            string targetColumnName;
                            if (targetColumns != null && targetColumns.Count > i)
                            {
                                targetColumnName = targetColumns[i].MultiPartIdentifier.Identifiers.Last().Value;
                            }
                            else if (_tempTables.ContainsKey(targetTable) && _tempTables[targetTable].Columns.Count > i)
                            {
                                targetColumnName = _tempTables[targetTable].Columns[i];
                            }
                            else
                            {
                                targetColumnName = scalarExpr.ColumnName?.Value ?? $"_unnamed_{i+1}";
                            }
                            
                            ProcessColumnExpression(new TableColumn(targetTable, targetColumnName), scalarExpr.Expression);
                        }
                    }
                }
            }
            
            _aliasScope.Pop();
        }

        private void ProcessColumnExpression(TableColumn target, TSqlFragment expression)
        {
            var columnVisitor = new ColumnReferenceVisitor(_aliasScope.Count > 0 ? _aliasScope.Peek() : new Dictionary<string, string>());
            expression.Accept(columnVisitor);

            foreach (var (sourceTable, sourceColumn) in columnVisitor.ReferencedColumns)
            {
                var source = new TableColumn(sourceTable, sourceColumn);
                _lineageFragments.Add(new LineageFragment
                {
                    Target = target,
                    Source = source,
                    Expression = GetFragmentText(expression)
                });
                
                // Add to inputs if it's a permanent table
                if (!IsTempTable(sourceTable) && !IsCte(sourceTable))
                {
                    _analysis.InputTables.Add(sourceTable);
                }
            }
        }

        private void ProcessFromClause(FromClause fromClause)
        {
            foreach (var tableReference in fromClause.TableReferences)
            {
                ProcessTableReference(tableReference);
            }
        }

        private void ProcessTableReference(TableReference tableReference)
        {
            if (tableReference is NamedTableReference namedTable)
            {
                var tableName = GetTableName(namedTable.SchemaObject);
                var alias = namedTable.Alias?.Value ?? tableName;

                if (!IsTempTable(tableName) && !IsCte(tableName))
                {
                    _analysis.InputTables.Add(tableName);
                }
                
                if (_aliasScope.Count > 0) _aliasScope.Peek()[alias] = tableName;
            }
            else if (tableReference is QualifiedJoin qualifiedJoin)
            {
                ProcessTableReference(qualifiedJoin.FirstTableReference);
                ProcessTableReference(qualifiedJoin.SecondTableReference);
            }
            // Other cases like QueryDerivedTable...
        }

        #endregion

        #region Lineage Resolution
        
        private void ResolveAndMergeLineage()
        {
            var lineageMap = _lineageFragments.ToDictionary(f => f.Target, f => f);

            foreach (var finalFragment in _lineageFragments.Where(f => !f.Target.IsTemporary))
            {
                var finalLineage = new LineageFragment
                {
                    Target = finalFragment.Target,
                    Source = FindOriginalSource(finalFragment.Source, lineageMap),
                    Expression = finalFragment.Expression
                };
                _analysis.FinalLineages.Add(finalLineage);
            }
        }

        private TableColumn FindOriginalSource(TableColumn immediateSource, Dictionary<TableColumn, LineageFragment> map)
        {
            var current = immediateSource;
            var visited = new HashSet<TableColumn>(); // To prevent infinite loops

            while (current.IsTemporary && map.ContainsKey(current) && !visited.Contains(current))
            {
                visited.Add(current);
                current = map[current].Source;
            }
            return current;
        }

        #endregion

        #region Utility Methods
        private bool IsTempTable(string tableName) => tableName != null && (tableName.StartsWith("#") || tableName.StartsWith("@"));
        private bool IsCte(string name) => _cteScope.Count > 0 && _cteScope.Peek().ContainsKey(name);
        private static string GetTableName(SchemaObjectName schemaObjectName) => schemaObjectName == null ? null : string.Join(".", schemaObjectName.Identifiers.Select(id => id.Value));
        private static string GetFragmentText(TSqlFragment fragment) => (fragment == null || fragment.ScriptTokenStream == null) ? string.Empty : string.Join("", fragment.ScriptTokenStream.Skip(fragment.FirstTokenIndex).Take(fragment.LastTokenIndex - fragment.FirstTokenIndex + 1).Select(t => t.Text));
        // Note: Dynamic SQL and CTE logic would be integrated here as in the previous version. They are omitted for brevity but would be included in a full merge.
        #endregion
    }

    #region Supporting Visitor and Main Program

    public class ColumnReferenceVisitor : TSqlFragmentVisitor
    {
        private readonly Dictionary<string, string> _aliases;
        public List<(string table, string column)> ReferencedColumns { get; } = new List<(string, string)>();

        public ColumnReferenceVisitor(Dictionary<string, string> aliases)
        {
            _aliases = aliases ?? new Dictionary<string, string>();
        }

        public override void Visit(ColumnReferenceExpression node)
        {
            string column = node.MultiPartIdentifier.Identifiers.Last().Value;
            string tableAliasOrName = "Unknown"; 

            if (node.MultiPartIdentifier.Identifiers.Count > 1)
            {
                tableAliasOrName = string.Join(".", node.MultiPartIdentifier.Identifiers.Take(node.MultiPartIdentifier.Identifiers.Count - 1).Select(i => i.Value));
            }
            
            if (_aliases.TryGetValue(tableAliasOrName, out var realTableName))
            {
                ReferencedColumns.Add((realTableName, column));
            }
            else
            {
                ReferencedColumns.Add((tableAliasOrName, column));
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            string sqlScript = @"
                CREATE PROCEDURE dbo.usp_ProcessDailySales
                AS
                BEGIN
                    -- Create a temp table to stage data
                    CREATE TABLE #StagedSales (
                        SaleID INT,
                        ProductName NVARCHAR(100),
                        Region NVARCHAR(50),
                        SaleAmount DECIMAL(18, 2)
                    );

                    -- Insert data from two different source tables into the temp table
                    INSERT INTO #StagedSales (SaleID, ProductName, Region, SaleAmount)
                    SELECT s.NA_SaleID, p.ProductName, 'North America', s.Amount
                    FROM dbo.NorthAmericaSales s
                    JOIN dbo.Products p ON s.ProductID = p.ProductID;

                    INSERT INTO #StagedSales (SaleID, ProductName, Region, SaleAmount)
                    SELECT s.EU_SaleID, p.ProductName, 'Europe', s.Amount
                    FROM dbo.EuropeSales s
                    JOIN dbo.Products p ON s.ProductID = p.ProductID;

                    -- Use the staged data to update a final reporting table
                    MERGE dbo.DailySalesSummary AS T
                    USING #StagedSales AS S
                    ON T.ProductName = S.ProductName AND T.Region = S.Region
                    WHEN MATCHED THEN
                        UPDATE SET T.TotalSales = T.TotalSales + S.SaleAmount
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (ProductName, Region, TotalSales)
                        VALUES (S.ProductName, S.Region, S.SaleAmount);
                END
                GO
            ";

            string[] batches = Regex.Split(sqlScript, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase);
            var parser = new TSql150Parser(true);
            var finalResults = new List<ProcedureAnalysis>();

            foreach (var batch in batches)
            {
                if (string.IsNullOrWhiteSpace(batch)) continue;
                var fragment = parser.Parse(new StringReader(batch), out var errors);
                if (errors.Any())
                {
                    Console.WriteLine($"Parse errors in batch: {string.Join(", ", errors.Select(e => e.Message))}");
                    continue;
                }
                var visitor = new ProcedureVisitor();
                fragment.Accept(visitor);
                if (visitor.CurrentAnalysis != null) finalResults.Add(visitor.CurrentAnalysis);
            }

            Console.WriteLine("====== PARSING COMPLETE ======");
            finalResults.ForEach(r => r.PrintResults());
        }
    }

    #endregion
}
