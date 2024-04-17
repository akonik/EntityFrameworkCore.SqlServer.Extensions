using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore;
using System.Data.Common;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;
using EntityFrameworkCore.SqlServer.Extensions.Extensions;

namespace EntityFrameworkCore.SqlServer.Extensions.BulkMerge
{
    public static class DbContextExtensions
    {
        public static Task BulkMergeAsync<T>(
            this DbContext context, 
            IEnumerable<T> entities, 
            CancellationToken cancellationToken) where T : class
        {
            return BulkMergeAsync(context, typeof(T), entities, cancellationToken);
        }

        public static Task BulkMergeAsync(
            this DbContext context, 
            Type type, 
            IEnumerable<object> entities, 
            CancellationToken cancellationToken)
        {
            return BulkMergeInternalAsync(context, type, entities, cancellationToken);
        }

        private static async Task BulkMergeInternalAsync(
            DbContext context, 
            Type type, 
            IEnumerable<object> entities, 
            CancellationToken cancellationToken)
        {
            IEntityType entityType = GetEntityType(context, type);

            string fullTableName = GetFullTableName(entityType);
            string temporaryTableName = string.Format("t_{0}_{1}", fullTableName, DateTime.Now.Ticks);

            DataTable dataTable = GetDatatableFromEntities(context, type, entities, temporaryTableName);

            using (var transaction = context.Database.BeginTransaction())
            {
                try
                {
                    await CreateTemporaryTableAsync(context, fullTableName, temporaryTableName, cancellationToken);

                    await ExecuteBulkCopyAsync(temporaryTableName, dataTable, transaction, context.Database.GetDbConnection(), cancellationToken);

                    await ExecuteBulkMergeAsync(context, entityType, fullTableName, temporaryTableName, cancellationToken);
                }
                catch
                {
                    await transaction.RollbackAsync(cancellationToken);
                    throw;
                }
                finally
                {
                    await DropTableIfExistsAsync(context, temporaryTableName, cancellationToken);
                }

                transaction.Commit();
            }


        }

        private static IEntityType GetEntityType(DbContext context, Type type)
        {
            var entityType = context.Model.FindEntityType(type);

            if (entityType is null)
                throw new Exception($"Can not find the entity {type.Name} type in the Context");
            
            return entityType;
        }

        private static DataTable GetDatatableFromEntities(DbContext context, Type type, IEnumerable<object> entities, string temporaryTableName)
        {
            var dataTable = entities.ToDataTable(context, type, temporaryTableName);

            if (dataTable is null)
                throw new Exception("Can not convert given entities to the DataTable");
            
            return dataTable;
        }

        private static async Task ExecuteBulkMergeAsync(
            DbContext context, 
            IEntityType entityType, 
            string fullTableName, 
            string temporaryTableName, 
            CancellationToken cancellationToken)
        {
            string mergeScript = entityType.GenerateMergeScript(fullTableName, temporaryTableName);
            await context.Database.ExecuteSqlRawAsync(mergeScript, cancellationToken);
        }

        private static async Task ExecuteBulkCopyAsync(
            string temporaryTableName, 
            DataTable dataTable, 
            IDbContextTransaction transaction, 
            DbConnection dbConnection, 
            CancellationToken cancellationToken)
        {
            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(
                (SqlConnection)dbConnection,
                SqlBulkCopyOptions.Default,
                (SqlTransaction)transaction.GetDbTransaction()))
            {
                foreach (DataColumn col in dataTable.Columns)
                {
                    sqlBulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                sqlBulkCopy.DestinationTableName = temporaryTableName;
                await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken);
            }
        }

        private static string GetFullTableName(IEntityType entityType)
        {
            string schema = entityType.GetSchema() ?? string.Empty;
            string tableName = entityType.GetTableName() ?? string.Empty;

            if (string.IsNullOrEmpty(tableName))
                throw new Exception($"Can not get the table name from the EntityType. Type : {entityType.Name}");

            string fullTableName = string.Empty;

            if (!string.IsNullOrEmpty(schema))
                fullTableName = $"{schema}.";

            fullTableName += tableName;
            return fullTableName;
        }

        private static async Task DropTableIfExistsAsync(
            this DbContext context, 
            string tableName, 
            CancellationToken cancellationToken)
        {
            await context.Database.ExecuteSqlRawAsync(
                $"DROP TABLE IF EXISTS {tableName}", 
                cancellationToken);
        }

        private static async Task CreateTemporaryTableAsync(
            this DbContext context, 
            string sourceTable, 
            string targetTable, 
            CancellationToken cancellationToken)
        {
            await context.Database.ExecuteSqlRawAsync(
                $""" 
                SELECT TOP 0 *
                INTO {targetTable}
                FROM {sourceTable};
                """, cancellationToken);
        }

        internal static string GenerateMergeScript(
            this IEntityType entityType, 
            string targetTableName, 
            string sourceTableName)
        {
            var columns = entityType
               .GetProperties()
               .ToArray();

            var e = columns.Select(x => x.GetAnnotations());

            var keys = columns
                .Where(x => x.IsPrimaryKey())
                .ToArray();

            if (!keys.Any())
            {
                keys = columns;
            }

            columns = columns.Where(x => x.GetAnnotation("SqlServer:ValueGenerationStrategy")?.Value?.ToString() != "IdentityColumn").ToArray();

            string command = $"""
                MERGE {targetTableName} as Target
                USING (SELECT DISTINCT * FROM {sourceTableName}) as Source
                ON {BuildONStatement(keys)}
                WHEN NOT MATCHED BY Target THEN
                    {BuildInsertBody(columns)}
                WHEN MATCHED THEN UPDATE SET
                    {BuildUpdateBody(columns)};
                """
           ;

            return command;
        }

        private static string BuildONStatement(IProperty[] keys)
        {
            return string.Join(" and ", keys.Select(s => $"Target.{s.GetColumnName()} = Source.{s.GetColumnName()}"));
        }

        private static string BuildInsertBody(IProperty[] dbColumns)
        {
            List<string> targetColumns = new List<string>();
            List<string> sourceColumns = new List<string>();

            foreach (var columnName in dbColumns.Select(s => s.GetColumnName()))
            {
                targetColumns.Add($"[{columnName}]");
                sourceColumns.Add($"Source.[{columnName}]");

            }

            return $"""
                INSERT ({string.Join(",", targetColumns)})
                VALUES ({string.Join(",", sourceColumns)})
                """;
        }
        private static string BuildUpdateBody(IProperty[] dbColumns)
        {
            List<string> values = new List<string>();

            foreach (var columnName in dbColumns.Select(s => s.GetColumnName()))
            {
                values.Add($"Target.[{columnName}] = Source.[{columnName}]");
            }

            return string.Join(",\r\n", values);
        }
    }
}
