using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.SqlServer.Extensions.Truncate
{
    public static class DbContextExtensions
    {
        public static Task TruncateAsync<T>(this DbContext context, CancellationToken cancellationToken)
        {
            return TruncateAsync(context, typeof(T), cancellationToken);
        }

        public static async Task TruncateAsync(this DbContext context, Type type, CancellationToken cancellationToken)
        {
            var entityType = context.Model.FindEntityType(type);

            if (entityType is null)
                return;

            string? schema = entityType.GetSchema();
            string? tableName = entityType.GetTableName();

            string fullTableName = string.Empty;

            if (!string.IsNullOrEmpty(schema))
                fullTableName = $"{schema}.";

            fullTableName += tableName;

            await context.Database.ExecuteSqlRawAsync($"""
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}' and TABLE_SCHEMA = '{schema}')
                    TRUNCATE TABLE {fullTableName}
                """, cancellationToken);
        }
    }
}
