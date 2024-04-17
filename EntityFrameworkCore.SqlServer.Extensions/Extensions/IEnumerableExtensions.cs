using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace EntityFrameworkCore.SqlServer.Extensions.Extensions
{
    internal static class IEnumerableExtensions
    {
        internal static DataTable? ToDataTable(
            this IEnumerable<object> entities,
            DbContext context,
            Type type,
            string tableName)
        {
            var entityType = context.Model.FindEntityType(type);

            if (entityType is null)
                return null;

            DataTable dt = new DataTable(tableName);

            var properties = entityType.GetProperties()
                .Where(x =>
                    x.PropertyInfo is not null
                    && !x.PropertyInfo.CustomAttributes
                        .Any(a => a.AttributeType == typeof(NotMappedAttribute))
                    && !IsCollection(x))
                .ToArray();

            foreach (var prop in properties)
            {
                dt.Columns.Add(new DataColumn
                {
                    ColumnName = prop.GetColumnName(),
                    AllowDBNull = true
                });
            }

            var entityProperties = type.GetProperties()
                .ToDictionary(x => x.Name, x => x);

            var entitiesArray = entities.ToArray();

            for (int l = 0; l < entitiesArray.Length; l++)
            {
                var e = entitiesArray[l];

                var row = dt.NewRow();

                for (int i = 0; i < properties.Length; i++)
                {
                    var prop = properties[i];

                    var propertyValue = entityProperties
                        .GetValueOrDefault(prop.Name)
                        ?.GetValue(e, null);

                    row[prop.Name] = propertyValue;
                }
                dt.Rows.Add(row);
            }

            return dt;

        }

        private static bool IsCollection(IProperty property)
        {
            var type = property.PropertyInfo?.PropertyType;
            return (
                typeof(IEnumerable).IsAssignableFrom(type) 
                && type != typeof(string));
        }
    }
}
