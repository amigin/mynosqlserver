using System.Threading.Tasks;

namespace MyNoSqlClient
{
    public static class MyNoSqlClientUtils
    {
        public static async Task<T> DeleteAsync<T>(
            this IMyNoSqlServerClient<MyNoSqlIndex> indexTableStorage,
            string indexPartitionKey,
            string indexRowKey,
            IMyNoSqlServerClient<T> tableStorage)
            where T : class, IMyNoSqlTableEntity, new()
        {
            var index = await indexTableStorage.DeleteAsync(indexPartitionKey, indexRowKey);
            if (index == null)
                return default (T);
            return await tableStorage.DeleteAsync(index.PrimaryPartitionKey, index.PrimaryRowKey);
        }
    }
}