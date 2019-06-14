using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlClient
{

    public interface IMyNoSqlConnection
    {
        void Subscribe<T>(string tableName, 
            Action<IReadOnlyList<T>> initAction, 
            Action<string, IReadOnlyList<T>> initPartitionAction, 
            Action<IReadOnlyList<T>> updateAction, 
            Action<IDictionary<string, string>> deleteActions);

        Task<IReadOnlyList<T>> RequestRowsAsync<T>(string tableName, string partitionKey);
        Task<T> RequestRowAsync<T>(string tableName, string partitionKey, string rowKey);
        
    }

}