using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlClient
{

    public interface IMyNoSqlConnection
    {
        
        string Url { get; }
        
        void Subscribe<T>(string tableName, 
            Action<IReadOnlyList<T>> initAction, 
            Action<string, IReadOnlyList<T>> initPartitionAction, 
            Action<IReadOnlyList<T>> updateAction, 
            Action<IDictionary<string, string>> deleteActions);


        Task<T> RequestAsync<T>(string methodName, params object[] @params);
    }

}