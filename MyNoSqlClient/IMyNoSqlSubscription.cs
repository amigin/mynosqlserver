using System;
using System.Collections.Generic;

namespace MyNoSqlClient
{

    public interface IMyNoSqlSubscriberConnection
    {
        void Subscribe<T>(string tableName, 
            Action<IReadOnlyList<T>> initAction, 
            Action<string, IReadOnlyList<T>> initPartitionAction, 
            Action<IReadOnlyList<T>> updateAction, 
            Action<IDictionary<string, string>> deleteActions);
    }



}