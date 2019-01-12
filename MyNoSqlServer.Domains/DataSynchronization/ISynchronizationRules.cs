using System.Collections.Generic;

namespace MyNoSqlServer.Domains.DataSynchronization
{

    public interface ISynchronizationRule
    {
        string Url { get; }
        string TableName { get; }        
    }
    
    public interface ISynchronizationRules
    {
        ISynchronizationRule[] GetRules(string tableName);
        
    }
}