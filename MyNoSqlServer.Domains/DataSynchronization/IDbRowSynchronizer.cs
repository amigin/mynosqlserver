using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    public interface IDbRowSynchronizer
    {
        void Synchronize(string tableName, DbRow[] dbRow);
    }
    
}