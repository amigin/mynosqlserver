using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    public interface IDbRowSynchronizer
    {
        void Synchronize(string tableName, DbRow[] dbRow);
    }
    
}