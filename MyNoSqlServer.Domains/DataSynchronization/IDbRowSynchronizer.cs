using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    public interface IDbRowSynchronizer
    {
        void Synchronize(string tableName, IReadOnlyList<DbRow> dbRow);

        void SynchronizeDelete(string tableName, IReadOnlyList<DbRow> dbRows);
    }
    
}