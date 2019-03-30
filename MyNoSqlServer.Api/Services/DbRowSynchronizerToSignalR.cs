using System.Collections.Generic;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Services
{
    public class DbRowSynchronizerToSignalR : IDbRowSynchronizer
    {
        public void Synchronize(string tableName, IReadOnlyList<DbRow> dbRow)
        {
            ChangesHub.BroadcastChange(tableName, dbRow);
        }

        public void SynchronizeDelete(string tableName, IReadOnlyList<DbRow> dbRows)
        {
            ChangesHub.BroadcastDelete(tableName, dbRows);
        }
    }
    
}