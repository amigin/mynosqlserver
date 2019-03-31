using System.Collections.Generic;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Services
{
    public class ChangesPublisherToSignalR : IChangesPublisher
    {


        public void PublishInitTable(DbTable dbTable)
        {
            ChangesHub.BroadCastInit(dbTable);
        }

        public void PublishInitPartition(DbTable dbTable, DbPartition partition)
        {
            ChangesHub.BroadCastInit(dbTable, partition);
        }

        public void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRow)
        {
            ChangesHub.BroadcastChange(dbTable, dbRow);
        }

        public void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
            ChangesHub.BroadcastDelete(dbTable, dbRows);
        }
    }
    
}