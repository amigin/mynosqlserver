using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    
    public class PartitionSnapshot
    {
        public string TableName { get; set; }
        public string PartitionKey { get; set; }
        public byte[] Snapshot { get; set; }

        public static PartitionSnapshot Create(string tableName, string partitionKey, byte[] snapshot)
        {
            return new PartitionSnapshot
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                Snapshot = snapshot
            };
        }


        public override string ToString()
        {
            return TableName + "/" + PartitionKey;
        }
    }
    
    public interface ISnapshotStorage
    {
        Task SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot);
        Task SaveTableSnapshotAsync(DbTable dbTable);

        Task DeleteTablePartitionAsync(string tableName, string partitionKey);

        Task LoadSnapshotsAsync(Action<IEnumerable<string>> tablesCallback, Action<PartitionSnapshot> callback);
    }
}