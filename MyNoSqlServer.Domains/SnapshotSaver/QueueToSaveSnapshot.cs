using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{

    public class SyncPartition
    {
        public DbTable DbTable { get; private set; }
        public DbPartition DbPartition { get; set; }

        public static SyncPartition Create(DbTable dbTable, DbPartition dbPartition)
        {
            return new SyncPartition
            {
                DbTable = dbTable,
                DbPartition = dbPartition
            };
        }
    }
    
    
    public class SyncTable
    {
        public DbTable DbTable { get; private set; }

        public static SyncTable Create(DbTable dbTable)
        {
            return new SyncTable
            {
                DbTable = dbTable
            };
        }
    }

    public class SyncDeletePartition
    {
        public string TableName { get; private set; }
        public string PartitionKey { get; private set; }

        public static SyncDeletePartition Create(string tableName, string partitionKey)
        {
            return new SyncDeletePartition
            {
                TableName = tableName,
                PartitionKey = partitionKey
            };
        }
    }
    
    public class QueueToSaveSnapshot
    {
        private readonly Dictionary<string, List<object>> _queue = new Dictionary<string, List<object>>();

        private List<object> GetQueueForTable(string tableName)
        {
            if (_queue.ContainsKey(tableName))
                return _queue[tableName];
            
            var queue = new List<object>();
            
              _queue.Add(tableName, queue);

            return queue;
        }

        public void Enqueue(DbTable dbTable, DbPartition partitionToSave)
        {
            lock (_queue)
            {

                var queue = GetQueueForTable(dbTable.Name);

                var elementToEnqueue = SyncPartition.Create(dbTable, partitionToSave);
                
                queue.Add(elementToEnqueue);

            }
        }

        public void Enqueue(DbTable dbTable)
        {
            lock (_queue)
            {
                var queue = GetQueueForTable(dbTable.Name);
                
                if (queue.Count > 0)
                    queue.Clear();
                
                queue.Add(SyncTable.Create(dbTable));
            }
        }
        
        public void EnqueueDeletePartition(string tableName, string partitionToDelete)
        {
            lock (_queue)
            {
                var queue = GetQueueForTable(tableName);
                var elementToEnqueue = SyncDeletePartition.Create(tableName, partitionToDelete);
                queue.Add(elementToEnqueue);
            }
        }

        public object Dequeue()
        {
            lock (_queue)
            {
                if (_queue.Count == 0)
                    return null;

                var queueGroup = _queue.First();

                var queue = queueGroup.Value;

                if (queue.Count == 0)
                    return null;
                
                var result = queue[0];
                
                queue.RemoveAt(0);

                if (queue.Count == 0)
                    _queue.Remove(queueGroup.Key);


                return result;
            }
        }
        
        
    }
}