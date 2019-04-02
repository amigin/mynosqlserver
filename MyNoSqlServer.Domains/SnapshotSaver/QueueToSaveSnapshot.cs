using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{


    public interface ISyncTask
    {
        DateTime SyncDateTime { get; }
    }
    
    public class SyncPartition : ISyncTask
    {
        public DbTable DbTable { get; private set; }
        public DbPartition DbPartition { get; set; }
        
        public DateTime SyncDateTime { get; } = DateTime.UtcNow.AddSeconds(1);

        public static SyncPartition Create(DbTable dbTable, DbPartition dbPartition)
        {
            return new SyncPartition
            {
                DbTable = dbTable,
                DbPartition = dbPartition
            };
        }


    }
    
    
    public class SyncTable: ISyncTask
    {
        public DbTable DbTable { get; private set; }
        
        public DateTime SyncDateTime { get; } = DateTime.UtcNow.AddSeconds(1);

        public static SyncTable Create(DbTable dbTable)
        {
            return new SyncTable
            {
                DbTable = dbTable
            };
        }
        
    }

    public class SyncDeletePartition: ISyncTask
    {
        public string TableName { get; private set; }
        public string PartitionKey { get; private set; }
        
        public DateTime SyncDateTime { get; } = DateTime.UtcNow.AddSeconds(1);

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
        private readonly Dictionary<string, List<ISyncTask>> _queue = new Dictionary<string, List<ISyncTask>>();

        private List<ISyncTask> GetQueueForTable(string tableName)
        {
            if (_queue.ContainsKey(tableName))
                return _queue[tableName];
            
            var queue = new List<ISyncTask>();
            
              _queue.Add(tableName, queue);

            return queue;
        }



        private static bool HasSynchronizationTask<T>(List<ISyncTask> queue, Func<T, bool> func) where T:ISyncTask
        {
            foreach (var item in queue)
            {
                if (item is T myType)
                {
                    if (func(myType))
                        return true;
                }
            }

            return false;

        }
        public void Enqueue(DbTable dbTable, DbPartition partitionToSave)
        {
            lock (_queue)
            {
                var queue = GetQueueForTable(dbTable.Name);

                if (HasSynchronizationTask<SyncPartition>(queue,
                    syncTask => syncTask.DbPartition.PartitionKey == partitionToSave.PartitionKey))
                    return;

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

                if (HasSynchronizationTask<SyncDeletePartition>(queue,
                    syncTask => syncTask.PartitionKey == partitionToDelete))
                    return;
                
                var elementToEnqueue = SyncDeletePartition.Create(tableName, partitionToDelete);
                queue.Add(elementToEnqueue);

            }
        }


        private void CollectGarbage()
        {
            if (_queue.Count <= 0) return;
            
            var emptyElements = _queue.Where(itm => itm.Value.Count == 0).ToList();
            foreach (var emptyElement in emptyElements)
                _queue.Remove(emptyElement.Key);
        }
        public object Dequeue()
        {

            var dt = DateTime.UtcNow;
            lock (_queue)
            {
                try
                {
                    foreach (var subQueue in _queue)
                    {
                        for (var i = 0; i < subQueue.Value.Count; i++)
                        {
                            if (dt < subQueue.Value[i].SyncDateTime) 
                                continue;
                            
                            var result = subQueue.Value[i];
                            subQueue.Value.RemoveAt(i);
                            return result;
                        }
                    }
                }
                finally
                {
                    CollectGarbage();
                }
            }
            return null;
        }
        
        
    }
}