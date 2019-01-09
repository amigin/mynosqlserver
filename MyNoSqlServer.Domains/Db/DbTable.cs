using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace MyNoSqlServer.Domains.Db
{

    public class DbTable
    {
        private DbTable(string name)
        {
            Name = name;
        }

        public static DbTable CreateByRequest(string name)
        {
            var result = new DbTable(name);
            result.UpdateSnapshotId();
            return result;
        }
        
        public static DbTable CreateByInit(string name)
        {
            return new DbTable(name);
        }
        
        public string Name { get; }

        public string SnapshotId;

        private void UpdateSnapshotId()
        {
            SnapshotId = Guid.NewGuid().ToString("N");
        }
        
        internal readonly ReaderWriterLockSlim ReaderWriterLockSlim = new ReaderWriterLockSlim();
        
        public readonly SortedDictionary<string, DbPartition> Partitions = new SortedDictionary<string, DbPartition>();

        public bool Insert(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            var dbRow = DbRow.CreateNew(entityInfo.PartitionKey, entityInfo.RowKey, data);
            
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                    Partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = Partitions[entityInfo.PartitionKey];

                return partition.Insert(dbRow);
                
            }
            finally
            {
                UpdateSnapshotId();
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }
        
        public void InsertOrReplace(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            var dbRow = DbRow.CreateNew(entityInfo.PartitionKey, entityInfo.RowKey, data);
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                    Partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = Partitions[entityInfo.PartitionKey];

                partition.InsertOrReplace(dbRow);
            }
            finally
            {
                UpdateSnapshotId();                
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }    

        public DbRow GetEntity(string partitionKey, string rowKey)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {

                if (!Partitions.ContainsKey(partitionKey))
                    return null;
                var partition = Partitions[partitionKey];

                return partition.GetRow(rowKey);

            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }
        

        public IEnumerable<DbRow> GetAllRecords(int? limit)
        {
            var result = new List<DbRow>();
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                if (limit == null)
                {
                    foreach (var partition in Partitions.Values)
                        result.AddRange(partition.GetAllRows());
                }
                else
                {
                    foreach (var partition in Partitions.Values)
                    foreach (var dbRow in partition.GetAllRows())
                    {
                        result.Add(dbRow);
                        if (result.Count >= limit.Value)
                            return result;
                    }
                }
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }

            return result;
        }

        public IEnumerable<DbRow> GetRecords(string partitionKey, int? limit, int? skip)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                if (!Partitions.ContainsKey(partitionKey))
                    return Array.Empty<DbRow>();

                var partition = Partitions[partitionKey];

                if (skip == null && limit == null)
                    return partition.GetAllRows();
                

                return partition.GetRowsWithLimit(limit, skip);
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }

        public bool DeleteRow(string partitionKey, string rowKey)
        {
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!Partitions.ContainsKey(partitionKey))
                    return false;

                var partition = Partitions[partitionKey];

                return partition.DeleteRow(rowKey);
            }
            finally
            {
                UpdateSnapshotId();                  
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }
        
        internal void RestoreRecord(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                Partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

            var partition = Partitions[entityInfo.PartitionKey];

            partition.RestoreRecord(entityInfo, data);

        }


        public bool HasRecord(IMyNoSqlDbEntity entityInfo)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                    return false;

                var partition = Partitions[entityInfo.PartitionKey];

                return partition.HasRecord(entityInfo.RowKey);
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        public void BulkInsertOrReplace(IEnumerable<ArraySpan> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .AsArray()
                    .ToDbRow())
                .ToArray();
            
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbRow in dbRows)
                {
                    if (!Partitions.ContainsKey(dbRow.PartitionKey))
                        Partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = Partitions[dbRow.PartitionKey];

                    partition.InsertOrReplace(dbRow);
                }
            }
            finally
            {
                UpdateSnapshotId();                
                ReaderWriterLockSlim.ExitWriteLock();
            }
            
        }
    }
}