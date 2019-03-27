using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Common;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Tables
{

    public class DbTable
    {
        private DbTable(string name)
        {
            Name = name;
        }

        public static DbTable CreateByRequest(string name)
        {
            return new DbTable(name);
        }
        
        public static DbTable CreateByInit(string name)
        {
            return new DbTable(name);
        }
        
        public string Name { get; }
        
        internal readonly ReaderWriterLockSlim ReaderWriterLockSlim = new ReaderWriterLockSlim();
        
        public readonly SortedList<string, DbPartition> Partitions = new SortedList<string, DbPartition>();


        public void InitPartitionFromSnapshot(string partitionKey, byte[] data)
        {
            
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                
                foreach (var dbRowMemory in data.SplitJsonArrayToObjects())
                {
                    var array = dbRowMemory.AsArray();
                    var jsonString = Encoding.UTF8.GetString(array);
                    var entityInfo =
                        Newtonsoft.Json.JsonConvert.DeserializeObject<MyNoSqlDbEntity>(jsonString);
                    
                    if (entityInfo.PartitionKey != partitionKey)
                        continue;
                    
                    if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                        Partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));
                    
                    var partition = Partitions[entityInfo.PartitionKey];
                    
                    var dbRow = DbRow.CreateNew(entityInfo.PartitionKey, entityInfo.RowKey, array);

                    partition.InsertOrReplace(dbRow);
                   
                }
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

        }

        public (DbPartition partition, DbRow dbRow) Insert(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                    Partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = Partitions[entityInfo.PartitionKey];
                
                var dbRow = DbRow.CreateNew(entityInfo.PartitionKey, entityInfo.RowKey, data);
                
                if (partition.Insert(dbRow))
                    return (partition, dbRow);
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return (null, null);
        }
        
        public (DbPartition partition, DbRow dbRow) InsertOrReplace(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
           
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!Partitions.ContainsKey(entityInfo.PartitionKey))
                    Partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = Partitions[entityInfo.PartitionKey];
                
                var dbRow = DbRow.CreateNew(entityInfo.PartitionKey, entityInfo.RowKey, data);
                partition.InsertOrReplace(dbRow);
                
                return (partition, dbRow);
            }
            finally
            {
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
        

        public IReadOnlyList<DbRow> GetAllRecords(int? limit)
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

        public DbPartition DeleteRow(string partitionKey, string rowKey)
        {
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!Partitions.ContainsKey(partitionKey))
                    return null;

                var partition = Partitions[partitionKey];
                
                if (partition.DeleteRow(rowKey))
                  return partition;
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return null;
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


        public (IEnumerable<DbPartition> partitions, IReadOnlyList<DbRow> rows) BulkInsertOrReplace(IEnumerable<ArraySpan<byte>> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .AsArray()
                    .ToDbRow())
                .ToArray();
            
            
            var partitionsToSync = new Dictionary<string, DbPartition>();
            
            var rowsToSync = new List<DbRow>();
            
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbRow in dbRows)
                {
                    if (!Partitions.ContainsKey(dbRow.PartitionKey))
                        Partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = Partitions[dbRow.PartitionKey];

                    partition.InsertOrReplace(dbRow);
                    
                    if (!partitionsToSync.ContainsKey(partition.PartitionKey))
                        partitionsToSync.Add(partition.PartitionKey, partition);
                    
                    rowsToSync.Add(dbRow);
                }
                
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
           
            }


            return (partitionsToSync.Values, rowsToSync);
        }



        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            var conditionsDict = queryConditions.GroupBy(itm => itm.FieldName).ToDictionary(itm => itm.Key, itm => itm.ToList());

            var partitions = conditionsDict.ContainsKey(DbRowDataUtils.PartitionKeyField)
                ? Partitions.FilterByQueryConditions(conditionsDict[DbRowDataUtils.PartitionKeyField]).ToList()
                : Partitions.Values.ToList();

            if (conditionsDict.ContainsKey(DbRowDataUtils.PartitionKeyField))
                conditionsDict.Remove(DbRowDataUtils.PartitionKeyField);
                
            foreach (var partition in partitions)
                foreach (var dbRow in partition.ApplyQuery(conditionsDict))
                    yield return dbRow;
            
        }
        
    }
}