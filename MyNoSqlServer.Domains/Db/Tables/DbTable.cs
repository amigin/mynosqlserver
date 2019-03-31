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
        
        private readonly SortedList<string, DbPartition> _partitions = new SortedList<string, DbPartition>();


        public IReadOnlyList<DbPartition> GetAllPartitions()
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.Values.ToList();
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        public DbPartition GetPartition(string partitionKey)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey] : null;
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


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
                    
                    if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                        _partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));
                    
                    var partition = _partitions[entityInfo.PartitionKey];
                    
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
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    _partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = _partitions[entityInfo.PartitionKey];
                
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
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    _partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = _partitions[entityInfo.PartitionKey];
                
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

                if (!_partitions.ContainsKey(partitionKey))
                    return null;
                var partition = _partitions[partitionKey];

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
                    foreach (var partition in _partitions.Values)
                        result.AddRange(partition.GetAllRows());
                }
                else
                {
                    foreach (var partition in _partitions.Values)
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
                if (!_partitions.ContainsKey(partitionKey))
                    return Array.Empty<DbRow>();

                var partition = _partitions[partitionKey];

                if (skip == null && limit == null)
                    return partition.GetAllRows();
                

                return partition.GetRowsWithLimit(limit, skip);
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }

        public (DbPartition dbPartition, DbRow dbRow) DeleteRow(string partitionKey, string rowKey)
        {
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(partitionKey))
                    return (null, null);

                var partition = _partitions[partitionKey];

                var row = partition.DeleteRow(rowKey);
                
                if (row != null)
                  return (partition, row);
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return (null, null);
        }
        
        public (DbPartition dbPartition, IReadOnlyList<DbRow> dbRows) CleanAndKeepLastRecords(string partitionKey, int amount)
        {
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(partitionKey))
                    return (null,null);

                var partition = _partitions[partitionKey];

                var dbRows = partition.CleanAndKeepLastRecords(amount);
                
                return (partition, dbRows);
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }
        
        internal void RestoreRecord(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                _partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

            var partition = _partitions[entityInfo.PartitionKey];

            partition.RestoreRecord(entityInfo, data);

        }


        public bool HasRecord(IMyNoSqlDbEntity entityInfo)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    return false;

                var partition = _partitions[entityInfo.PartitionKey];

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
                .ToList();
            
            
            var partitionsToSync = new Dictionary<string, DbPartition>();
            
            var rowsToSync = new List<DbRow>();
            
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbRow in dbRows)
                {
                    if (!_partitions.ContainsKey(dbRow.PartitionKey))
                        _partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = _partitions[dbRow.PartitionKey];

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


        public void CleanAndBulkInsert(IEnumerable<ArraySpan<byte>> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .AsArray()
                    .ToDbRow())
                .ToList();
            
            ReaderWriterLockSlim.EnterWriteLock();
            
            try
            {
                _partitions.Clear();
                
                foreach (var dbRow in dbRows)
                {
                    if (!_partitions.ContainsKey(dbRow.PartitionKey))
                        _partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = _partitions[dbRow.PartitionKey];

                    partition.InsertOrReplace(dbRow);
                }
                
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }
        
        public IEnumerable<DbPartition> CleanAndBulkInsert(string partitionKey, IEnumerable<ArraySpan<byte>> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .AsArray()
                    .ToDbRow())
                .ToArray();
            
            ReaderWriterLockSlim.EnterWriteLock();
            
            

            var result = new Dictionary<string, DbPartition>();
            
            try
            {
                if (_partitions.ContainsKey(partitionKey))
                    _partitions[partitionKey].Clean();
                
                foreach (var dbRow in dbRows)
                {
                    if (!_partitions.ContainsKey(dbRow.PartitionKey))
                        _partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = _partitions[dbRow.PartitionKey];

                    partition.InsertOrReplace(dbRow);

                    if (!result.ContainsKey(dbRow.PartitionKey))
                        result.Add(dbRow.PartitionKey, partition);
                }
                
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return result.Values;
        }
        
        public void Clean()
        {
            ReaderWriterLockSlim.EnterWriteLock();
            
            try
            {
                _partitions.Clear();
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }


        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            var conditionsDict = queryConditions.GroupBy(itm => itm.FieldName).ToDictionary(itm => itm.Key, itm => itm.ToList());

            var partitions = conditionsDict.ContainsKey(DbRowDataUtils.PartitionKeyField)
                ? _partitions.FilterByQueryConditions(conditionsDict[DbRowDataUtils.PartitionKeyField]).ToList()
                : _partitions.Values.ToList();

            if (conditionsDict.ContainsKey(DbRowDataUtils.PartitionKeyField))
                conditionsDict.Remove(DbRowDataUtils.PartitionKeyField);
                
            foreach (var partition in partitions)
                foreach (var dbRow in partition.ApplyQuery(conditionsDict))
                    yield return dbRow;
            
        }


    }
}