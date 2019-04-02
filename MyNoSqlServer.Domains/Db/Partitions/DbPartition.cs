using System;
using System.Collections.Generic;
using System.Linq;
using Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Partitions
{
    
    /// <summary>
    /// DbPartition Uses SlimLock of Table
    /// </summary>
    public class DbPartition
    {
        public string PartitionKey { get; private set; }
        
        private readonly SortedList<string, DbRow> _rows = new SortedList<string, DbRow>();

        public bool Insert(DbRow row)
        {
            if (_rows.ContainsKey(row.RowKey))
                return false;
            
            _rows.Add(row.RowKey, row);
            return true;
        }

        public void InsertOrReplace(DbRow row)
        {
            if (_rows.ContainsKey(row.RowKey))
                _rows[row.RowKey] = row;
            else
                _rows.Add(row.RowKey, row);
        }


        public DbRow GetRow(string rowKey)
        {
            return _rows.ContainsKey(rowKey) ? _rows[rowKey] : null;
        }

        public bool HasRecord(string rowKey)
        {
            return _rows.ContainsKey(rowKey);
        }
        
        public IReadOnlyList<DbRow> GetAllRows()
        {
            return _rows.Values.ToList();
        }
        
        public IReadOnlyList<DbRow> GetRowsWithLimit(int? limit, int? skip)
        {
            IEnumerable<DbRow> result = _rows.Values;


            if (skip != null)
                result = result.Skip(skip.Value);
            
            if (limit != null)
                result = result.Take(limit.Value);
            
            return result.ToList();
        }
        
        

        public static DbPartition Create(string partitionKey)
        {
            return new DbPartition
            {
                PartitionKey = partitionKey
            };
        }

        public DbRow DeleteRow(string rowKey)
        {
            if (!_rows.ContainsKey(rowKey))
                return null;

            var result = _rows[rowKey];
            _rows.Remove(rowKey);
            return result;
        }

        public void RestoreRecord(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            if (!_rows.ContainsKey(entityInfo.RowKey))
                _rows.Add(entityInfo.RowKey, DbRow.RestoreSnapshot(entityInfo.PartitionKey, entityInfo.RowKey, data));
        }


        public IEnumerable<DbRow> ApplyQuery(IDictionary<string, List<QueryCondition>> conditionsDict)
        {
            var rows = conditionsDict.ContainsKey(DbRowDataUtils.RowKeyField)
                ? _rows.FilterByQueryConditions(conditionsDict[DbRowDataUtils.RowKeyField])
                : _rows.Values;

            if (conditionsDict.ContainsKey(DbRowDataUtils.RowKeyField))
                conditionsDict.Remove(DbRowDataUtils.RowKeyField);

            if (conditionsDict.Count == 0)
            {
                foreach (var row in rows)
                    yield return row;
            }
            else
            {
                foreach (var row in rows)
                    if (row.MatchesQuery(conditionsDict))
                        yield return row;
            }
        }


        public override string ToString()
        {
            return PartitionKey+"; Count: "+_rows.Count;
        }


        public void Clean()
        {
            _rows.Clear();
        }
            
        public IReadOnlyList<DbRow> CleanAndKeepLastRecords(int amount)
        {
            if (amount<0)
                throw new Exception("Amount must be greater than zero");
            
            Queue<KeyValuePair<string, DbRow>> rowsByLastInsertDateTime = null;
            
            var result = new List<DbRow>();
            
            while (_rows.Count>amount)
            {
                if (rowsByLastInsertDateTime == null)
                    rowsByLastInsertDateTime = _rows.OrderBy(itm => itm.Value.Timestamp).ToQueue();
                
                var item = rowsByLastInsertDateTime.Dequeue();
                
                result.Add(item.Value);
                _rows.Remove(item.Key);
            }

            return result;
        }

        public IReadOnlyList<DbRow> GetRows(string[] rowKeys)
        {
            return (from rowKey in rowKeys 
                where _rows.ContainsKey(rowKey) 
                select _rows[rowKey]).ToList();
        }

        public int GetRecordsCount()
        {
            return _rows.Count;
        }
    }
}