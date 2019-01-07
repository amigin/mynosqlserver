using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Domains.Db
{
    
    /// <summary>
    /// This Object Uses SlimLock of Table
    /// </summary>
    public class DbPartition
    {
        private string PartitionKey { get; set; }
        
        private readonly SortedDictionary<string, DbRow> _rows = new SortedDictionary<string, DbRow>();

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
        
        public DbRow[] GetAllRows()
        {
            return _rows.Values.ToArray();
        }
        
        public DbRow[] GetRowsWithLimit(int? limit, int? skip)
        {
            IEnumerable<DbRow> result = _rows.Values;


            if (skip != null)
                result = result.Skip(skip.Value);
            
            if (limit != null)
                result = result.Take(limit.Value);
            
            return result.ToArray();
        }
        
        

        public static DbPartition Create(string partitionKey)
        {
            return new DbPartition
            {
                PartitionKey = partitionKey
            };
        }

        public bool DeleteRow(string rowKey)
        {
            if (!_rows.ContainsKey(rowKey))
                return false;

            _rows.Remove(rowKey);
            return true;
        }

        public void InitRecord(IMyNoSqlDbEntity entityInfo, byte[] data)
        {
            if (!_rows.ContainsKey(entityInfo.RowKey))
                _rows.Add(entityInfo.RowKey, DbRow.CreateNew(entityInfo, data));
        }


        public override string ToString()
        {
            return PartitionKey+"; Count: "+_rows.Count;
        }
    }
}