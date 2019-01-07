using System;
using System.Linq;

namespace MyNoSqlServer.Domains.Db
{
    public class DbRow
    {
        private DbRow(string rowKey, string timestamp, byte[] data)
        {
            RowKey = rowKey;
            Timestamp = timestamp;
            Data = data;
        }
        
        public string RowKey { get; }
        public string Timestamp { get; }
        public byte[] Data { get; }

        public static DbRow CreateNew(IMyNoSqlDbEntity dbEntity, byte[] data)
        {
            var timestamp = DateTime.UtcNow.ToString("O");
            data = data.InjectTimestamp(timestamp).ToArray();
            return new DbRow(dbEntity.RowKey, timestamp, data);
        }

    }
    
}