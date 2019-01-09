using System;
using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Domains.Db
{
    public class DbRow
    {
        private DbRow(string partitionKey, string rowKey, string timestamp, byte[] data)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new Exception("Partition key can not be empty");
            
            if (string.IsNullOrEmpty(rowKey))
                throw new Exception("Row key can not be empty");

            PartitionKey = partitionKey;
            RowKey = rowKey;
            Timestamp = timestamp;
            Data = data;
        }
        
        public string PartitionKey { get; }

        public string RowKey { get; }
        public string Timestamp { get; }
        public byte[] Data { get; }

        public static DbRow CreateNew(string partitionKey, string rowKey, byte[] data)
        {
            var timeStamp = DateTime.UtcNow.ToTimeStampString();
            data = data.InjectTimeStamp(timeStamp).ToArray();
            return new DbRow(partitionKey, rowKey, timeStamp, data);
        }

        public static DbRow RestoreSnapshot(string partitionKey, string rowKey, byte[] data)
        {
            var timeStamp = DateTime.UtcNow.ToTimeStampString();
            data = data.InjectTimeStamp(timeStamp).ToArray();
            return new DbRow(partitionKey, rowKey, timeStamp, data);
        }

        public static DbRow CreateNew(byte[] data)
        {
            var keyValue = new Dictionary<string, string>
            {
                [DbRowDataUtils.PartitionKeyField] = null,
                [DbRowDataUtils.RowKeyField] = null,
            };
            var timeStamp = DateTime.UtcNow.ToTimeStampString();
            data = data.InjectTimeStamp(timeStamp, keyValue).ToArray();


            return new DbRow(keyValue[DbRowDataUtils.PartitionKeyField],
                keyValue[DbRowDataUtils.RowKeyField],
                timeStamp,
                data);
        }

    }



    public static class DbRowHelpers
    {
        public static DbRow ToDbRow(this byte[] byteArray)
        {
            return DbRow.CreateNew(byteArray);
        }
    }
    
}