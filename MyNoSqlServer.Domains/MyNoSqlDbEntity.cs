using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace MyNoSqlServer.Domains
{

    public interface IMyNoSqlDbEntity
    {
        string PartitionKey { get; }
        string RowKey { get; }
        string Timestamp { get; }
    }

    public class MyNoSqlDbEntity : IMyNoSqlDbEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Timestamp { get; set; }

    }

}