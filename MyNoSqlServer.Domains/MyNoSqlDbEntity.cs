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
        [JsonProperty("partitionKey")]        
        public string PartitionKey { get; set; }
        [JsonProperty("rowKey")]                
        public string RowKey { get; set; }
        [JsonProperty("timestamp")]                        
        public string Timestamp { get; set; }
    }

}