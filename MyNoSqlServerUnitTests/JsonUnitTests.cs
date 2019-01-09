using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class JsonUnitTests
    {
        [Fact]
        public void SimpleFieldsTimeStampInserting()
        {
            var fieldA = "\"Fiel\\\"dA\"";
            var valueA = "\"ValueA\"";
            var fieldB = "\"FieldA\"";
            var valueB = "null";
            
            var fieldC = "\"FieldC\"";
            var valueC = "[{\"A\":[1,2,3]},{\"B\":\"[\"}]";

            
            var example = "{"+$"{fieldA}:{valueA},{fieldB}:{valueB},{fieldC}:{valueC}"+"}";

            var byteArray = Encoding.UTF8.GetBytes(example);

            var result = new List<(ArraySpan key, ArraySpan value)>();

            foreach (var kvp in byteArray.ParseFirstLevelOfJson())
            {
                result.Add(kvp);
            }
            
            Assert.True(result.Count==3);
            
            Assert.True(result[0].key.AsString() == fieldA);
            Assert.True(result[0].value.AsString() == valueA);
            
            Assert.True(result[1].key.AsString() == fieldB);
            Assert.True(result[1].value.AsString() == valueB);
            
            Assert.True(result[2].key.AsString() == fieldC);
            Assert.True(result[2].value.AsString() == valueC);
            
            
        }


        [Fact]
        public void CheckReadingKeysWhileInsertingTimestamp()
        {
            var fieldA = "\"Fiel\\\"dA\"";
            var valueA = "\"ValueA\"";
            var fieldB = "\"PartitionKey\"";
            var valueB = "\"ABC\"";
            
            var fieldC = "\"FieldC\"";
            var valueC = "[{\"A\":[1,2,3]},{\"B\":\"[\"}]";
            
            var example = "{"+$"{fieldA}:{valueA},{fieldB}:{valueB},{fieldC}:{valueC}"+"}";

            var keyValue = new Dictionary<string, string>
            {
                ["PartitionKey"] = null,
                ["RowKey"] = null,
            };

            var bytes = Encoding.UTF8.GetBytes(example);

            bytes = bytes.InjectTimeStamp("timestamp", keyValue).ToArray();

            var json = Encoding.UTF8.GetString(bytes);

            var jsonDeserialized = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
            
            
            Assert.Equal("ABC", keyValue["PartitionKey"]);
            Assert.Null(keyValue["RowKey"]);
            
            Assert.Equal("timestamp", (string)jsonDeserialized["Timestamp"]);

        }
        
    }



}