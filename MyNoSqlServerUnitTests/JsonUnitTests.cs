using System.Collections.Generic;
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
        
    }



}