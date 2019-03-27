using System.Linq;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Query;
using Xunit;

namespace MyNoSqlServerUnitTests
{

    public class TestRecord : MyNoSqlDbEntity
    {
        public string TestField { get; set; }
    }
    
    public class TestQueryConditions
    {

        [Fact]
        public void TestSimpleQuery()
        {
            var dbTable = DbTable.CreateByRequest("myTable");

            var recordToInsert = new TestRecord
            {
                PartitionKey = "MyPartition",
                RowKey = "MyRow",
                TestField = "Test"
            };

            var recordIsByteArray = recordToInsert.AsByteArray();
            
            dbTable.Insert(recordToInsert, recordIsByteArray);

            var query = "PartitionKey eq 'MyPartition' and RowKey eq 'MyRow'";

            var queryCondition = query.ParseQueryConditions();
            
            var result = dbTable.ApplyQuery(queryCondition).Select(itm => itm.Data.DeserializeDbEntity<TestRecord>()).First();

            Assert.Equal(recordToInsert.TestField, result.TestField);

        }
        
        [Fact]
        public void TestSimpleRangeQuery()
        {
            var dbTable = DbTable.CreateByRequest("myTable");

            for (var i = 0; i < 100; i++)
            {
                var key = (i * 2).ToString("000");
                var recordToInsert = new TestRecord
                {
                    PartitionKey = "MyPartition",
                    RowKey = key,
                    TestField = key
                };
                
                var recordIsByteArray = recordToInsert.AsByteArray();
            
                dbTable.Insert(recordToInsert, recordIsByteArray);
            }

            var query = "PartitionKey eq 'MyPartition' and RowKey ge '001' and RowKey le '003'";

            var queryCondition = query.ParseQueryConditions();
            
            var result = dbTable.ApplyQuery(queryCondition).Select(itm => itm.Data.DeserializeDbEntity<TestRecord>()).ToArray();

            Assert.Single(result);

            Assert.Equal("002", result[0].TestField);

        }
        
        [Fact]
        public void TestSimpleRangeAboveQuery()
        {
            var dbTable = DbTable.CreateByRequest("myTable");

            for (var i = 0; i <= 100; i++)
            {
                var key = (i * 2).ToString("000");
                var recordToInsert = new TestRecord
                {
                    PartitionKey = "MyPartition",
                    RowKey = key,
                    TestField = key
                };
                
                var recordIsByteArray = recordToInsert.AsByteArray();
            
                dbTable.Insert(recordToInsert, recordIsByteArray);
            }

            var query = "PartitionKey eq 'MyPartition' and RowKey ge '199'";

            var queryCondition = query.ParseQueryConditions();
            
            var result = dbTable.ApplyQuery(queryCondition).Select(itm => itm.Data.DeserializeDbEntity<TestRecord>()).ToArray();

            Assert.Single(result);

            Assert.Equal("200", result[0].TestField);

        } 
        
        
    }
    
    
    
}