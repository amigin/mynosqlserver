using System.Linq;
using MyNoSqlServer.Domains.Query;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class TestQueryConditions
    {


        [Fact]
        public void TestSimpleQueryCondition()
        {

            var query = "MyField eq 'My string'";


            var conditions = query.ParseQueryConditions().ToArray();


            Assert.Single(conditions);
            Assert.Equal("MyField", conditions[0].FieldName);
            Assert.Equal(QueryOperation.Eq, conditions[0].Operation);
            Assert.Equal("'My string'", conditions[0].Value);

        }
        
        [Fact]
        public void TestQueryConditionWithOneAnd()
        {

            var query = "PartitionKey eq 'My string' and RowKey ge '45' and RowKey lt 50";


            var conditions = query.ParseQueryConditions().ToArray();


            Assert.Equal(3, conditions.Length);
            Assert.Equal("PartitionKey", conditions[0].FieldName);
            Assert.Equal(QueryOperation.Eq, conditions[0].Operation);
            Assert.Equal("'My string'", conditions[0].Value);

            Assert.Equal("RowKey", conditions[1].FieldName);
            Assert.Equal(QueryOperation.Ge, conditions[1].Operation);
            Assert.Equal("'45'", conditions[1].Value);

            Assert.Equal("RowKey", conditions[2].FieldName);
            Assert.Equal(QueryOperation.Lt, conditions[2].Operation);
            Assert.Equal("50", conditions[2].Value);

        }
        
    }
}