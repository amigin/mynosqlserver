using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]    
    public class DataController : Controller
    {
        [HttpGet("Row")]
        public IActionResult List([Required][FromQuery] string tableName, [FromQuery] string partitionKey,
            [FromQuery] string rowKey, [FromQuery] int? limit, [FromQuery] int? skip)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);

            if (partitionKey != null)
            {
                if (rowKey == null)
                {
                    var entities = table.GetRecords(partitionKey, limit, skip);
                    return this.ToDbRowsResult(entities);
                }

                var entity = table.GetEntity(partitionKey, rowKey);

                if (entity == null)
                    return this.RowNotFound(tableName, partitionKey, rowKey);


                return this.ToDbRowResult(entity);
            }

            // PartitionKey == null and RowKey == null
            if (rowKey == null)
            {
                var entities = table.GetAllRecords(limit);
                return this.ToDbRowsResult(entities);
            }

            return Conflict("Not Supported when PartitionKey==null and RowKey!=null");
        }

        [HttpPost("Row/Insert")]
        public IActionResult InsertEntity([Required][FromQuery] string tableName, [Required][FromBody] MyNoSqlDbEntity body)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);


            if (string.IsNullOrEmpty(body.PartitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(body.RowKey))
                return this.RowKeyIsNull();

            var data = Request.BodyAsByteArray();
            var result = table.Insert(body, data);

            return result ? this.ResponseOk() : this.ResponseConflict("Can not insert entity");
        }
        
        [HttpPost("Row/InsertOrReplace")]
        public IActionResult InsertOrReplaceEntity([Required][FromQuery] string tableName, [Required][FromBody] MyNoSqlDbEntity body)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);


            if (string.IsNullOrEmpty(body.PartitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(body.RowKey))
                return this.RowKeyIsNull();
            
            var data = Request.BodyAsByteArray();
            table.InsertOrReplace(body, data);

            return this.ResponseOk();
        }

        [HttpDelete("Row")]
        public IActionResult Delete([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey,
            [Required][FromQuery] string rowKey)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            if (string.IsNullOrEmpty(partitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(rowKey))
                return this.RowKeyIsNull();

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);


            var result = table.DeleteRow(partitionKey, rowKey);

            if (result)
                return this.ResponseOk();

            return this.RowNotFound(tableName, partitionKey, rowKey);
        }

    }
}