using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]    
    
    public class RowController : Controller
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

                return entity == null 
                    ? this.RowNotFound(tableName, partitionKey, rowKey) 
                    : this.ToDbRowResult(entity);
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

            var table = DbInstance.CreateTableIfNotExists(tableName);


            if (string.IsNullOrEmpty(body.PartitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(body.RowKey))
                return this.RowKeyIsNull();

            if (table.HasRecord(body))
                this.ResponseConflict("Record with the same PartitionKey and RowKey is already exists");

            var data = Request.BodyAsByteArray();
            
            var (dbPartition, dbRow) = table.Insert(body, data);

            if (dbPartition != null)
            {
                ServiceLocator.SnapshotSaverEngine.Synchronize(table.Name, dbPartition);
                ServiceLocator.Synchronizer.DbRowSynchronizer?.SynchronizeUpdate(table.Name, new[]{dbRow});
            }

            return dbPartition != null ? this.ResponseOk() : this.ResponseConflict("Can not insert entity");
        }
        
        [HttpPost("Row/InsertOrReplace")]
        public IActionResult InsertOrReplaceEntity([Required][FromQuery] string tableName, [Required][FromBody] MyNoSqlDbEntity body)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.CreateTableIfNotExists(tableName);

            if (string.IsNullOrEmpty(body.PartitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(body.RowKey))
                return this.RowKeyIsNull();
            
            var data = Request.BodyAsByteArray();
            var (dbPartition, dbRow) = table.InsertOrReplace(body, data);
            
            ServiceLocator.SnapshotSaverEngine.Synchronize(table.Name, dbPartition);
            ServiceLocator.Synchronizer.DbRowSynchronizer?.SynchronizeUpdate(table.Name, new[]{dbRow});

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

            var (dbPartition, dbRow) = table.DeleteRow(partitionKey, rowKey);

            if (dbPartition == null) 
                return this.RowNotFound(tableName, partitionKey, rowKey);
            
            ServiceLocator.SnapshotSaverEngine.Synchronize(tableName, dbPartition);
            ServiceLocator.Synchronizer.DbRowSynchronizer.SynchronizeDelete(tableName, new[]{dbRow});
            
            return this.ResponseOk();

        }

        
        [HttpDelete("CleanAndKeepLastRecords")]
        public IActionResult CleanAndKeepLastRecords([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey, [Required][FromQuery] int amount)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            if (string.IsNullOrEmpty(partitionKey))
                return this.PartitionKeyIsNull();
            
            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);


            var (dbPartition, dbRows) = table.CleanAndKeepLastRecords(partitionKey, amount);

            if (dbPartition != null)
            {
                ServiceLocator.SnapshotSaverEngine.Synchronize(tableName, dbPartition);
                ServiceLocator.Synchronizer.DbRowSynchronizer.SynchronizeDelete(tableName, dbRows);
            }
            
            return this.ResponseOk();
        }

    }
}