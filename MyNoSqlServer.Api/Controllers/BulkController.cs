using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Common;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    [Route("Bulk/[Action]")]
    public class BulkController : Controller
    {
        [HttpPost]
        public IActionResult InsertOrReplace([Required][FromQuery] string tableName, [Required][FromBody] MyNoSqlDbEntity[] body)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();
            
            var table = DbInstance.CreateTableIfNotExists(tableName);

            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            var (dbPartitions, dbRows) = table.BulkInsertOrReplace(entitiesToInsert);

            foreach (var dbPartition in dbPartitions)
                ServiceLocator.SnapshotSaverEngine.SynchronizePartition(table, dbPartition);

            ServiceLocator.Synchronizer.ChangesPublisher?.SynchronizeUpdate(table, dbRows);
            
            return this.ResponseOk();

        }




        private static void CleanPartitionAndBulkInsert(DbTable table, IEnumerable<ArraySpan<byte>> entitiesToInsert, string partitionKey)
        {
            var partitionsToSynchronize = table.CleanAndBulkInsert(partitionKey, entitiesToInsert);

            foreach (var dbPartition in partitionsToSynchronize)
            {
                ServiceLocator.SnapshotSaverEngine.SynchronizePartition(table, dbPartition);
                ServiceLocator.Synchronizer.ChangesPublisher?.PublishInitPartition(table, dbPartition); 
            }
        }
        
        
        private static void CleanTableAndBulkInsert(DbTable table, IEnumerable<ArraySpan<byte>> entitiesToInsert)
        {
            table.CleanAndBulkInsert(entitiesToInsert);
            
            ServiceLocator.SnapshotSaverEngine.SynchronizeTable(table);
            ServiceLocator.Synchronizer.ChangesPublisher?.PublishInitTable(table);
        }


        [HttpPost]
        public IActionResult CleanAndBulkInsert([Required] [FromQuery] string tableName,
            [FromQuery] string partitionKey, [Required] [FromBody] MyNoSqlDbEntity[] body)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.CreateTableIfNotExists(tableName);
            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            if (string.IsNullOrEmpty(partitionKey))
                CleanTableAndBulkInsert(table, entitiesToInsert);
            else
                CleanPartitionAndBulkInsert(table, entitiesToInsert, partitionKey);

            return this.ResponseOk();
        }

    }

}