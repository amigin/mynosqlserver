using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Common;
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
        public IActionResult InsertOrReplace([Required][FromQuery] string tableName, [Required][FromBody] MyNoSqlDbEntity[] body, 
            [FromQuery]string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;
            
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();
            
            var table = DbInstance.CreateTableIfNotExists(tableName);

            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            var (dbPartitions, dbRows) = table.BulkInsertOrReplace(entitiesToInsert);

            foreach (var dbPartition in dbPartitions)
                ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());

            ServiceLocator.DataSynchronizer?.SynchronizeUpdate(table, dbRows);
            
            return this.ResponseOk();

        }

        private static void CleanPartitionAndBulkInsert(DbTable table, IEnumerable<ArraySpan<byte>> entitiesToInsert, string partitionKey, 
            [FromQuery]string syncPeriod)
        {
            
            var partitionsToSynchronize = table.CleanAndBulkInsert(partitionKey, entitiesToInsert);

            foreach (var dbPartition in partitionsToSynchronize)
            {
                ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());
                ServiceLocator.DataSynchronizer?.PublishInitPartition(table, dbPartition); 
            }
        }
        
        
        private static void CleanTableAndBulkInsert(DbTable table, IEnumerable<ArraySpan<byte>> entitiesToInsert, 
            [FromQuery]string syncPeriod)
        {
            table.CleanAndBulkInsert(entitiesToInsert);
            
            ServiceLocator.SnapshotSaverScheduler.SynchronizeTable(table, syncPeriod.ParseSynchronizationPeriod());
            ServiceLocator.DataSynchronizer?.PublishInitTable(table);
        }


        [HttpPost]
        public IActionResult CleanAndBulkInsert([Required] [FromQuery] string tableName,
            [FromQuery] string partitionKey, [Required] [FromBody] MyNoSqlDbEntity[] body,
            [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.CreateTableIfNotExists(tableName);
            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            if (string.IsNullOrEmpty(partitionKey))
                CleanTableAndBulkInsert(table, entitiesToInsert, syncPeriod);
            else
                CleanPartitionAndBulkInsert(table, entitiesToInsert, partitionKey, syncPeriod);

            return this.ResponseOk();
        }

    }

}