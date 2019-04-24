using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.SnapshotSaver;

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

            var theSyncPeriod = syncPeriod.ParseSynchronizationPeriod();

            if (theSyncPeriod == DataSynchronizationPeriod.Immediately)
                return Conflict("Bulk insert does not support immediate persistence");
            
            var table = DbInstance.CreateTableIfNotExists(tableName);

            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            var (dbPartitions, dbRows) = table.BulkInsertOrReplace(entitiesToInsert);

            ServiceLocator.DataSynchronizer.SynchronizeUpdate(table, dbRows);
            
            foreach (var dbPartition in dbPartitions)
                ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(table, dbPartition, theSyncPeriod);
            
            return this.ResponseOk();

        }

        private static void CleanPartitionAndBulkInsert(DbTable table, IEnumerable<ArraySpan<byte>> entitiesToInsert, string partitionKey, 
            DataSynchronizationPeriod syncPeriod)
        {
            var partitionsToSynchronize = table.CleanAndBulkInsert(partitionKey, entitiesToInsert);

            foreach (var dbPartition in partitionsToSynchronize)
            {
                ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(table, dbPartition, syncPeriod);
                ServiceLocator.DataSynchronizer?.PublishInitPartition(table, dbPartition); 
            }
        }
        
        
        private static void CleanTableAndBulkInsert(DbTable table, IEnumerable<ArraySpan<byte>> entitiesToInsert, 
            DataSynchronizationPeriod syncPeriod)
        {
            table.CleanAndBulkInsert(entitiesToInsert);
            
            ServiceLocator.SnapshotSaverScheduler.SynchronizeTable(table, syncPeriod);
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
            
            var theSyncPeriod = syncPeriod.ParseSynchronizationPeriod();

            if (theSyncPeriod == DataSynchronizationPeriod.Immediately)
                return Conflict("CleanAndBulkInsert insert does not support immediate persistence");


            var table = DbInstance.CreateTableIfNotExists(tableName);
            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            if (string.IsNullOrEmpty(partitionKey))
                CleanTableAndBulkInsert(table, entitiesToInsert, theSyncPeriod);
            else
                CleanPartitionAndBulkInsert(table, entitiesToInsert, partitionKey, theSyncPeriod);

            return this.ResponseOk();
        }

    }

}