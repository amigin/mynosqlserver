using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;

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
        
          
        [HttpPost]
        public IActionResult CleanAndBulkInsert([Required][FromQuery] string tableName, [Required][FromBody] MyNoSqlDbEntity[] body)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();
            
            var table = DbInstance.CreateTableIfNotExists(tableName);

            var entitiesToInsert = Request.BodyAsByteArray().SplitJsonArrayToObjects().ToList();

            table.CleanAndBulkInsert(entitiesToInsert);

            ServiceLocator.SnapshotSaverEngine.SynchronizeTable(table);

            ServiceLocator.Synchronizer.ChangesPublisher?.PublishInitTable(table);
            
            return this.ResponseOk();

        }
        
    }

}