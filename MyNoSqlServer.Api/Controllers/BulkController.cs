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
                ServiceLocator.SnapshotSaverEngine.Synchronize(tableName, dbPartition);

            ServiceLocator.Synchronizer.DbRowSynchronizer?.Synchronize(tableName, dbRows);
            
            return this.ResponseOk();

        }
        
    }
}