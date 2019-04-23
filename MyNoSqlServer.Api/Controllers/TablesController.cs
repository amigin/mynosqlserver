using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]    
    public class TablesController : Controller
    {
        [HttpGet("Tables/List")]
        public IActionResult List()
        {
            var list = DbInstance.GetTablesList();
            return Json(list);
        }
        
        [HttpPost("Tables/CreateIfNotExists")]
        public IActionResult CreateIfNotExists([Required][FromQuery]string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict("Please specify table name");
            
            DbInstance.CreateTableIfNotExists(tableName);
            return this.ResponseOk();
        }
        
        [HttpPost("Tables/Create")]
        public IActionResult Create([Required][FromQuery]string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict("Please specify table name");

            if (DbInstance.CreateTable(tableName))
              return this.ResponseOk();

            return this.ResponseConflict("Can not create table: " + tableName);
        }
        
        [HttpDelete("Tables/Clean")]
        public IActionResult Clean([Required][FromQuery]string tableName, [FromQuery]string syncPeriod)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict("Please specify table name");


            var table = DbInstance.GetTable(tableName);
            if (table == null)
                return this.TableNotFound(tableName);

            table.Clean();
            
            ServiceLocator.SnapshotSaverScheduler.SynchronizeTable(table, syncPeriod.ParseSynchronizationPeriod());
            ServiceLocator.DataSynchronizer.PublishInitTable(table);
            
            return Ok();
        }
    }
}