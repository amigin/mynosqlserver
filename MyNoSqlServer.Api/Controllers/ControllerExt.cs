using System.Collections.Generic;
using System.IO;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Api.Controllers
{
    public static class ControllerExt
    {
        public static IActionResult ResponseOk(this Controller ctx)
        {
            return ctx.Content("OK");
        }

        public static IActionResult ResponseConflict(this Controller ctx, string message)
        {
            return ctx.Conflict(message);
        }
        
        public static IActionResult TableNameIsNull(this Controller ctx)
        {
            return ctx.Conflict("Please specify table name");
        }
        
        public static IActionResult TableNotFound(this Controller ctx, string tableName)
        {
            return ctx.NotFound($"Table {tableName} name");
        }

        public static IActionResult PartitionKeyIsNull(this Controller ctx)
        {
            return ctx.NotFound($"Please specify PartitionKey");
        }

        public static IActionResult RowKeyIsNull(this Controller ctx)
        {
            return ctx.NotFound($"Please specify RowKey");
        }
        
        public static IActionResult RowNotFound(this Controller ctx, string tableName, string partitionKey, string rowKey)
        {
            return ctx.NotFound($"Entity with PartitionKey={partitionKey} and RowKey={rowKey} at table={tableName} is not found");
        }

        
        private const string AppJsonContentType = "application/json";


        public static IActionResult ToDbRowResult(this Controller ctx, DbRow dbRow)
        {
            return ctx.File(dbRow.Data, AppJsonContentType);
        }
        
        public static IActionResult ToDbRowsResult(this Controller ctx, IEnumerable<DbRow> dbRows)
        {
            return ctx.File(dbRows.ToJsonArray().AsArray(), AppJsonContentType);
        }

        public static byte[] BodyAsByteArray(this HttpRequest request)
        {
            var memArray = new MemoryStream();
            request.Body.Position = 0;
            request.Body.CopyTo(memArray);
            return memArray.ToArray();
        }
        
        
        
    }
}