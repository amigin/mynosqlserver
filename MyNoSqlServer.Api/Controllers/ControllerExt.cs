using System.Collections.Generic;
using System.IO;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Controllers
{
    public static class ControllerExt
    {

        private const string AppJsonContentType = "application/json";


        public static IActionResult ToDbRowResult(this Controller ctx, DbRow dbRow)
        {
            return ctx.File(dbRow.Data, AppJsonContentType);
        }
        
        public static IActionResult ToDbRowsResult(this Controller ctx, IEnumerable<DbRow> dbRows)
        {
            var response = dbRows.ToJsonArray().AsArray();
            return ctx.File(response, AppJsonContentType);
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