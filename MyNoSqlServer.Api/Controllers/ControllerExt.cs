using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.SnapshotSaver;

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

        public static async ValueTask<IReadOnlyList<byte>> BodyAsByteArrayAsync(this HttpRequest request)
        {
            
            var result = (await request.BodyReader.ReadAsync()).Buffer;

            if (result.IsSingleSegment)
                return result.FirstSpan.ToArray();


            var list = new List<byte>();
            var pos = result.Start;
            while (result.TryGet(ref pos, out var mem))
                list.AddRange(mem.ToArray());
            return list.ToArray();
        }

        public static IActionResult CheckOnShuttingDown(this Controller ctx)
        {
            if (ServiceLocator.ShuttingDown)
                return ctx.Conflict("Application is shutting down");
            
            return null;
        }
        
        
    

    }
}