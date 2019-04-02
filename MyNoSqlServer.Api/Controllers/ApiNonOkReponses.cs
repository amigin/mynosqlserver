using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{

    public static class ApiNonOkResponses
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

        public static IActionResult QueryIsNull(this Controller ctx)
        {
            return ctx.Conflict("Please specify query as body json field");
        }


        public static IActionResult TableNotFound(this Controller ctx, string tableName)
        {
            return ctx.StatusCode(204,$"Table {tableName} name");
        }

        public static IActionResult PartitionKeyIsNull(this Controller ctx)
        {
            return ctx.StatusCode(204,"Please specify PartitionKey");
        }

        public static IActionResult RowKeyIsNull(this Controller ctx)
        {
            return ctx.StatusCode(204,"Please specify RowKey");
        }

        public static IActionResult RowNotFound(this Controller ctx, string tableName, string partitionKey, string rowKey)
        {
            return ctx.StatusCode(204, $"Entity with PartitionKey={partitionKey} and RowKey={rowKey} at table={tableName} is not found");
        }



    }

}
