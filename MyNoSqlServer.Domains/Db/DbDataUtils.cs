using System;

namespace MyNoSqlServer.Domains.Db
{
    public static class DbRowDataUtils
    {

        public const string PartitionKeyField = "PartitionKey";
        public const string RowKeyField = "RowKey";
        
        public static string ToTimeStampString(this DateTime timeStamp)
        {
            return timeStamp.ToString("O");
        }
    }
}