using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Text;
using Common;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    public interface IDbRowSynchronizationModel
    {
        
        string TableName { get; }
        string PartitionKey { get; }
        string RowKey { get; }
        
        string Timestamp { get; }
        
        byte[] Data { get; }
        
    }

/*

    public class DbRowSynchronizationModel : IDbRowSynchronizationModel
    {
        public string TableName { get; set; }

        byte[] IDbRowSynchronizationModel.Data => Encoding.UTF8.GetBytes(Data);
        
        public string[] Data { get; set; }


        public static DbRowSynchronizationModel Create(string tableName, DbRow[] dbRow)
        {
            return new DbRowSynchronizationModel
            {
                TableName = tableName,
                PartitionKey = dbRow.PartitionKey,
                RowKey = dbRow.RowKey,
                Data = Encoding.UTF8.GetString(dbRow.Data),
                Timestamp = dbRow.Timestamp

            };
        }
    }
*/

    public static class DbRowSynchronizationModel
    {

        private static IEnumerable<byte> GenerateSynchronizationModel(string tableName, DbRow[] dbRows)
        {
            throw new NotImplementedException();
        }

        public static Stream CreateSynchronizationModel(string tableName, DbRow[] dbRows)
        {
            throw new NotImplementedException();
            /*
            var stream =  new ChunkedStream();

            var s = new MemoryStream();
            return GenerateSynchronizationModel(tableName, dbRows);
            */
        }
        
    }
    
    
}