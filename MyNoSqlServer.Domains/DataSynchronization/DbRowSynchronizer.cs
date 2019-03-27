using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Flurl.Http;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    
    public class DbRowSynchronizer : IDbRowSynchronizer
    {
        
        private readonly Dictionary<string, BufferBlock<IReadOnlyList<DbRow>>>  _queue = new Dictionary<string, BufferBlock<IReadOnlyList<DbRow>>>();

        public void Synchronize(string tableName, IReadOnlyList<DbRow> dbRows)
        {
            var rules = ServiceLocator.Synchronizer.SynchronizationRules.GetRules(tableName);
            
            if (rules.Length == 0)
                return;

            lock (_queue)
            {
                foreach (var rule in rules)
                {
                    var ruleUrl = rule.Url.ToLower();

                    if (!_queue.ContainsKey(ruleUrl))
                    {
                        var bufferBlock = new BufferBlock<IReadOnlyList<DbRow>>();
                        _queue.Add(ruleUrl, bufferBlock);
                        Task.Run(()=>BackgroundReader(ruleUrl, tableName, bufferBlock));
                    }
                    
                    _queue[ruleUrl].Post(dbRows);
                }
                               
            }

        }


       


        public void Stop()
        {
            _working = false;
        }

        private bool _working = true;

        private async Task BackgroundReader(string url, string tableName, ISourceBlock<IReadOnlyList<DbRow>> queue)
        {
            

            while (_working)
            {
                try
                {
                    var dbRows = await queue.ReceiveAsync();


                    await SendDbRowsAsync(url, tableName, dbRows);
                    
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
            
        }
        
        private static async Task SendDbRowsAsync(string url, string tableName, IReadOnlyList<DbRow> dbRows)
        {
            while (true)
                try
                {

                    var toSend = DbRowSynchronizationModel.CreateSynchronizationModel(tableName, dbRows);

                    var httpContent = new StreamContent(toSend);

                    httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    await url.PostAsync(httpContent).ReceiveString();

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
        }
            
    }
}