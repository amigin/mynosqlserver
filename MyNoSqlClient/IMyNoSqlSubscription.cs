using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Client;

namespace MyNoSqlClient
{

    public interface IMyNoSqlSubscriberConnection
    {
        void Subscribe<T>(string tableName, 
            Action<IReadOnlyList<T>> initAction, 
            Action<string, IReadOnlyList<T>> initPartitionAction, 
            Action<IReadOnlyList<T>> updateAction, 
            Action<IDictionary<string, string>> deleteActions);
    }


    public class MyNoSqlSubscriberSignalR : IMyNoSqlSubscriberConnection
    {
        
        private readonly Dictionary<string, Func<byte[], IEnumerable<object>>> _deserializers 
            = new Dictionary<string, Func<byte[], IEnumerable<object>>>();
        
        private readonly Dictionary<string, Action<IEnumerable<object>>> _initCallbacks 
            = new Dictionary<string, Action<IEnumerable<object>>>();
        
        private readonly Dictionary<string, Action<string, IEnumerable<object>>> _initPartitionCallbacks 
            = new Dictionary<string, Action<string, IEnumerable<object>>>();
        
        private readonly Dictionary<string, Action<IEnumerable<object>>> _updateCallbacks 
            = new Dictionary<string, Action<IEnumerable<object>>>();
        
        private readonly Dictionary<string, Action<IDictionary<string, string>>> _deleteCallbacks 
            = new Dictionary<string, Action<IDictionary<string, string>>>();
        


        private const string PathForSubscribes = "changes";


        private readonly HubConnection _hubConnection;
        private string _url;
        public MyNoSqlSubscriberSignalR(string url)
        {
            _url = url.Last() == '/' ? url+PathForSubscribes : url + "/"+PathForSubscribes;
            
            _hubConnection = new HubConnectionBuilder()
                .WithUrl(_url)
                .Build();
        }
        
        private void HandleInitEvent(string tableName, byte[] data)
        {
            var items = _deserializers[tableName](data);
            _initCallbacks[tableName](items);
        }
        
        private void HandleInitPartitionEvent(string tableName, string partitionKey, byte[] data)
        {
            var items = _deserializers[tableName](data);
            _initPartitionCallbacks[tableName](partitionKey, items);
        }

        private void HandleUpdateEvent(string tableName, byte[] data)
        {
            var items = _deserializers[tableName](data);
            _updateCallbacks[tableName](items);
            
        }
        
        private void HandleDeleteEvent(string tableName, byte[] data)
        {
            var json = Encoding.UTF8.GetString(data);
            var items = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            _deleteCallbacks[tableName](items);
        }
        
        public void Subscribe<T>(string tableName, 
            Action<IReadOnlyList<T>> initAction, 
            Action<string, IReadOnlyList<T>> initPartitionAction, 
            Action<IReadOnlyList<T>> updateAction, 
            Action<IDictionary<string, string>> deleteAction)
        {
            
            _deserializers.Add(tableName, data =>
            {
                var json = Encoding.UTF8.GetString(data);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(json).Cast<object>();
            });
            
            _initCallbacks.Add(tableName, items =>
            {
                initAction(items.Cast<T>().ToList());
            });
            
            _initPartitionCallbacks.Add(tableName, (partitionKey, items) =>
            {
                initPartitionAction(partitionKey, items.Cast<T>().ToList());
            });
            
            _updateCallbacks.Add(tableName, items =>
            {
                updateAction(items.Cast<T>().ToList());
            });
            
            _deleteCallbacks.Add(tableName, deleteAction);
            
            _hubConnection.On<string, byte[]>(tableName, (action,data)=>
            {
                
               // Console.WriteLine("SignalR update: "+tableName+" action:"+action);
                if (action == "i")
                    HandleInitEvent(tableName, data);
                    
                if (action == "u")
                    HandleUpdateEvent(tableName, data);

                if (action == "d")
                    HandleDeleteEvent(tableName, data);
                
                if (action.StartsWith("i:"))
                    HandleInitPartitionEvent(tableName, action.Substring(2, action.Length-2), data);
            });
        }


        private async Task StartAsync()
        {

            while (true)
            {
                try
                {
                    Console.WriteLine("Connecting to MyNoSql Server using SignalR: "+_url);
                    await _hubConnection.StartAsync();

                    foreach (var tableName in _deserializers.Keys)
                    {
                        Console.WriteLine("Subscribed to MyNoSql Server table: "+tableName);
                        await _hubConnection.SendAsync("Subscribe", tableName);
                    }
                    
                    
                    Console.WriteLine("Connected to MyNoSql Server using SignalR");
                    return;

                }
                catch (Exception e)
                {
                    Console.WriteLine($"MyNoSql SignalR connection error to {_url}. Error: "+e.Message);
                    await Task.Delay(1000);
                }
            }
            
        } 

        public void Start()
        {
            _hubConnection.Closed += async exception =>
            {
                await Task.Delay(500);
                await StartAsync();
            };
            
            Task.Run(StartAsync);
        }
    }
}