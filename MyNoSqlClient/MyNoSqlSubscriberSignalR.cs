using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Client;

namespace MyNoSqlClient
{
    public class MyNoSqlSubscriberSignalR : IMyNoSqlSubscriberConnection
    {

        private const string SystemAction = "system";

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


        private readonly string _url;
        private readonly TimeSpan _pingTimeOut;
        public MyNoSqlSubscriberSignalR(string url, TimeSpan pingTimeOut)
        {
            _url = url.Last() == '/' ? url + PathForSubscribes : url + "/" + PathForSubscribes;
            _pingTimeOut = pingTimeOut;
        }

        public MyNoSqlSubscriberSignalR(string url) :
            this(url, TimeSpan.FromSeconds(30))
        {
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

            if (tableName == SystemAction)
                throw new Exception("Table can not have name: " + SystemAction);

            _deserializers.Add(tableName, data =>
            {
                var json = Encoding.UTF8.GetString(data);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(json).Cast<object>();
            });

            _initCallbacks.Add(tableName, items => { initAction(items.Cast<T>().ToList()); });

            _initPartitionCallbacks.Add(tableName,
                (partitionKey, items) => { initPartitionAction(partitionKey, items.Cast<T>().ToList()); });

            _updateCallbacks.Add(tableName, items => { updateAction(items.Cast<T>().ToList()); });

            _deleteCallbacks.Add(tableName, deleteAction);

        }


        private async Task SubscribeAsync(HubConnection hubConnection)
        {

            hubConnection.On<string>(SystemAction, action =>
            {
                if (action == "heartbeat")
                    _lastIncomingDateTime = DateTime.UtcNow;
            });

            foreach (var tableName in _deserializers.Keys)
            {
                hubConnection.On<string, byte[]>(tableName, (action, data) =>
                {

                    _lastIncomingDateTime = DateTime.UtcNow;

                    switch (action)
                    {
                        case "i":
                            HandleInitEvent(tableName, data);
                            break;

                        case "u":
                            HandleUpdateEvent(tableName, data);
                            break;

                        case "d":
                            HandleDeleteEvent(tableName, data);
                            break;

                        default:
                            if (action.StartsWith("i:"))
                                HandleInitPartitionEvent(tableName, action.Substring(2, action.Length - 2), data);
                            break;
                    }

                });

                Console.WriteLine("Subscribed to MyNoSql Server table: " + tableName);
                await hubConnection.SendAsync("Subscribe", tableName);
            }

        }


        private async Task StartAsync(HubConnection hubConnection)
        {

            while (true)
            {
                try
                {

                    Console.WriteLine("Connecting to MyNoSql Server using SignalR: " + _url);
                    await hubConnection.StartAsync();
                    Console.WriteLine("Connected to MyNoSql Server using SignalR");
                    _lastIncomingDateTime = DateTime.UtcNow;
                    return;

                }
                catch (Exception e)
                {
                    Console.WriteLine($"MyNoSql SignalR connection error to {_url}. Error: " + e.Message);
                    await Task.Delay(1000);
                }
            }

        }


        private async Task PingProcessAsync(HubConnection hubConnection)
        {
            while (DateTime.UtcNow - _lastIncomingDateTime < _pingTimeOut && hubConnection.State == HubConnectionState.Connected)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
                await hubConnection.SendAsync("Ping");
            }
            Console.WriteLine($"Disconnected: Last incoming packet has been: {(DateTime.UtcNow - _lastIncomingDateTime).TotalSeconds} ms ago. Connection state: {HubConnectionState.Connected}");

        }

        private DateTime _lastIncomingDateTime;

        private async Task TheTask()
        {
            while (_started)
            {
                try
                {
                    var hubConnection = new HubConnectionBuilder()
                        .WithUrl(_url)
                        .Build();

                    await StartAsync(hubConnection);
                    await SubscribeAsync(hubConnection);
                    await PingProcessAsync(hubConnection);
                    await hubConnection.StopAsync();
                }
                catch (Exception e)
                {
                    Console.WriteLine("TheTask:" + e);
                }
            }
        }


        private Task _task;

        private bool _started;

        public void Start()
        {

            _started = true;
            _task = TheTask();
        }

        public void Stop()
        {
            _started = false;
            _task.Wait();
        }
    }
}