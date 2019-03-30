using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Hubs
{
    
    public class ChangesConnection : IConnection
    {
        public IClientProxy Client { get; }
        public string Id { get; }

        private readonly Dictionary<string, string> _subscribes = new Dictionary<string, string>();

        public void Subscribe(string tableName)
        {
            lock (_subscribes)
            {
                if (!_subscribes.ContainsKey(tableName))
                    _subscribes.Add(tableName, tableName);
            }
        }
        
        public bool SubscribedToTable(string tableChangeSubscribed)
        {
            lock (_subscribes)
                return _subscribes.ContainsKey(tableChangeSubscribed);
        }
        public ChangesConnection(string id, IClientProxy clientProxy)
        {
            Client = clientProxy;
            Id = id;
        }
    }
    
    public class ChangesHub : Hub
    {
        private static readonly ConnectionsManager<ChangesConnection> Connections = new ConnectionsManager<ChangesConnection>();
        
        public override Task OnConnectedAsync()
        {
            Connections.Add(Context.ConnectionId, new ChangesConnection(Context.ConnectionId, Clients.Caller));
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception exception)
        {
            Connections.Delete(Context.ConnectionId);
            return base.OnDisconnectedAsync(exception);
        }

        private static byte[] PreparePacketToBroadcast(IReadOnlyList<DbRow> entities)
        {
            return entities.ToJsonArray().AsArray();
        }
        
        
        
        public static void BroadcastChange(string tableName, IReadOnlyList<DbRow> entities)
        {
            var clientsToSend = Connections.Get(itm => itm.SubscribedToTable(tableName)).Select(itm => itm.Client);

            byte[] packetToBroadcast = null;

            foreach (var clientProxy in clientsToSend)
            {
                if (packetToBroadcast == null)
                    packetToBroadcast = PreparePacketToBroadcast(entities);
                
                clientProxy.SendAsync(tableName, packetToBroadcast);
            }
            
        }

        public async Task Subscribe(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return;

            var table = DbInstance.GetTable(tableName);
            
            if (table == null)
                return;
            
            Connections.Update(Context.ConnectionId, itm => { itm.Subscribe(tableName); });

            var rows = table.GetAllRecords(null);

            var dataToSend = PreparePacketToBroadcast(rows);

            await Clients.Caller.SendCoreAsync(tableName, new object[]{dataToSend});
        }
        
    }
    
    
}