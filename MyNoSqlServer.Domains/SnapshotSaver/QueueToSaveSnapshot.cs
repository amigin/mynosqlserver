using System.Collections.Generic;
using System.Linq;
using Common;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    public class QueueToSaveSnapshot
    {
        private readonly Dictionary<string, DbPartition> _saveQueue = new Dictionary<string, DbPartition>();

        private const char TableNamePartitionSplitter = (char) 1;
        private static string GenerateKey(string tableName, string partitionKey)
        {
            return (tableName +TableNamePartitionSplitter + partitionKey).ToBase64();
        }

        private static (string tableName, string partitionKey) DecodeKey(string base64Key)
        {
            var pair = base64Key.Base64ToString().Split(TableNamePartitionSplitter);
            return (pair[0], pair[1]);
        }
        public void Enqueue(string tableName, DbPartition partitionToSave)
        {
            var key = GenerateKey(tableName, partitionToSave.PartitionKey);
            lock (_saveQueue)
            {
                if (_saveQueue.ContainsKey(key))
                    _saveQueue[key] = partitionToSave;
                else
                    _saveQueue.Add(key, partitionToSave);
            }
        }
        
        
        public (string tableName, DbPartition dbPartition) Dequeue()
        {
            lock (_saveQueue)
            {
                if (_saveQueue.Count == 0)
                    return (null, null);

                var result = _saveQueue.First();

                _saveQueue.Remove(result.Key);

                var (table, _) = DecodeKey(result.Key);

                return (table, result.Value);
            }
        }
        
        
    }
}