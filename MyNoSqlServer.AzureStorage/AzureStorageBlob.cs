using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Microsoft.WindowsAzure.Storage;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.AzureStorage
{
    
    public class AzureStorageBlob : ISnapshotInfrastructure
    {
        private readonly CloudStorageAccount _storageAccount;

        public AzureStorageBlob(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        public async Task SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(partitionSnapshot.TableName);

            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped synchronizing snapshot: {partitionSnapshot}");
                return;
            }

            await container.SavePartitionAsync(partitionSnapshot.PartitionKey, partitionSnapshot.Snapshot);
            
            Console.WriteLine($"{DateTime.UtcNow:s} Saved snapshot: {partitionSnapshot}");
        }

        public async Task SaveTableSnapshotAsync(DbTable dbTable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped synchronizing table: {dbTable.Name}");
                return;
            }

            await container.CleanContainerAsync();
            Console.WriteLine($"{DateTime.UtcNow:s} Container cleaned: {dbTable.Name}");

            var partitions = dbTable.GetAllPartitions();

            foreach (var dbPartition in partitions)
            {
                var data = dbPartition.GetAllRows().ToJsonArray().AsArray();
                await container.SavePartitionAsync(dbPartition.PartitionKey, data);
            
                Console.WriteLine($"{DateTime.UtcNow:s} Saved snapshot: {dbTable.Name}/{dbPartition.PartitionKey}");
            }
        }

        public async Task DeleteTablePartitionAsync(string tableName, string partitionKey)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(tableName);
            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped deleting snapshot: {tableName}/{partitionKey}");
                return;
            }

            await container.DeletePartitionAsync(partitionKey);
            
            Console.WriteLine($"{DateTime.UtcNow:s} Snapshot is deleted: {tableName}/{partitionKey}");
            
        }

        public async Task LoadSnapshotsAsync(Action<PartitionSnapshot> callback)
        {
        
            const string ignoreContainerName = "nosqlsnapshots";

            var containers = await _storageAccount.GetListOfContainersAsync();

            foreach (var container in containers.Where(c => c.Name != ignoreContainerName))
            {

                foreach (var blockBlob in await container.GetListOfBlobs())
                {
                    var memoryStream = new MemoryStream();

                    await blockBlob.DownloadToStreamAsync(memoryStream);

                    var snapshot = new PartitionSnapshot
                    {
                        TableName = container.Name,
                        PartitionKey = blockBlob.Name.Base64ToString(),
                        Snapshot =  memoryStream.ToArray()
                    };

                    callback(snapshot);
                    
                    
                    Console.WriteLine("Loaded snapshot: "+snapshot);
                }
           
            }

        }
    }
    
    
}