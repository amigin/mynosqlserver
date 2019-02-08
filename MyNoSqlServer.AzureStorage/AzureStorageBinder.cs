using System;
using System.Collections.Generic;
using Common;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        
        private const char TableNamePartitionSplitter = (char) 1;
        
        private static string GenerateBlobName(string tableName, string partitionKey)
        {
            return (tableName +TableNamePartitionSplitter + partitionKey).ToBase64();
        }

        public static (string tableName, string partitionKey) DecodeKey(string base64Key)
        {
            var pair = base64Key.Base64ToString().Split(TableNamePartitionSplitter);
            return (pair[0], pair[1]);
        }

        public static void BindAzureStorage(this string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return;


            var storageBlob = new AzureStorageBlob(connectionString);

            const string containerName = "nosqlsnapshots";
 

            SnapshotSaverEngine.Instance = new SnapshotSaverEngine(

                partitionSnapshot =>
                {
                    Console.WriteLine($"Save snapshot {partitionSnapshot.TableName}/{partitionSnapshot.PartitionKey}. Data Length: {partitionSnapshot.Snapshot.Length}");
                    var blobName = GenerateBlobName(partitionSnapshot.TableName, partitionSnapshot.PartitionKey);
                    return storageBlob.SaveToBlobAsync(containerName, blobName, partitionSnapshot.Snapshot);
                },

                async () =>
                {
                    var files = await storageBlob.GetFilesAsync(containerName);
                    var result = new List<PartitionSnapshot>();

                    foreach (var file in files)
                    {
                        var data = await storageBlob.LoadBlobAsync(containerName, file);
                        var (tableName, partitionKey) = DecodeKey(file);
                        Console.WriteLine($"Loaded snapshot : {tableName}/{partitionKey}");

                        var snapshot = PartitionSnapshot.Create(tableName, partitionKey, data);
                        result.Add(snapshot);
                    }

                    return result;
                }

            );

            SnapshotSaverEngine.Instance.Start();
        }

    }
}