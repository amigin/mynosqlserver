using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {

        public static void BindAzureStorages(this string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return;


            var storageBlob = new AzureStorageBlob(connectionString);

            const string containerName = "nosqlsnapshots";

            SnapshotSaverEngine.Instance = new SnapshotSaverEngine(

                tableSnapshot =>
                {
                    Console.WriteLine($"Save snapshot for table {tableSnapshot.TableName}. Data Length: {tableSnapshot.Snapshot.Length}");
                    return storageBlob.SaveToBlobAsync(containerName, tableSnapshot.TableName, tableSnapshot.Snapshot);
                },

                async () =>
                {
                    var files = await storageBlob.GetFilesAsync(containerName);
                    var result = new List<TableSnapshot>();

                    foreach (var file in files)
                    {
                        var data = await storageBlob.LoadBlobAsync(containerName, file);
                        Console.WriteLine("Loaded snapshot for table: " + file);

                        var snapshot = TableSnapshot.Create(file, data);
                        result.Add(snapshot);
                    }

                    return result;
                }

            );

            SnapshotSaverEngine.Instance.Start();
        }

    }
}