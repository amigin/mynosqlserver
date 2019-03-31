using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        public static void BindAzureStorage(this string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return;

            var storageBlob = new AzureStorageBlob(connectionString);

            SnapshotSaverEngine.Instance = new SnapshotSaverEngine(
                partitionSnapshot => storageBlob.SaveToBlobAsync(partitionSnapshot.TableName, partitionSnapshot.PartitionKey, partitionSnapshot.Snapshot),
                async () => await storageBlob.LoadBlobsAsync());

            ServiceLocator.SnapshotSaverEngine = SnapshotSaverEngine.Instance;
            SnapshotSaverEngine.Instance.Start();
        }
    }
    
}