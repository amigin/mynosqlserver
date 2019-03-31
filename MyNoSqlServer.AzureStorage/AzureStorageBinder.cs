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
            SnapshotSaverEngine.Instance = new SnapshotSaverEngine(storageBlob);
            ServiceLocator.SnapshotSaverEngine = SnapshotSaverEngine.Instance;
            SnapshotSaverEngine.Instance.Start();
        }
    }
    
}