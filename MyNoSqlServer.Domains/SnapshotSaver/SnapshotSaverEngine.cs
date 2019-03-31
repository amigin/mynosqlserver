using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{

    public class PartitionSnapshot
    {
        public string TableName { get; set; }
        public string PartitionKey { get; set; }
        public byte[] Snapshot { get; set; }

        public static PartitionSnapshot Create(string tableName, string partitionKey, byte[] snapshot)
        {
            return new PartitionSnapshot
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                Snapshot = snapshot
            };
        }


        public override string ToString()
        {
            return TableName + "/" + PartitionKey;
        }
    }

    public interface ISnapshotSaverEngine
    {
        void SynchronizePartition(DbTable dbTable, DbPartition partitionToSave);
        
        void SynchronizeTable(DbTable dbTable);
        
        void SynchronizeDeletePartition(string tableName, string partitionKey);
    }



    public interface ISnapshotInfrastructure
    {
        Task SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot);
        Task SaveTableSnapshotAsync(DbTable dbTable);

        Task DeleteTablePartitionAsync(string tableName, string partitionKey);
        
        Task<IEnumerable<PartitionSnapshot>> LoadSnapshotsAsync();
        
    }
    
    public class SnapshotSaverEngine : ISnapshotSaverEngine
    {
        private readonly ISnapshotInfrastructure _snapshotInfrastructure;

        private readonly QueueToSaveSnapshot _queueToSaveSnapshot = new QueueToSaveSnapshot();

        public void SynchronizePartition(DbTable dbTable, DbPartition partitionToSave)
        {
            _queueToSaveSnapshot.Enqueue(dbTable, partitionToSave);
        }

        public void SynchronizeTable(DbTable tableName)
        {
            _queueToSaveSnapshot.Enqueue(tableName);
        }

        public void SynchronizeDeletePartition(string tableName, string partitionKey)
        {
            _queueToSaveSnapshot.EnqueueDeletePartition(tableName, partitionKey);
        }


        public SnapshotSaverEngine(ISnapshotInfrastructure snapshotInfrastructure)
        {
            _snapshotInfrastructure = snapshotInfrastructure;
        }

        private async Task LoadSnapshotsAsync()
        {

            var snapshots = await _snapshotInfrastructure.LoadSnapshotsAsync();
            foreach (var snapshot in snapshots)
            {
                try
                {
                    var table = DbInstance.CreateTableIfNotExists(snapshot.TableName);
                    table.InitPartitionFromSnapshot(snapshot.PartitionKey, snapshot.Snapshot);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Snapshots {snapshot.TableName}/{snapshot.PartitionKey} could not be loaded: " + e.Message);
                }

            }

        }

        public async void TheLoop()
        {

            await LoadSnapshotsAsync();
            
            while (true)
                try
                {
                    var elementToSave = _queueToSaveSnapshot.Dequeue();

                    while (elementToSave != null)
                    {
                        switch (elementToSave)
                        {
                            
                            case SyncTable syncTable:
                                await _snapshotInfrastructure.SaveTableSnapshotAsync(syncTable.DbTable);
                                break;
                            
                            case SyncPartition syncPartition:
                                
                                var dbRowsAsByteArray = syncPartition.DbPartition.GetAllRows().ToJsonArray().AsArray();
                                var partitionSnapshot = PartitionSnapshot.Create(syncPartition.DbTable.Name, syncPartition.DbPartition.PartitionKey, dbRowsAsByteArray);
                                await _snapshotInfrastructure.SavePartitionSnapshotAsync(partitionSnapshot);
                                
                                break;
                            
                            case SyncDeletePartition syncDeletePartition:
                                await _snapshotInfrastructure.DeleteTablePartitionAsync(syncDeletePartition.TableName,
                                    syncDeletePartition.PartitionKey);
                                break;
                            
                        }

                        elementToSave = _queueToSaveSnapshot.Dequeue();
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine("There is something wrong during saving the snapshot. " + e.Message);
                }
                finally
                {
                    await Task.Delay(1000);
                }
        }

        public void Start()
        {
            TheLoop();
        }

        public static SnapshotSaverEngine Instance;

    }
    
}