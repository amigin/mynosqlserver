using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;

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
    }

    public interface ISnapshotSaverEngine
    {
        void Synchronize(string tableName, DbPartition partitionToSave);
    }
    
    public class SnapshotSaverEngine : ISnapshotSaverEngine
    {
        
        private readonly QueueToSaveSnapshot _queueToSaveSnapshot = new QueueToSaveSnapshot();

        public void Synchronize(string tableName, DbPartition partitionToSave)
        {
            _queueToSaveSnapshot.Enqueue(tableName, partitionToSave);
        }        
        
        private readonly Func<PartitionSnapshot, ValueTask> _saveSnapshot;
        private readonly Func<Task<IEnumerable<PartitionSnapshot>>> _loadSnapshots;

        public SnapshotSaverEngine(Func<PartitionSnapshot, ValueTask> saveSnapshot, Func<Task<IEnumerable<PartitionSnapshot>>> loadSnapshots)
        {
            _saveSnapshot = saveSnapshot;
            _loadSnapshots = loadSnapshots;
        }

        private async Task LoadSnapshotsAsync()
        {

            var snapshots = await _loadSnapshots();
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

        private async ValueTask SaveSnapshotAsync(string tableName, DbPartition dbPartition)
        {
            var dbRowsAsByteArray = dbPartition.GetAllRows().ToJsonArray().AsArray();
            
            var tableSnapshot = PartitionSnapshot.Create(tableName, dbPartition.PartitionKey, dbRowsAsByteArray);
            await _saveSnapshot(tableSnapshot);
        }

        public async void TheLoop()
        {

            await LoadSnapshotsAsync();
            
            while (true)
                try
                {
                    var elementToSave = _queueToSaveSnapshot.Dequeue();

                    while (elementToSave.tableName != null)
                    {
                        await SaveSnapshotAsync(elementToSave.tableName, elementToSave.dbPartition);
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