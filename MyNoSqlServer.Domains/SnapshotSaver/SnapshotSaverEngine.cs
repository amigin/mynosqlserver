using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Domains.SnapshotSaver
{

    public class TableSnapshot
    {
        public string TableName { get; set; }
        public byte[] Snapshot { get; set; }

        public static TableSnapshot Create(string tableName, byte[] snapshot)
        {
            return new TableSnapshot
            {
                TableName = tableName,
                Snapshot = snapshot
            };
        }
    }
    
    public class SnapshotSaverEngine
    {
        private readonly Func<TableSnapshot, Task> _saveSnapshot;
        private readonly Func<Task<IEnumerable<TableSnapshot>>> _loadSnapshots;

        public SnapshotSaverEngine(Func<TableSnapshot, Task> saveSnapshot, Func<Task<IEnumerable<TableSnapshot>>> loadSnapshots)
        {
            _saveSnapshot = saveSnapshot;
            _loadSnapshots = loadSnapshots;
        }
        
        private readonly Dictionary<string, string> _lastSavedSnapshotsIds = new Dictionary<string, string>();

        private bool ShouldWeSaveTheSnapshot(DbTable dbTable)
        {
            if (dbTable.SnapshotId == null)
                return false;
            
            if (_lastSavedSnapshotsIds.ContainsKey(dbTable.Name))
                return _lastSavedSnapshotsIds[dbTable.Name] != dbTable.SnapshotId;

            return true;
        }

        private void SnapshotIsSaved(DbTable dbTable)
        {
            if (_lastSavedSnapshotsIds.ContainsKey(dbTable.Name))
                _lastSavedSnapshotsIds[dbTable.Name] = dbTable.SnapshotId;
            else
                _lastSavedSnapshotsIds.Add(dbTable.Name, dbTable.SnapshotId);
        }


        private async Task LoadSnapshotsAsync()
        {

            try
            {
                var snapshots = await _loadSnapshots();
                foreach (var snapshot in snapshots)
                {
                    using(var tableInit = DbInstance.InitNewTable(snapshot.TableName))
                    {
                        foreach (var dbRowMemory in snapshot.Snapshot.SplitByDbRows())
                        {
                            var array = dbRowMemory.ToArray();
                            var jsonString = Encoding.UTF8.GetString(array);
                            var entityInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<MyNoSqlDbEntity>(jsonString);                        
                            tableInit.InitDbRecord(entityInfo, array);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Snapshots could not be loaded: "+e.Message);
            }


        }

        public async void TheLoop()
        {

            await LoadSnapshotsAsync();
            
            while (true)
                try
                {
                    var tables = DbInstance.GetTables();

                    foreach (var table in tables)
                    {
                       
                        if (!ShouldWeSaveTheSnapshot(table)) continue;

                        var dbRowsAsByteArray = table.GetAllRecords(0).ToJsonArray().ToArray();
                        var tableSnapshot = TableSnapshot.Create(table.Name, dbRowsAsByteArray);
                        await _saveSnapshot(tableSnapshot);
                        SnapshotIsSaved(table);
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine("There is something wrong during saving the snapshot. " + e.Message);
                }
                finally
                {
                    await Task.Delay(10000);
                }
        }

        public void Start()
        {
            TheLoop();
        }

        public static SnapshotSaverEngine Instance;

    }
    
}