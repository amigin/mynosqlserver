using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Domains
{
    public static class ServiceLocator
    {

        
        public static SnapshotSaverEngine SnapshotSaverEngine { get; set; }
        
        public static class Synchronizer
        {
            public static IDbRowSynchronizer DbRowSynchronizer { get; set; }
        
        
            public static ISynchronizationRules SynchronizationRules { get; set; }
            
        }
        
        
    }
}