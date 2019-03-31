using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Domains
{
    public static class ServiceLocator
    {

        
        public static ISnapshotSaverEngine SnapshotSaverEngine { get; set; }
        
        public static class Synchronizer
        {
            public static IChangesPublisher ChangesPublisher { get; set; }
            
        }
        
        
    }
}