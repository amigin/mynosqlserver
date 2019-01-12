using MyNoSqlServer.Domains.DataSynchronization;

namespace MyNoSqlServer.Domains
{
    public static class ServiceLocator
    {

        public static class Synchronizer
        {
            public static IDbRowSynchronizer DbRowSynchronizer { get; set; }
        
        
            public static ISynchronizationRules SynchronizationRules { get; set; }
            
        }
        
        
    }
}