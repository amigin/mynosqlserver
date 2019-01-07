using System;
using System.IO;
using MyNoSqlServer.Domains;

namespace MyNoSqlServer.Api
{
    public class SettingsModel
    {
        public string BackupAzureConnectString { get; set; }
        
    }


    public static class SettingsLoader
    {
        public static SettingsModel LoadSettings()
        {
            
            var homeFolder = Environment.GetEnvironmentVariable("HOME");

            var fileName = homeFolder.AddLastSymbolIfOneNotExists('/')+".mynosqlserver";

            var json = File.ReadAllText(fileName);

            var result = Newtonsoft.Json.JsonConvert.DeserializeObject<SettingsModel>(json);
            
            if (string.IsNullOrEmpty(result.BackupAzureConnectString))
                throw new Exception("{ \"BackupAzureConnectString\":null } but it should not be null ");

            return result;            
        }
        
    }
}