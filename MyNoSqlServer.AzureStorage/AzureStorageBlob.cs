using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.AzureStorage
{
    
    public class AzureStorageBlob
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly TimeSpan _maxExecutionTime = TimeSpan.FromSeconds(10);


        private BlobRequestOptions _requestOptions;

        public AzureStorageBlob(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);

            _requestOptions =
                new BlobRequestOptions
                {
                    MaximumExecutionTime = _maxExecutionTime
                };
        }

        private CloudBlobContainer GetContainerReference(string container)
        {
            NameValidator.ValidateContainerName(container);

            var blobClient = _storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(container.ToLower());
        }
        
        
        
        private async ValueTask<CloudBlobContainer> GetBlockBlobReferenceAsync(string container)
        {
            var containerRef = GetContainerReference(container);
            
            if (!await containerRef.ExistsAsync())
                return null;
            
            var permissions = await containerRef.GetPermissionsAsync(null, _requestOptions, null);
            permissions.PublicAccess = BlobContainerPublicAccessType.Container;
            await containerRef.SetPermissionsAsync(permissions, null, _requestOptions, null);
            
            return containerRef;
        } 
        
        public async ValueTask SaveToBlobAsync(string containerName, string blobName, byte[] bytes)
        {
            var container = await GetBlockBlobReferenceAsync(containerName);

            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped saving blob: {containerName}/{blobName}");
                return;
            }

            var blob = container.GetBlockBlobReference(blobName.ToBase64());
            blob.Properties.ContentType = "application/json";

            await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
            Console.WriteLine($"{DateTime.UtcNow:s} Saved saving blob: {containerName}/{blobName}");
        }

   
        private async Task<IReadOnlyList<CloudBlobContainer>> GetListOfContainersAsync()
        {
            var cloudBlobClient = _storageAccount.CreateCloudBlobClient();
            
            
            BlobContinuationToken continuationToken = null;
            var containers = new List<CloudBlobContainer>();

            do
            {
                ContainerResultSegment response = await cloudBlobClient.ListContainersSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                containers.AddRange(response.Results);

            } while (continuationToken != null);

            return containers;
        }

        private static async Task<IReadOnlyList<CloudBlob>> GetListOfBlobs(CloudBlobContainer cloudBlobContainer)
        {

            var dir = cloudBlobContainer.GetDirectoryReference("");

            BlobContinuationToken continuationToken = null;
            var results = new List<CloudBlockBlob>();
            
            do
            {
                var response = await dir.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.Cast<CloudBlockBlob>());
            } while (continuationToken != null);

            return results;
        }


        private const string IgnoreContainerName = "nosqlsnapshots";
        public async Task<IReadOnlyList<PartitionSnapshot>> LoadBlobsAsync()
        {

            var result = new List<PartitionSnapshot>();
            var containers = await GetListOfContainersAsync();

            foreach (var container in containers.Where(c => c.Name != IgnoreContainerName))
            {

                foreach (var blockBlob in await GetListOfBlobs(container))
                {
                    var memoryStream = new MemoryStream();

                    await blockBlob.DownloadToStreamAsync(memoryStream);

                    var snapshot = new PartitionSnapshot
                    {
                        TableName = container.Name,
                        PartitionKey = blockBlob.Name.Base64ToString(),
                        Snapshot =  memoryStream.ToArray()
                    };
                    
                    result.Add(snapshot);
                    
                    Console.WriteLine("Loaded snapshot: "+snapshot);
                }
           
            }

            return result;
        }        
        
        
    }
    
    
}