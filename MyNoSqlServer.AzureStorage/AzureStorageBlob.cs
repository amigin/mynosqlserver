using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MyNoSqlServer.AzureStorage
{
    
    public class AzureStorageBlob
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly TimeSpan _maxExecutionTime = TimeSpan.FromSeconds(10);
        
        public AzureStorageBlob(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }
        
        private CloudBlobContainer GetContainerReference(string container)
        {
            NameValidator.ValidateContainerName(container);

            var blobClient = _storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(container.ToLower());
        }
        
        private BlobRequestOptions GetRequestOptions()
        {
            return new BlobRequestOptions
            {
                MaximumExecutionTime = _maxExecutionTime
            };
        }
        
        private async Task<CloudBlockBlob> GetBlockBlobReferenceAsync(string container, string key, bool anonymousAccess = false, bool createIfNotExists = false)
        {
            NameValidator.ValidateBlobName(key);

            var containerRef = GetContainerReference(container);

            if (createIfNotExists)
            {
                await containerRef.CreateIfNotExistsAsync(GetRequestOptions(), null);
            }
            if (anonymousAccess)
            {
                var permissions = await containerRef.GetPermissionsAsync(null, GetRequestOptions(), null);
                permissions.PublicAccess = BlobContainerPublicAccessType.Container;
                await containerRef.SetPermissionsAsync(permissions, null, GetRequestOptions(), null);
            }
            
            return containerRef.GetBlockBlobReference(key);
        }        

        public async Task SaveToBlobAsync(string containerName, string blobName, byte[] data)
        {
            var blockBlob = await GetBlockBlobReferenceAsync(containerName, blobName, false, true);

            blockBlob.Properties.ContentType = "application/json";

            var memoryStream = new MemoryStream(data);

            await blockBlob.UploadFromStreamAsync(memoryStream, null, GetRequestOptions(), null);
        }
        
        public async Task<byte[]> LoadBlobAsync(string containerName, string blobName)
        {
            var blockBlob = await GetBlockBlobReferenceAsync(containerName, blobName, false, true);

            blockBlob.Properties.ContentType = "application/json";
            
            var memoryStream = new MemoryStream();

            await blockBlob.DownloadToStreamAsync(memoryStream);

            return memoryStream.ToArray();
        }        


        public async Task<IEnumerable<string>> GetFilesAsync(string containerName)
        {
            var blobRef = GetContainerReference(containerName);

            var dir = blobRef.GetDirectoryReference("");

            BlobContinuationToken continuationToken = null;
            var results = new List<CloudBlockBlob>();
            do
            {
                var response = await dir.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.Cast<CloudBlockBlob>());
            } while (continuationToken != null);

            return results.Select(x => x.Name).ToList();
        }
        
    }
    
    
}