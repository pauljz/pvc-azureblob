using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using PvcCore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace PvcPlugins
{
    public class PvcAzureBlob : PvcPlugin
    {
        private readonly string containerName;
        private readonly CloudStorageAccount storageAccount;
        private readonly CloudBlobClient blobClient;

        private static string connectionString = null;
        /// <summary>
        /// Sets a default storage connection string. If omitted, attempts to use the Azure Cloud Configuration Manager.
        /// </summary>
        public static string ConnectionString
        {
            get
            {
                if (connectionString != null)
                    return connectionString;
                return CloudConfigurationManager.GetSetting("StorageConnectionString");
            }
            set { connectionString = value; }
        }

        /// <summary>
        /// Uploads Pvc streams to Azure Blob Storage
        /// </summary>
        /// <param name="containerName">The container name to stream files to. This container must exist already.</param>
        /// <param name="storageConnectionString">An Azure connection string. If ommitted, PvcAzureBlob will attempt to use PvcAzureBlob.StorageConnectionString if present, or the Azure Cloud Configuration Manager.</param>
        public PvcAzureBlob(string containerName = null, string connectionString = null)
        {
            this.containerName = containerName;

            if (connectionString == null)
            {
                connectionString = PvcAzureBlob.ConnectionString;
            }
            this.storageAccount = CloudStorageAccount.Parse(connectionString);
            this.blobClient = this.storageAccount.CreateCloudBlobClient();
        }

        public override IEnumerable<PvcStream> Execute(IEnumerable<PvcStream> inputStreams)
        {
            CloudBlobContainer container = this.blobClient.GetContainerReference(this.containerName);

            Parallel.ForEach<PvcStream>(inputStreams, (inputStream) =>
            {
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(inputStream.StreamName);
                Console.WriteLine(string.Format("Uploading {0}", inputStream.StreamName));
                blockBlob.UploadFromStream(inputStream);
            });

            return inputStreams;
        }
    }
}
