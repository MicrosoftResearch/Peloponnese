/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

*/

using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.Shared;

namespace Microsoft.Research.Peloponnese.Azure
{
    public class Utils
    {
        public static readonly string BlobScheme = "azureblob";
        public static readonly string BlobHostSuffix = ".blob.core.windows.net";

        public static void FromAzureUri(Uri uri, out string account, out string key, out string container, out string blob)
        {
            NameValueCollection queryCollection;
            FromAzureUri(uri, out account, out key, out container, out blob, out queryCollection);
        }

        public static void FromAzureUri(Uri uri, out string account, out string key, out string container, out string blob,
                                        out NameValueCollection queryCollection)
        {
            if (uri.Scheme != BlobScheme)
            {
                throw new ApplicationException("Malformed Azure Uri " + uri.AbsoluteUri + " bad scheme");
            }

            if (!uri.Host.EndsWith(BlobHostSuffix))
            {
                throw new ApplicationException("Malformed Azure Uri " + uri.AbsoluteUri + " bad hostname");
            }

            account = uri.Host.Substring(0, uri.Host.Length - BlobHostSuffix.Length);

            string fullPath = uri.AbsolutePath.TrimStart('/');
            int firstSep = fullPath.IndexOf('/');
            if (firstSep < 0)
            {
                throw new ApplicationException("Malformed Azure Uri " + uri.AbsoluteUri);
            }

            container = fullPath.Substring(0, firstSep);
            blob = fullPath.Substring(firstSep+1);

            queryCollection = System.Web.HttpUtility.ParseQueryString(uri.Query);
            key = queryCollection.Get("key");
        }

        public static Uri ToAzureUri(string account, string container, string blob)
        {
            return ToAzureUri(account, container, blob, null, null as NameValueCollection);
        }

        public static Uri ToAzureUri(string account, string container, string blob, string query)
        {
            return ToAzureUri(account, container, blob, query, null as string);
        }

        public static Uri ToAzureUri(string account, string container, string blob, string query, string key)
        {
            if (query == null)
            {
                return ToAzureUri(account, container, blob, key, null as NameValueCollection);
            }
            else
            {
                NameValueCollection queryCollection = System.Web.HttpUtility.ParseQueryString(query);
                return ToAzureUri(account, container, blob, key, queryCollection);
            }
        }

        public static Uri ToAzureUri(string account, string container, string blob, string key, NameValueCollection queryCollection)
        {
            UriBuilder uri = new UriBuilder();

            uri.Scheme = BlobScheme;
            uri.Host = account + BlobHostSuffix;

            uri.Path = container + "/" + blob;

            if (queryCollection == null)
            {
                queryCollection = System.Web.HttpUtility.ParseQueryString("");
            }

            if (key != null)
            {
                queryCollection["key"] = key;
            }

            uri.Query = queryCollection.ToString();

            return uri.Uri;
        }

        public static async Task WrapInRetry(ILogger logger, Func<Task> a)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    await a.Invoke();
                    return;
                }
                catch (StorageException e)
                {
                    ++retryCount;
                    if (retryCount > AzureConsts.NumberOfWrappedRetries ||
                        !(e.RequestInformation != null &&
                          e.RequestInformation.Exception != null &&
                          e.RequestInformation.Exception.InnerException != null &&
                          e.RequestInformation.Exception.InnerException is System.TimeoutException))
                    {
                        throw e;
                    }
                }

                // the request timed out; sleep then try again
                Random random = new Random();
                int delayMs = 30000 + random.Next(30000);
                if (logger != null)
                {
                    logger.Log("Got timeout; sleeping " + delayMs + " ms");
                }
                await Task.Delay(delayMs);
                // go around the while loop again
                if (logger != null)
                {
                    logger.Log("Retrying count " + retryCount);
                }
            }
        }

        public static async Task<T> WrapInRetry<T>(ILogger logger, Func<Task<T>> f)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    return await f.Invoke();
                }
                catch (StorageException e)
                {
                    ++retryCount;
                    if (retryCount > AzureConsts.NumberOfWrappedRetries ||
                        !(e.RequestInformation != null &&
                          e.RequestInformation.Exception != null &&
                          e.RequestInformation.Exception.InnerException != null &&
                          e.RequestInformation.Exception.InnerException is System.TimeoutException))
                    {
                        throw e;
                    }
                }

                // the request timed out; sleep then try again
                Random random = new Random();
                int delayMs = 30000 + random.Next(30000);
                if (logger != null)
                {
                    logger.Log("Got timeout; sleeping " + delayMs + " ms");
                }
                await Task.Delay(delayMs);
                // go around the while loop again
                if (logger != null)
                {
                    logger.Log("Retrying count " + retryCount);
                }
            }
        }

        public static string GetConnectionString(string accountName, string accountKey)
        {
            return "DefaultEndpointsProtocol=http;AccountName=" + accountName + ";AccountKey=" + accountKey;
        }

        public static CloudBlobContainer InitContainer(Uri uri, bool isPublic, out string blobName,
                                                       int threadCount = -1, long uploadThreshold = -1, ILogger logger = null)
        {
            string accountName, accountKey, containerName;
            Utils.FromAzureUri(uri, out accountName, out accountKey, out containerName, out blobName);
            return InitContainer(accountName, accountKey, containerName, isPublic, threadCount, uploadThreshold, logger);
        }

        public static CloudBlobContainer InitContainer(string accountName, string accountKey, string containerName, bool isPublic,
                                                       int threadCount = -1, long uploadThreshold = -1, ILogger logger = null)
        {
            if (logger != null)
            {
                logger.Log("Initializing container " + accountName + " " + containerName + " account key " + ((accountKey == null) ? "null" : "non-null"));
            }

            if (!AzureConsts.ValidContainerName(containerName))
                throw new BlobException(BlobException.Status.InvalidContainerName);

            // Create the blob client.
            CloudBlobClient blobClient;
            if (accountKey == null)
            {
                Uri baseUri = new Uri("http://" + accountName + Utils.BlobHostSuffix);
                blobClient = new CloudBlobClient(baseUri);
            }
            else
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(GetConnectionString(accountName, accountKey));
                blobClient = storageAccount.CreateCloudBlobClient();
            }
            
            blobClient.DefaultRequestOptions.MaximumExecutionTime = AzureConsts.DefaultMaximumExecutionTime;

            if (threadCount > 0)
            {
                blobClient.DefaultRequestOptions.ParallelOperationThreadCount = threadCount;
            }
            if (uploadThreshold > 0)
            {
                blobClient.DefaultRequestOptions.SingleBlobUploadThresholdInBytes = uploadThreshold;
            }

            // Retrieve a reference to a container. 
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Create the container if it doesn't already exist.
            bool created = container.CreateIfNotExists();
            if (created)
            {
                if (logger != null)
                {
                    logger.Log("Created container");
                }
                // Set permissions on the container.
                BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
                containerPermissions.PublicAccess = (isPublic) ? BlobContainerPublicAccessType.Blob : BlobContainerPublicAccessType.Off;
                container.SetPermissions(containerPermissions);
                if (logger != null)
                {
                    logger.Log("Set container permissions");
                }
            }

            return container;
        }

        public static void KillJob(Uri jobDirectory)
        {
            string account, key, container, blob;
            FromAzureUri(jobDirectory, out account, out key, out container, out blob);
            KillJob(account, key, container, blob);
        }

        public static void KillJob(string account, string key, string containerName, string jobDirectory)
        {
            CloudBlobContainer container = InitContainer(account, key, containerName, false);
            string blobName = jobDirectory.TrimEnd('/') + "/kill";
            CloudPageBlob killBlob = container.GetPageBlobReference(blobName);
            if (!killBlob.Exists())
            {
                killBlob.Create(512);
            }
        }
    }

    public class AzureDfsClient : DfsClient
    {
        private CloudBlobContainer _container = null;
        private List<string> _blockIdList = new List<string>();

        private string _accountName;
        private string _accountKey;
        private string _containerName;

        public string AccountName
        {
            get { return _accountName; }
            set { _accountName = value; }
        }
        public string AccountKey
        {
            get { return _accountKey; }
            set { _accountKey = value; }
        }
        public CloudBlobContainer Container { get { return _container; } }
        public string ContainerName { get { return _container.Name; } }

        private long BeginTime = new DateTime(1970, 1, 1).Ticks / TimeSpan.TicksPerMillisecond;
        private int MaxSingleBlobUploadThresholdInBytes64MB = 64 * 1024 * 1024;
        private int ThreadCount = 2;

        private string UriBlob(Uri uri)
        {
            string account, key, container, blob;
            Utils.FromAzureUri(uri, out account, out key, out container, out blob);
            if (account != _accountName || key != _accountKey || container != _containerName)
            {
                throw new ApplicationException("Azure client used with wrong container " + uri.AbsoluteUri);
            }

            return blob;
        }

        public AzureDfsClient(Uri uri, bool isPublic = false, ILogger logger = null)
        {
            string blobName;
            Utils.FromAzureUri(uri, out _accountName, out _accountKey, out _containerName, out blobName);

            try
            {
                _container = Utils.InitContainer(uri, isPublic, out blobName, ThreadCount, MaxSingleBlobUploadThresholdInBytes64MB, logger);
            }
            catch (Exception e)
            {
                throw new BlobException(BlobException.Status.BadRequest, e.Message, e);
            }
        }

        public AzureDfsClient(string accountName, string accountKey, string containerName, bool isPublic = false, ILogger logger = null)
        {
            _accountName = accountName.Split('.').First();
            _accountKey = accountKey;
            _containerName = containerName;

            try
            {
                _container = Utils.InitContainer(_accountName, _accountKey, _containerName, isPublic, ThreadCount, MaxSingleBlobUploadThresholdInBytes64MB, logger);
            }
            catch (Exception e)
            {
                throw new BlobException(BlobException.Status.BadRequest, e.Message, e);
            }
        }

        public override void Dispose()
        {
        }

        public void SetParallelThreadCount(int threadCount)
        {
            Container.ServiceClient.DefaultRequestOptions.ParallelOperationThreadCount = threadCount;
        }

        public override bool IsThreadSafe { get { return false; } }

        public override bool IsFileExists(Uri dfsPath)
        {
            string blob = UriBlob(dfsPath);
            CloudBlockBlob remoteFile = _container.GetBlockBlobReference(blob);
            return remoteFile.Exists();
        }

        private string IntIdToStringID(int id)
        {
            return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(id.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        }

        public override void PutDfsFileResolveConflict(Uri dfsPath, string localPath)
        {
            PutDfsFile(dfsPath, localPath);
        }

        public override void PutDfsFileResolveConflict(Uri dfsPath, byte[] bytes)
        {
            PutDfsFile(dfsPath, bytes);
        }

        public override Stream GetDfsStreamReader(Uri dfsPath, long offset, long length, int byteBufferSize)
        {
            return GetDfsStreamReaderAsync(dfsPath).Result;
        }

        public async Task<Stream> GetDfsStreamReaderAsync(Uri dfsPath, TimeSpan? retryTimeout = null, ILogger logger = null)
        {
            CloudBlockBlob blockBlob = _container.GetBlockBlobReference(UriBlob(dfsPath));

            if (retryTimeout.HasValue)
            {
                Stream stream = await Utils.WrapInRetry(logger, async () =>
                    {
                        BlobRequestOptions options = new BlobRequestOptions();
                        options.MaximumExecutionTime = retryTimeout.Value;
                        return await blockBlob.OpenReadAsync(null, options, null);
                    });
                return stream;
            }
            else
            {
                return await blockBlob.OpenReadAsync();
            }
        }

        public override Stream GetDfsStreamWriter(Uri dfsPath)
        {
            return GetDfsStreamWriterAsync(dfsPath).Result;
        }

        public async Task<Stream> GetDfsStreamWriterAsync(Uri dfsPath, TimeSpan? retryTimeout = null, ILogger logger = null)
        {
            CloudBlockBlob blockBlob = _container.GetBlockBlobReference(UriBlob(dfsPath));

            if (retryTimeout.HasValue)
            {
                return await Utils.WrapInRetry(logger, async () =>
                {
                    BlobRequestOptions options = new BlobRequestOptions();
                    options.MaximumExecutionTime = retryTimeout.Value;
                    return await blockBlob.OpenWriteAsync(null, options, null);
                });
            }
            else
            {
                return await blockBlob.OpenWriteAsync();
            }
        }

        public override bool GetFileStatus(Uri dfsPath, out long modificationTime, out long size)
        {
            modificationTime = -1;
            size = -1;

            try
            {
                CloudBlockBlob blockBlob = _container.GetBlockBlobReference(UriBlob(dfsPath));
                blockBlob.FetchAttributes();
                DateTimeOffset? dt = blockBlob.Properties.LastModified;
                if (dt.HasValue)
                {
                    modificationTime = ((((DateTimeOffset)dt).UtcDateTime).Ticks / TimeSpan.TicksPerMillisecond) - BeginTime;
                }
                size = blockBlob.Properties.Length;
            }
            catch (StorageException)
            {
                return false;
            }
            catch (Exception e)
            {
                throw new ApplicationException("BlockAzureClient::GetFileStatus failed", e);
            }

            return (size != -1);
        }

        public override void GetDirectoryContentSummary(Uri dfsPath, bool expandBlocks, ref long totalSize, ref int numberOfParts)
        {
            totalSize = -1;
            numberOfParts = -1;

            try
            {
                CloudBlockBlob blockBlob = _container.GetBlockBlobReference(UriBlob(dfsPath));
                blockBlob.FetchAttributes();
                totalSize = blockBlob.Properties.Length;

                // TODO: can we get the length of the list without downloading it?
                ICollection<ListBlockItem> blockList = (ICollection<ListBlockItem>)blockBlob.DownloadBlockList(BlockListingFilter.All);
                numberOfParts = Math.Max(blockList.Count, 1);
            }
            catch (Exception e)
            {
                throw new ApplicationException("BlockAzureClient::GetContentSummary failed", e);
            }

            return;
        }

        public override Uri GetClusterInternalUri(Uri externalUri)
        {
            UriBuilder builder = new UriBuilder();

            builder.Scheme = "wasb";

            builder.UserName = _containerName;
            builder.Host = _accountName + ".blob.core.windows.net";
            builder.Path = UriBlob(externalUri);

            return builder.Uri;
        }

        public override Uri Combine(Uri baseUri, params string[] components)
        {
            StringBuilder builder = new StringBuilder(baseUri.AbsolutePath.TrimEnd('/'));

            for (int i = 0; i < components.Length; ++i)
            {
                builder.Append("/");
                builder.Append(components[i].Trim('/'));
            }

            UriBuilder uriBuilder = new UriBuilder(baseUri);
            uriBuilder.Path = builder.ToString();
            return uriBuilder.Uri;
        }

        public override void EnsureDirectory(Uri dfsDir, bool worldWritable)
        {
            // directories are fictional
            return;
        }

        private string BlobName(IListBlobItem blob)
        {
            if (blob is CloudPageBlob)
            {
                return (blob as CloudPageBlob).Name;
            }
            else if (blob is CloudBlockBlob)
            {
                return (blob as CloudBlockBlob).Name;
            }
            else if (blob is CloudBlobDirectory)
            {
                return (blob as CloudBlobDirectory).Prefix.TrimEnd('/');
            }
            else
            {
                return null;
            }
        }

        public override IEnumerable<Uri> ExpandFileOrDirectory(Uri dfsPath)
        {
            CloudBlobDirectory directory = _container.GetDirectoryReference(UriBlob(dfsPath));
            return directory.ListBlobs()
                            .Select(b => BlobName(b))
                            .Where(n => n != null)
                            .Select(n => Utils.ToAzureUri(this._accountName, this._containerName, n, null, this._accountKey));
        }

        public override void DeleteDfsFile(Uri dfsPath, bool recursive = false)
        {
            throw new NotImplementedException();
        }
    }
}
