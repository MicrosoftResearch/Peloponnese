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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.Azure
{
    public class BlobException : Exception
    {
        public enum Status
        {
            BadRequest,
            MaxBlockSizeExceeded,
            MaxBlocksExceeded,
            BlobAlreadyExists,
            BlobNotFound,
            InvalidBlobOrBlock,
            InvalidBlockId,
            InvalidBlockList,
            InvalidContainerName
        }
        public BlobException() : base() { }

        public BlobException(Status status)
            : base(status.ToString())
        {
        }

        public BlobException(Status status, string message)
            : base(NewMessage(status, message))
        {
        }

        public BlobException(Status status, string message, Exception inner)
            : base(NewMessage(status, message), inner)
        {
        }

        private static string NewMessage(Status status, string message)
        {
            return (status.ToString() + ": " + message);
        }
    }

    public static class AzureConsts
    {
        public static int Size1KB = 1024;
        public static int Size1MB = Size1KB * Size1KB;
        public static int Size256MB = 256 * Size1MB;
        public static int Size1GB = Size1MB * Size1KB;
        public static long Size1TB = (long)Size1GB * (long)Size1KB;
        public static int MaxBlockSize4MB = 4 * Size1MB;
        public static int MaxBlobSize200GB = 200 * Size1GB;
        public static int MaxBlocks = 50000;
        public static int MaxSingleBlobUploadThresholdInBytes64MB = 64 * Size1MB;

        public static TimeSpan DefaultMaximumExecutionTime = new TimeSpan(0, 5, 0); // 5 minutes
        public static int NumberOfWrappedRetries = 5;

        public static int PageSize = 512;

        public static bool ValidContainerName(string containerName)
        {
            if (containerName.Length < 3 ||
                containerName.Length > 63 ||
                !Regex.IsMatch(containerName, @"^[a-z0-9]+(-[a-z0-9]+)*$"))
            {
                return false;
            }
            return true;
        }
    }

    public class AzureLogAppendStream : Stream
    {
        private static TimeSpan MaxExecutionTime = new TimeSpan(0, 0, 30);
        private CloudBlobContainer _container = null;
        private CloudPageBlob _pageBlob = null;
        private long _streamSize = AzureConsts.Size1TB;

        private readonly ILogger _logger;
        private readonly string _streamName = null;
        private readonly byte _padding;

        private readonly byte[] _currentPage;
        private long _writePosition;

        public AzureLogAppendStream(Uri uri, byte padding, bool isPublic, bool deleteIfExists, ILogger logger)
        {
            _logger = logger;
            string accountName, accountKey, containerName;
            Utils.FromAzureUri(uri, out accountName, out accountKey, out containerName, out _streamName);
            _streamSize = AzureConsts.Size1TB;
            _padding = padding;

            _currentPage = new byte[AzureConsts.PageSize];
            _writePosition = 0;
            PadPage(0);
            try
            {
                InitStream(accountName, accountKey, containerName, isPublic, deleteIfExists).Wait();
            }
            catch (Exception e)
            {
                throw new BlobException(BlobException.Status.BadRequest, e.Message, e);
            }
        }

        public AzureLogAppendStream(string accountName, string accountKey, string containerName,
                                    string streamName, byte padding, bool isPublic, bool deleteIfExists, ILogger logger)
        {
            _logger = logger;
            _streamName = streamName;
            _streamSize = AzureConsts.Size1TB;
            _padding = padding;

            _currentPage = new byte[AzureConsts.PageSize];
            _writePosition = 0;
            PadPage(0);
            try
            {
                InitStream(accountName, accountKey, containerName, isPublic, deleteIfExists).Wait();
            }
            catch (Exception e)
            {
                throw new BlobException(BlobException.Status.BadRequest, e.Message, e);
            }
        }

        public bool IsThreadSafe { get { return false; } }

        private void PadPage(int begin)
        {
            for (int i = begin; i < AzureConsts.PageSize; ++i)
            {
                _currentPage[i] = _padding;
            }
        }

        private async Task InitStream(string accountName, string accountKey, string containerName, bool isPublic, bool deleteIfExists)
        {
            _container = Utils.InitContainer(accountName, accountKey, containerName, isPublic);

            // Get a reference to the page blob that will be created.
            _pageBlob = _container.GetPageBlobReference(_streamName);

            BlobRequestOptions options = new BlobRequestOptions();
            options.MaximumExecutionTime = MaxExecutionTime;

            bool exists = await Utils.WrapInRetry(_logger, async () =>
                { return await _pageBlob.ExistsAsync(options, null); });

            if (exists)
            {
                if (deleteIfExists)
                {
                    await Utils.WrapInRetry(_logger, async () => { await _pageBlob.DeleteAsync(DeleteSnapshotsOption.None, null, options, null); });
                }
                else
                {
                    throw new ApplicationException("Trying to open existing page blob " + _streamName);
                }
            }

            await Utils.WrapInRetry(_logger, async () => { await _pageBlob.CreateAsync(_streamSize, null, options, null); });

            await Utils.WrapInRetry(_logger, async () =>
                {
                    _pageBlob.Metadata["writePosition"] = 0.ToString();
                    await _pageBlob.SetMetadataAsync(null, options, null);
                    await _pageBlob.SetPropertiesAsync(null, options, null);
                });
        }

        public override bool CanRead { get { return false; } }
        public override bool CanSeek { get { return false; } }
        public override bool CanWrite { get { return true; } }
        public override long Length { get { return _writePosition; } }
        public override long Position { get { return _writePosition; } set { throw new NotImplementedException(); }  }

        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) { throw new NotImplementedException(); }
        public override long Seek(long offset, SeekOrigin origin) { throw new NotImplementedException(); }
        public override void SetLength(long value) { throw new NotImplementedException(); }

        public override void Write(byte[] buffer, int offset, int count)
        {
            WriteAsync(buffer, offset, count, System.Threading.CancellationToken.None).Wait();
        }

        public async override Task WriteAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            BlobRequestOptions options = new BlobRequestOptions();
            options.MaximumExecutionTime = MaxExecutionTime;

            if (_logger != null)
            {
                _logger.Log("In WriteAsync offset " + offset + " count " + count);
            }

            if (_writePosition + count >= _streamSize)
            {
                throw new ApplicationException("Unable to write to log file. Log file full");
            }

            if (count == 0)
            {
                return;
            }

            int pageOffset = (int) (_writePosition % AzureConsts.PageSize);
            long pageBegin = _writePosition - pageOffset;
            if (pageOffset > 0)
            {
                // first write partial page at beginning
                int toWrite = Math.Min(count, AzureConsts.PageSize - pageOffset);
                Buffer.BlockCopy(buffer, offset, _currentPage, pageOffset, toWrite);

                await Utils.WrapInRetry(_logger, async () =>
                    {
                        await _pageBlob.WritePagesAsync(new MemoryStream(_currentPage, 0, AzureConsts.PageSize), pageBegin, null, null, options, null);
                    });

                _writePosition += toWrite;
                offset += toWrite;
                count -= toWrite;
            }

            int remainder = count % AzureConsts.PageSize;
            while (count > remainder)
            {
                // write all the complete pages, in batches of no more than 4MB
                int toWrite = Math.Min(count - remainder, AzureConsts.MaxBlockSize4MB);

                await Utils.WrapInRetry(_logger, async () =>
                    {
                        await _pageBlob.WritePagesAsync(new MemoryStream(buffer, offset, toWrite), _writePosition, null, null, options, null);
                    });

                _writePosition += toWrite;
                offset += toWrite;
                count -= toWrite;
            }

            if (remainder > 0)
            {
                // write partial page at end
                Buffer.BlockCopy(buffer, offset, _currentPage, 0, remainder);
                PadPage(remainder);

                await Utils.WrapInRetry(_logger, async () =>
                    {
                        await _pageBlob.WritePagesAsync(new MemoryStream(_currentPage, 0, AzureConsts.PageSize), _writePosition, null, null, options, null);
                    });

                _writePosition += remainder;
            }
            else
            {
                if (_writePosition % AzureConsts.PageSize == 0)
                {
                    // prepare the buffered page for the next write
                    PadPage(0);
                }
            }

            await Utils.WrapInRetry(_logger, async () =>
                {
                    _pageBlob.Metadata["writePosition"] = _writePosition.ToString();
                    await _pageBlob.SetMetadataAsync(null, options, null);
                    await _pageBlob.SetPropertiesAsync(null, options, null);
                });

            if (_logger != null)
            {
                _logger.Log("Done writing");
            }
        }
    }

    public class AzureLogReaderStream : Stream
    {
        private CloudBlobContainer _container = null;
        private CloudPageBlob _pageBlob = null;

        private long _readPosition;

        public AzureLogReaderStream(Uri uri)
        {
            string accountName, accountKey, containerName, streamName;
            Utils.FromAzureUri(uri, out accountName, out accountKey, out containerName, out streamName);

            _readPosition = 0; // this is the current read position
            try
            {
                InitStream(accountName, accountKey, containerName, streamName);
            }
            catch (Exception e)
            {
                throw new BlobException(BlobException.Status.BadRequest, e.Message, e);
            }
        }

        public AzureLogReaderStream(string accountName, string accountKey, string containerName, string streamName)
        {
            _readPosition = 0; // this is the current read position
            try
            {
                InitStream(accountName, accountKey, containerName, streamName);
            }
            catch (Exception e)
            {
                throw new BlobException(BlobException.Status.BadRequest, e.Message, e);
            }
        }

        public bool IsThreadSafe { get { return false; } }

        private void InitStream(string accountName, string accountKey, string containerName, string streamName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Utils.GetConnectionString(accountName, accountKey));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve a reference to a container. 
            _container = blobClient.GetContainerReference(containerName);

            // Get a reference to the page blob
            _pageBlob = _container.GetPageBlobReference(streamName);
        }

        public override bool CanRead { get {return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return false; } }
        public override long Length 
        { 
            get 
            {
                long writePosition = 0;
                try
                {
                    _pageBlob.FetchAttributes();
                    long.TryParse(_pageBlob.Metadata["writePosition"], out writePosition);
                }
                catch
                {
                    // TODO: throw?
                    writePosition = 0;
                }

                return writePosition;
            } 
        }
        public override long Position { get { return _readPosition; } set { _readPosition = value; } }

        public override void SetLength(long value) { throw new NotImplementedException(); }
        public override void Write(byte[] buffer, int offset, int count) { throw new NotImplementedException(); }
        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            long currentLength = Length;
            // can't skip ahead of write
            if (_readPosition + count > currentLength)
            {
                count = (int)(currentLength - _readPosition);
            }

            if (count == 0)
            {
                return 0;
            }

            int numBytesRead = _pageBlob.DownloadRangeToByteArray(buffer, offset, _readPosition, count);

            _readPosition += numBytesRead;
            return numBytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    if (offset >= Length)
                    {
                        throw new ApplicationException("AzureLogReaderStream invalid seek");
                    }
                    _readPosition = offset;
                    break;
                case SeekOrigin.Current:
                    if (_readPosition + offset >= Length)
                    {
                        throw new ApplicationException("AzureLogReaderStream invalid seek");
                    }
                    _readPosition += offset;
                    break;
                case SeekOrigin.End:
                    _readPosition = Length;
                    break;
                default:
                    break;
            }

            return _readPosition;
        }
    }
}
