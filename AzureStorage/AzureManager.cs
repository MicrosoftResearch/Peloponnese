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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.Azure
{
    public abstract class AzureCollectionPart
    {
        internal AzureCollectionPart()
        {
        }

        public abstract Uri ToUri(AzureCollectionPartition parent);
    }

    internal class AzureCollectionPartSmallBlob : AzureCollectionPart
    {
        public AzureCollectionPartSmallBlob(List<string> smallBlobs)
        {
            blobs = smallBlobs.Aggregate("", (a, s) => a + "," + s, a => a.TrimStart(','));
        }

        public override Uri ToUri(AzureCollectionPartition parent)
        {
            UriBuilder builder = new UriBuilder(parent.BaseUri);

            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(parent.BaseUri.Query);
            query["blobs"] = blobs;
            builder.Query = query.ToString();

            return builder.Uri;
        }

        private string blobs;
    }

    internal class AzureCollectionPartSingleBlob : AzureCollectionPart
    {
        public AzureCollectionPartSingleBlob(string name)
        {
            blob = name;
        }

        public override Uri ToUri(AzureCollectionPartition parent)
        {
            UriBuilder builder = new UriBuilder(parent.BaseUri);
            builder.Path += blob;
            return builder.Uri;
        }

        private string blob;
    }

    internal class AzureCollectionPartSubBlob : AzureCollectionPart
    {
        public AzureCollectionPartSubBlob(string name, long o, long l, bool sB)
        {
            blob = name;
            offset = o;
            length = l;
            seekBoundaries = sB;
        }

        public override Uri ToUri(AzureCollectionPartition parent)
        {
            UriBuilder builder = new UriBuilder(parent.BaseUri);
            builder.Path += blob;

            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(parent.BaseUri.Query);

            query["offset"] = offset.ToString();
            query["length"] = length.ToString();
            if (seekBoundaries)
            {
                query["seekBoundaries"] = parent.SeekBoundaries;
            }

            builder.Query = query.ToString();

            return builder.Uri;
        }

        private string blob;
        private long offset;
        private long length;
        private bool seekBoundaries;
    }

    public class AzureCollectionPartition
    {
        private class AzureCollectionStream : Stream
        {
            private readonly AzureCollectionPartition partition;
            private IEnumerator<CloudBlockBlob> blobs;
            private Stream currentBlob;
            private long cachedLength = -1;
            private long position = 0;

            public AzureCollectionStream(AzureCollectionPartition p)
            {
                partition = p;
                blobs = partition.GetBlobEnumerable().GetEnumerator();
                currentBlob = null;
            }

            private async Task<bool> Advance(System.Threading.CancellationToken cancellationToken)
            {
                if (blobs.MoveNext())
                {
                    currentBlob = await blobs.Current.OpenReadAsync(cancellationToken);
                    return true;
                }
                else
                {
                    blobs = null;
                    return false;
                }
            }

            protected override void Dispose(bool disposing)
            {
                if (currentBlob != null)
                {
                    currentBlob.Dispose();
                    currentBlob = null;
                    blobs = null;
                }

                base.Dispose(disposing);
            }

            public override bool CanRead
            {
                get { return true; }
            }

            public override bool CanSeek
            {
                get { return false; }
            }

            public override bool CanWrite
            {
                get { return false; }
            }

            public override void Flush()
            {
                throw new NotImplementedException();
            }

            public override long Length
            {
                get
                {
                    if (cachedLength == -1)
                    {
                        // enumerating the whole partition sets the length
                        int nParts = partition.GetPartition().Count();
                        cachedLength = partition.TotalLength;
                    }

                    return cachedLength;
                }
            }

            public override long Position
            {
                get
                {
                    return position;
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return ReadAsync(buffer, offset, count).Result;
            }

            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
            {
                while (true)
                {
                    if (blobs == null)
                    {
                        // we are at the end of the partition
                        return 0;
                    }

                    if (currentBlob == null)
                    {
                        bool moreBlobs = await Advance(cancellationToken);
                        if (!moreBlobs)
                        {
                            // we are at the end of the partition
                            return 0;
                        }
                    }

                    int nRead = await currentBlob.ReadAsync(buffer, offset, count, cancellationToken);
                    if (nRead != 0)
                    {
                        position += nRead;
                        return nRead;
                    }

                    currentBlob.Dispose();
                    currentBlob = null;

                    // go around and try to read from the next blob
                }
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }
        }

        private Uri _uri;
        private CloudBlobContainer _container = null;
        private string _seekBoundaries;
        private long _totalLength;

        public AzureCollectionPartition(Uri uri, bool isPublic = false)
        {
            _uri = uri;
            _totalLength = -1;

            try
            {
                string blobName;
                _container = Utils.InitContainer(uri, isPublic, out blobName);
            }
            catch (Exception ex)
            {
                throw new BlobException(BlobException.Status.BadRequest, ex.Message, ex);
            }
        }

        public AzureCollectionPartition(string accountName, string containerName, string blobName, string accountKey = null, bool isPublic = false)
        {
            _uri = Utils.ToAzureUri(accountName, containerName, blobName, null, accountKey);
            _totalLength = -1;

            try
            {
                _container = Utils.InitContainer(accountName, accountKey, containerName, isPublic);
            }
            catch (Exception ex)
            {
                throw new BlobException(BlobException.Status.BadRequest, ex.Message, ex);
            }
        }

        public Uri BaseUri { get { return _uri; } }

        public string SeekBoundaries { get { return _seekBoundaries; } }

        public long TotalLength { get { return _totalLength; } }

        private IEnumerable<long> FixedBlockLengths(long blockLength, long totalLength)
        {
            while (totalLength > blockLength)
            {
                yield return blockLength;
                totalLength -= blockLength;
            }
            yield return totalLength;
        }

        public bool IsCollectionExists()
        {
            return GetBlobEnumerable().GetEnumerator().MoveNext();
        }

        public void DeleteCollection()
        {
            foreach (CloudBlockBlob blob in GetBlobEnumerable())
            {
                blob.Delete();
            }
        }

        public void DeleteMatchingParts(Uri parts)
        {
            string thisAccount, thisKey, thisContainer, thisCollection;
            Utils.FromAzureUri(parts, out thisAccount, out thisKey, out thisContainer, out thisCollection);

            string matchAccount, matchKey, matchContainer, blobPrefix;
            Utils.FromAzureUri(parts, out matchAccount, out matchKey, out matchContainer, out blobPrefix);

            if ((thisAccount != matchAccount) ||
                (thisKey != matchKey) ||
                (thisContainer != matchContainer) ||
                (!blobPrefix.StartsWith(thisCollection)))
            {
                throw new ApplicationException("Collection " + _uri.AbsoluteUri + " asked to delete non-matching parts " + parts.AbsoluteUri);
            }

            // use Container.ListBlobs here directly instead of getting a directory reference because
            // we're looking for blobs that match a partial prefix
            IEnumerable<CloudBlockBlob> matching =
                _container.ListBlobs(blobPrefix)
                          .Where(b => b is CloudBlockBlob)
                          .Select(b => b as CloudBlockBlob);

            foreach (CloudBlockBlob blob in matching)
            {
                blob.Delete();
            }
        }

        private IEnumerable<CloudBlockBlob> GetBlobEnumerable()
        {
            string account, key, container, blobPrefix;
            Utils.FromAzureUri(_uri, out account, out key, out container, out blobPrefix);

            CloudBlockBlob singleton = _container.GetBlockBlobReference(blobPrefix);
            if (singleton.Exists())
            {
                singleton.FetchAttributes();
                return new CloudBlockBlob[] { singleton };
            }

            CloudBlobDirectory directory = _container.GetDirectoryReference(blobPrefix);
            return directory.ListBlobs(true, BlobListingDetails.Metadata)
                            .Where(x => x.GetType() == typeof(CloudBlockBlob))
                            .Select(x => x as CloudBlockBlob);
        }

        public IEnumerable<AzureCollectionPart> GetPartition()
        {
            string account, key, container, blobPrefix;
            Utils.FromAzureUri(_uri, out account, out key, out container, out blobPrefix);

            NameValueCollection query = System.Web.HttpUtility.ParseQueryString(_uri.Query);

            long lowReadSize = -1;
            if (query["lowReadSize"] != null)
            {
                lowReadSize = Int64.Parse(query["lowReadSize"]);
                query.Remove("lowReadSize");
            }

            long highReadSize = Int64.MaxValue;
            if (query["highReadSize"] != null)
            {
                highReadSize = Int64.Parse(query["highReadSize"]);
                query.Remove("highReadSize");
            }

            _seekBoundaries = null;
            if (query["seekBoundaries"] != null)
            {
                _seekBoundaries = query["seekBoundaries"];
                query.Remove("seekBoundaries");
            }

            UriBuilder strippedQuery = new UriBuilder(_uri);
            strippedQuery.Query = query.ToString();
            _uri = strippedQuery.Uri;

            IEnumerator<CloudBlockBlob> blobs = GetBlobEnumerable().GetEnumerator();

            long totalLength = 0;
            bool moreBlobs = blobs.MoveNext();
            while (moreBlobs)
            {
                // first accumulate any small blobs that need to be combined into a single URI
                List<string> smallBlobs = new List<string>();
                long combinedLength = 0;
                while (moreBlobs && combinedLength + blobs.Current.Properties.Length <= lowReadSize)
                {
                    if (blobs.Current.Name.StartsWith(blobPrefix))
                    {
                        // ignore empty inputs
                        if (blobs.Current.Properties.Length > 0)
                        {
                            smallBlobs.Add(blobs.Current.Name.Substring(blobPrefix.Length));
                            combinedLength += blobs.Current.Properties.Length;
                        }

                        moreBlobs = blobs.MoveNext();
                    }
                    else
                    {
                        throw new ApplicationException("Unexpected blob " + blobs.Current.Name + " in directory " + blobPrefix);
                    }
                }

                if (smallBlobs.Count > 0)
                {
                    totalLength += combinedLength;
                    yield return new AzureCollectionPartSmallBlob(smallBlobs);
                    // go back around the loop to see if there are more small blobs to combine
                    continue;
                }

                if (moreBlobs)
                {
                    // now take the next blob and split it into one or more pieces
                    CloudBlockBlob blob = blobs.Current;

                    totalLength += blob.Properties.Length;

                    string blobName;
                    if (blob.Name.StartsWith(blobPrefix))
                    {
                        blobName = blob.Name.Substring(blobPrefix.Length);
                    }
                    else
                    {
                        throw new ApplicationException("Unexpected blob " + blob.Name + " in directory " + blobPrefix);
                    }

                    bool canSplitAtBlocks = (blob.Metadata.ContainsKey("recordsRespectBlockBoundaries") &&
                                             blob.Metadata["recordsRespectBlockBoundaries"] == "true");

                    bool canSplit = canSplitAtBlocks || (_seekBoundaries != null);

                    if (blob.Properties.Length < highReadSize || !canSplit)
                    {
                        // we either don't want to split this blob or can't, so emit the entire blob and
                        // move on to the next one
                        yield return new AzureCollectionPartSingleBlob(blobName);
                        moreBlobs = blobs.MoveNext();
                        continue;
                    }

                    // make a blob-local copy of seekBoundaries that may be overridden below
                    bool thisSeekBoundaries = (_seekBoundaries != null);

                    IEnumerator<long> blockLengths;
                    if (canSplitAtBlocks)
                    {
                        if (_seekBoundaries == null)
                        {
                            // use blocks to chunk the reads
                            blockLengths = blob.DownloadBlockList().Select(b => b.Length).GetEnumerator();
                        }
                        else
                        {
                            // we have a choice between using blocks or record boundaries to split. Decide based on
                            // whether the written blocks are compatible with highReadSize or not. If the blocks are
                            // longer than highReadSize we would rather seek to record boundaries
                            long[] blockLengthArray = blob.DownloadBlockList().Select(b => b.Length).ToArray();
                            if (blockLengthArray.Count(l => l < highReadSize) >= blockLengthArray.Length / 2)
                            {
                                // more than half of the blocks are shorter than highReadSize so use them for
                                // subdividing the blob
                                blockLengths = blockLengthArray.AsEnumerable().GetEnumerator();
                                // don't tell the reader to seek for record boundaries
                                thisSeekBoundaries = false;
                            }
                            else
                            {
                                // use fixed chunks instead of blocks and seek for record boundaries at the reader
                                blockLengths = FixedBlockLengths(highReadSize, blob.Properties.Length).GetEnumerator();
                            }
                        }
                    }
                    else
                    {
                        // use fixed chunks and seek for record boundaries at the reader
                        blockLengths = FixedBlockLengths(highReadSize, blob.Properties.Length).GetEnumerator();
                    }

                    long offset = 0;
                    long chunkLength = 0;
                    blockLengths.MoveNext();
                    while (offset < blob.Properties.Length)
                    {
                        // accumulate a chunk that is at least 1 block long, but no longer than highReadSize
                        // unless the first block is longer than highReadSize
                        while (offset < blob.Properties.Length &&
                               (chunkLength == 0 || chunkLength + blockLengths.Current <= highReadSize))
                        {
                            chunkLength += blockLengths.Current;
                            offset += blockLengths.Current;
                            blockLengths.MoveNext();
                        }

                        // make a URI describing this portion of the blob
                        yield return new AzureCollectionPartSubBlob(blobName, offset-chunkLength, chunkLength, thisSeekBoundaries);
                        chunkLength = 0;
                    }

                    moreBlobs = blobs.MoveNext();
                } // if (moreBlobs)
            } // while (moreBlobs)

            _totalLength = totalLength;
        }

        public Stream GetReadStream()
        {
            return new AzureCollectionStream(this);
        }
    }

    public class AzureBlockBlobWriter
    {
        private readonly TimeSpan _maximumExecutionTime;
        private readonly ILogger _logger;
        private CloudBlobContainer _container = null;
        private CloudBlockBlob _blob;

        private List<string> _toCommitList = new List<string>();
        private Int32 _blockCount = 0;

        public AzureBlockBlobWriter(Uri uri, TimeSpan maxExecutionTime, bool isPublic = false, ILogger logger = null)
        {
            _maximumExecutionTime = maxExecutionTime;
            _logger = logger;
            try
            {
                string blobName;
                _container = Utils.InitContainer(uri, isPublic, out blobName);
                InitBlob(blobName);
            }
            catch (Exception ex)
            {
                throw new BlobException(BlobException.Status.BadRequest, ex.Message, ex);
            }
        }

        public AzureBlockBlobWriter(string accountName, string accountKey, string containerName, string blobName, TimeSpan maxExecutionTime, bool isPublic = false, ILogger logger = null)
        {
            _maximumExecutionTime = maxExecutionTime;
            _logger = logger;
            try
            {
                _container = Utils.InitContainer(accountName, accountKey, containerName, isPublic);
                InitBlob(blobName);
            }
            catch (Exception ex)
            {
                throw new BlobException(BlobException.Status.BadRequest, ex.Message, ex);
            }
        }

        private string IntIdToStringID(int id)
        {
            return id.ToString("D8");
        }

        private void InitBlob(string blobName)
        {
            _blob = _container.GetBlockBlobReference(blobName);
            _blockCount = 0;
        }

        public async Task WriteBlockAsync(byte[] buffer, int offset, int size)
        {
            if (_blockCount >= AzureConsts.MaxBlocks)
                throw new BlobException(BlobException.Status.MaxBlocksExceeded);
            if (size > AzureConsts.MaxBlockSize4MB)
                throw new BlobException(BlobException.Status.MaxBlockSizeExceeded);

            string blockName = IntIdToStringID(_blockCount);
            _blockCount++;

            await Utils.WrapInRetry(_logger, async () =>
                {
                    using (Stream stream = new MemoryStream(buffer, offset, size))
                    {
                        stream.Position = 0;
                        if (_logger != null)
                        {
                            _logger.Log("Writing block " + blockName + " " + size + " bytes");
                        }
                        BlobRequestOptions options = new BlobRequestOptions();
                        options.MaximumExecutionTime = _maximumExecutionTime;
                        await _blob.PutBlockAsync(blockName, stream, null, null, options, null);
                        if (_logger != null)
                        {
                            _logger.Log("Written block");
                        }
                        _toCommitList.Add(blockName);
                        return;
                    }
                });
        }

        public async Task CommitBlocksAsync()
        {
            await Utils.WrapInRetry(_logger, async () =>
                {
                    if (_logger != null)
                    {
                        _logger.Log("Putting block list length " + _toCommitList.Count);
                    }
                    await _blob.PutBlockListAsync(_toCommitList);
                    if (_logger != null)
                    {
                        _logger.Log("Put block list");
                    }
                });
        }

        public async Task SetMetadataAsync(string key, string value)
        {
            await Utils.WrapInRetry(_logger, async () =>
                {
                    if (_logger != null)
                    {
                        _logger.Log("Setting metadata " + key + "=" + value);
                    }
                    _blob.Metadata[key] = value;
                    await _blob.SetMetadataAsync();
                    if (_logger != null)
                    {
                        _logger.Log("Set metadata");
                    }
                });
        }
        public async Task<string> GetMetaDataAsync(string key)
        {
            return await Utils.WrapInRetry(_logger, async () =>
                {
                    if (_logger != null)
                    {
                        _logger.Log("Getting metadata");
                    }
                    await _blob.FetchAttributesAsync();
                    if (_logger != null)
                    {
                        _logger.Log("Got metadata");
                    }
                    return _blob.Metadata[key];
                });
        }
    }
}
