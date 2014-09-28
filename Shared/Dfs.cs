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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.Shared
{
    /// <summary>
    /// wrapper that exposes a Stream interface to an hdfs file reader. Calls to Read are passed
    /// through directly to a sequence of underlying WebHdfs requests. Calls to ReadByte are buffered
    /// in this class. This buffering is designed so that it is always possible to seek backwards one
    /// byte in the stream after a successful ReadByte without closing any underlying WebHdfs request;
    /// this is useful for performance when seeking for line ends. Other Seeks as supported, but in
    /// general cause the WebHdfs request to be closed and a new one opened, so performance will be bad.
    /// </summary>
    public abstract class DfsStreamReader : Stream
    {
        protected abstract int ReadInternal(byte[] buffer, int offset, int count);

        /// <summary>
        /// the read position in the stream. this corresponds to offset this.startOffset+this.position in the
        /// hdfs file
        /// </summary>
        private long position;

        /// <summary>
        /// the buffer used for byte-at-time reads
        /// </summary>
        private byte[] buffer;
        /// <summary>
        /// the current offset into the byte-at-a-time buffer
        /// </summary>
        private int bufferOffset;
        /// <summary>
        /// the number of valid bytes in the byte-at-a-time buffer
        /// </summary>
        private int bufferValid;

        #region stream interface implementation
        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
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
            get { throw new NotImplementedException(); }
        }

        public override long Position
        {
            get
            {
                return this.position;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Byte at a time reads are buffered, partly to improve performance and partly so that it is possible to
        /// seek backwards the most recently read byte, which helps to avoid breaking the WebHdfs request when seeking
        /// to the end of a line of text
        /// </summary>
        /// <returns>the next byte in the stream, or -1 if the stream has ended</returns>
        public override int ReadByte()
        {
            if (this.bufferOffset < this.bufferValid)
            {
                // we already have unreturned valid bytes in the buffer, so return the next one and bump the relevant
                // pointers
                int nextByte = this.buffer[this.bufferOffset];

                ++this.bufferOffset;
                ++this.position;

                return nextByte;
            }

            // fetch another buffer worth of data
            this.bufferValid = Read(this.buffer, 0, this.buffer.Length);
            if (this.bufferValid == 0)
            {
                // we have reached the end of the stream
                return -1;
            }
            else
            {
                // the call to Read advanced position past the buffered data, so put it back to the start of the buffer
                this.position -= this.bufferValid;
                // recurse to actually read the byte; this is guaranteed to succeed without recursing again
                return ReadByte();
            }
        }

        public override int Read(byte[] dstBuffer, int offset, int count)
        {
            int bufferedDataRemaining = this.bufferValid - this.bufferOffset;
            if (bufferedDataRemaining > 0)
            {
                // there is leftover data in the byte-at-a-time buffer, so return as much of it as we can
                // and bump the relevant pointers
                int toCopy = Math.Min(count, bufferedDataRemaining);
                Array.Copy(this.buffer, this.bufferOffset, dstBuffer, offset, toCopy);

                this.bufferOffset += toCopy;
                this.position += toCopy;

                return toCopy;
            }

            // ensure there won't appear to be any valid data in the buffer if we try to seek later
            this.bufferOffset = 0;
            this.bufferValid = 0;

            // read some bytes out of the current request stream
            int nRead = this.ReadInternal(dstBuffer, offset, count);
            this.position += nRead;

            return nRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    throw new NotImplementedException();

                case SeekOrigin.Current:
                    break;

                case SeekOrigin.End:
                    throw new NotImplementedException();
            }

            long adjustedBufferOffset = this.bufferOffset + offset;
            if (adjustedBufferOffset < 0 || adjustedBufferOffset > this.bufferValid)
            {
                throw new NotImplementedException();
            }

            this.bufferOffset = (int)adjustedBufferOffset;
            this.position += offset;

            return this.position;
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
        }
        #endregion

        /// <summary>
        /// create a ReaderStream
        /// </summary>
        /// <param name="client">client to use to read from the file</param>
        /// <param name="dataNode">datanode to use, or null if it isn't known</param>
        /// <param name="file">hdfs file to read</param>
        /// <param name="startOffset">starting offset in the hdfs file</param>
        /// <param name="length">number of bytes to read from the hdfs file. this must not go beyond the end
        /// of the file</param>
        /// <param name="requestLength">number of bytes to request in each webhdfs read request</param>
        /// <param name="byteBufferSize">size of the buffer to use for byte-at-a-time data reads</param>
        public DfsStreamReader(int byteBufferSize)
        {
            this.buffer = new byte[byteBufferSize];
            this.bufferOffset = 0;
            this.bufferValid = 0;

            this.position = 0;
        }
    }

    /// <summary>
    /// wrapper around a web request to create or append to a file, which cleans up the http request when the stream is closed
    /// </summary>
    public abstract class DfsStreamWriter : Stream
    {
        protected abstract void WriteInternal(byte[] buffer, int offset, int count);

        /// <summary>
        /// the write position in the stream.
        /// </summary>
        private long position;

        #region stream interface implementation
        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get { return this.position; }
        }

        public override long Position
        {
            get
            {
                return this.position;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] srcBuffer, int offset, int count)
        {
            this.WriteInternal(srcBuffer, offset, count);
            this.position += count;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
        }
        #endregion

        /// <summary>
        /// create a WriterStream
        /// </summary>
        /// <param name="request">web request to write data into</param>
        /// <param name="desiredStatusCode">status code that a successful request will return. this is
        /// HttpStatusCode.Created for CREATE and HttpStatusCode.Ok for APPEND</param>
        public DfsStreamWriter()
        {
            this.position = 0;
        }
    }

    public abstract class DfsClient : IDisposable
    {
        private class DirectoryStreamReader : Stream
        {
            private readonly IEnumerator<Uri> directoryFiles;
            private readonly DfsClient client;

            private Stream currentStream;

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
                get { throw new NotImplementedException(); }
            }

            public override long Position
            {
                get
                {
                    throw new NotImplementedException();
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                while (true)
                {
                    if (currentStream == null)
                    {
                        if (directoryFiles.MoveNext())
                        {
                            Uri currentFile = directoryFiles.Current;
                            currentStream = client.GetDfsStreamReader(currentFile);
                        }
                        else
                        {
                            // we are at the end of the directory
                            return 0;
                        }
                    }

                    int nRead = currentStream.Read(buffer, offset, count);
                    if (nRead > 0)
                    {
                        return nRead;
                    }

                    // we're at the end of one file; start reading from the next rather than returning 0 bytes
                    currentStream = null;
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

            public DirectoryStreamReader(DfsClient client, Uri directoryPath)
            {
                this.client = client;
                this.directoryFiles = client.ExpandFileOrDirectory(directoryPath).GetEnumerator();
                this.currentStream = null;
            }
        }

        public static string GetDfsUser(string user)
        {
            if (user == null)
            {
                user = Environment.GetEnvironmentVariable("USER");
            }
            if (user == null)
            {
                user = Environment.UserName;
            }
            return user;
        }

        public abstract void Dispose();

        public abstract bool IsThreadSafe { get; }

        public abstract Uri GetClusterInternalUri(Uri externalUri);

        public abstract bool IsFileExists(Uri dfsPath);

        public abstract IEnumerable<Uri> ExpandFileOrDirectory(Uri dfsPath);

        public abstract void EnsureDirectory(Uri dfsDir, bool worldWritable);

        public abstract void DeleteDfsFile(Uri dfsPath, bool recursive = false);

        public abstract bool GetFileStatus(Uri dfsPath, out long modificationTime, out long size);

        public abstract void GetDirectoryContentSummary(Uri dfsPath, bool expandBlocks, ref long totalSize, ref int numberOfParts);

        public abstract Uri Combine(Uri baseUri, params string[] components);

        public abstract Stream GetDfsStreamReader(Uri dfsPath, long offset, long length, int byteBufferSize);

        public Stream GetDfsDirectoryStreamReader(Uri dfsPath)
        {
            return new DirectoryStreamReader(this, dfsPath);
        }

        public Stream GetDfsStreamReader(Uri dfsPath)
        {
            long modificationTime;
            long length;

            if (!GetFileStatus(dfsPath, out modificationTime, out length))
            {
                throw new ApplicationException("Dfs file " + dfsPath + " not found");
            }

            return GetDfsStreamReader(dfsPath, 0, length, 1);
        }

        public void GetDfsFile(string localPath, Uri dfsPath)
        {
            using (var fs = new FileStream(localPath, FileMode.CreateNew, FileAccess.Write))
            {
                using (Stream rs = GetDfsStreamReader(dfsPath))
                {
                    rs.CopyTo(fs);
                }
            }
        }

        public abstract Stream GetDfsStreamWriter(Uri dfsPath);

        public void PutDfsFile(Uri dfsPath, string localPath)
        {
            using (var fs = new FileStream(localPath, FileMode.Open, FileAccess.Read))
            {
                using (Stream rs = GetDfsStreamWriter(dfsPath))
                {
                    fs.CopyTo(rs);
                }
            }
        }

        public void PutDfsFile(Uri dfsPath, byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                using (Stream rs = GetDfsStreamWriter(dfsPath))
                {
                    ms.CopyTo(rs);
                }
            }
        }

        public abstract void PutDfsFileResolveConflict(Uri dfsPath, string localPath);

        public abstract void PutDfsFileResolveConflict(Uri dfsPath, byte[] bytes);
    }

    [Serializable]
    public class HdfsFile
    {
        /// <summary>
        /// the full path name of the file
        /// </summary>
        public Uri path;
        /// <summary>
        /// the file size in bytes
        /// </summary>
        public long length;
        /// <summary>
        /// the size in bytes of each file block
        /// </summary>
        public long blockSize;
    }

    public abstract class HdfsClientBase : DfsClient
    {
        public abstract void Rename(Uri dstPath, Uri srcPath);
        public abstract IEnumerable<Uri> EnumerateSubdirectories(Uri uri);
        public abstract IEnumerable<HdfsFile> ExpandFileOrDirectoryToFile(Uri uri);
        public abstract IEnumerable<IPEndPoint[]> GetBlockLocations(HdfsFile file);
        public abstract Stream GetDfsStreamReader(Uri requestUri, long offset, long length, int byteBufferSize, IPEndPoint dataNode);

        public override Uri GetClusterInternalUri(Uri externalUri)
        {
            return externalUri;
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

        public override void PutDfsFileResolveConflict(Uri dfsPath, string localPath)
        {
            UriBuilder builder = new UriBuilder(dfsPath);
            builder.Path += "." + Guid.NewGuid().ToString();
            Uri tempPath = builder.Uri;

            try
            {
                PutDfsFile(tempPath, localPath);
                Rename(dfsPath, tempPath);
            }
            catch
            {
            }
            finally
            {
                // delete temp file
                if (true == IsFileExists(tempPath))
                {
                    DeleteDfsFile(tempPath);
                }
            }

            // check if file exists
            if (false == IsFileExists(dfsPath))
            {
                throw new ApplicationException("Failed to upload DFS file " + dfsPath);
            }
        }

        public override void PutDfsFileResolveConflict(Uri dfsPath, byte[] bytes)
        {
            UriBuilder builder = new UriBuilder(dfsPath);
            builder.Path += "." + Guid.NewGuid().ToString();
            Uri tempPath = builder.Uri;

            try
            {
                PutDfsFile(tempPath, bytes);
                Rename(dfsPath, tempPath);
            }
            catch
            {
            }
            finally
            {
                // delete temp file
                if (true == IsFileExists(tempPath))
                {
                    DeleteDfsFile(tempPath);
                }
            }

            // check if file exists
            if (false == IsFileExists(dfsPath))
            {
                throw new ApplicationException("Failed to upload HDFS file " + dfsPath);
            }
        }
    }
}
