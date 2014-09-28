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

using Microsoft.Research.Peloponnese.Shared;

namespace Microsoft.Research.Peloponnese.Hdfs
{
    public class HdfsClient : HdfsClientBase
    {
        private struct NameNode
        {
            public string host;
            public int port;

            public NameNode(string host, int port)
            {
                this.host = host;
                this.port = port;
            }
        }

        private readonly string user;
        private readonly Dictionary<NameNode, HdfsInstance> instances;

        private class HdfsStreamReader : DfsStreamReader
        {
            private HdfsReader reader;
            private long offset;
            private long bytesToRead;

            protected override int ReadInternal(byte[] buffer, int bufferOffset, int count)
            {
                int toRead = (int)Math.Min(count, bytesToRead);
                if (toRead == 0)
                {
                    return 0;
                }

                int nRead = reader.ReadBlock(this.offset, buffer, bufferOffset, toRead);

                this.offset += nRead;
                this.bytesToRead -= nRead;

                return nRead;
            }

            public override void Close()
            {
                if (reader != null)
                {
                    reader.Dispose();
                    reader = null;
                }
            }

            public HdfsStreamReader(HdfsInstance instance, string dfsPath, long offset, long length, int byteBufferSize)
                : base(byteBufferSize)
            {
                this.reader = instance.OpenReader(dfsPath);
                this.offset = offset;
                this.bytesToRead = length;
            }
        }

        private class HdfsStreamWriter : DfsStreamWriter
        {
            private HdfsWriter writer;

            protected override void WriteInternal(byte[] buffer, int offset, int count)
            {
                writer.WriteBlock(buffer, offset, count, false);
            }

            public override void Flush()
            {
                writer.Sync();
            }

            public override void Close()
            {
                if (writer != null)
                {
                    writer.Dispose();
                    writer = null;
                }
            }

            public HdfsStreamWriter(HdfsInstance instance, string dfsPath, string user, int bufferSize, long blockSize)
            {
                this.writer = instance.OpenCreate(dfsPath, bufferSize, blockSize);
                instance.SetOwnerAndPermission(dfsPath, user, null, Convert.ToInt16("644", 8));
            }
        }

        private HdfsInstance Instance(Uri uri)
        {
            NameNode nameNodeAndPort = new NameNode(uri.Host, uri.Port);

            HdfsInstance instance;
            if (!this.instances.TryGetValue(nameNodeAndPort, out instance))
            {
                instance = new HdfsInstance(uri);
                this.instances.Add(nameNodeAndPort, instance);
            }

            return instance;
        }

        public override void Dispose()
        {
            foreach (HdfsInstance instance in this.instances.Values)
            {
                instance.Dispose();
            }
            this.instances.Clear();
        }

        public override void Rename(Uri dstPath, Uri srcPath)
        {
            HdfsInstance dstInstance = this.Instance(dstPath);
            HdfsInstance srcInstance = this.Instance(srcPath);
            if (dstInstance != srcInstance)
            {
                throw new ApplicationException("Can't rename HDFS files from different file systems: " + dstPath.AbsoluteUri + ", " + srcPath.AbsoluteUri);
            }
            dstInstance.RenameFile(dstPath.AbsolutePath, srcPath.AbsolutePath);
        }

        public override void DeleteDfsFile(Uri dfsPath, bool recursive = false)
        {
            this.Instance(dfsPath).DeleteFile(dfsPath.AbsolutePath, recursive);
        }

        public override void EnsureDirectory(Uri dfsDir, bool worldWritable)
        {
            if (!IsFileExists(dfsDir))
            {
                var components = dfsDir.AbsolutePath.Split(new[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
                if (components.Length == 0)
                {
                    throw new ApplicationException("HDFS empty directory does not exist");
                }

                string upperDir = "";
                for (int i = 0; i < components.Length - 1; ++i)
                {
                    upperDir = upperDir + components[i] + "/";
                }

                UriBuilder builder = new UriBuilder(dfsDir);
                builder.Path = upperDir;
                EnsureDirectory(builder.Uri, worldWritable);

                this.Instance(dfsDir).EnsureDirectory(dfsDir.AbsolutePath);
                this.Instance(dfsDir).SetOwnerAndPermission(
                    dfsDir.AbsolutePath, this.user, null,
                    worldWritable ? Convert.ToInt16("777", 8) : Convert.ToInt16("755", 8));
            }

        }

        public override IEnumerable<Uri> ExpandFileOrDirectory(Uri dfsPath)
        {
            HdfsFileInfo info = this.Instance(dfsPath).GetFileInfo(dfsPath.AbsolutePath, false);
            if (info.IsDirectory)
            {
                foreach (string path in info.fileNameArray)
                {
                    UriBuilder builder = new UriBuilder(dfsPath);
                    builder.Path = path;
                    foreach (Uri entry in this.ExpandFileOrDirectory(builder.Uri))
                    {
                        yield return entry;
                    }
                }
            }
            else
            {
                UriBuilder builder = new UriBuilder(dfsPath);
                builder.Path = info.fileNameArray[0];
                yield return builder.Uri;
            }
        }

        public override IEnumerable<Uri> EnumerateSubdirectories(Uri dfsPath)
        {
            string[] directories = this.Instance(dfsPath).EnumerateSubdirectories(dfsPath.AbsolutePath);

            foreach (string directory in directories)
            {
                UriBuilder builder = new UriBuilder(dfsPath);
                builder.Path = directory;
                yield return builder.Uri;
            }
        }

        public override IEnumerable<HdfsFile> ExpandFileOrDirectoryToFile(Uri uri)
        {
            HdfsFileInfo fileInfo = this.Instance(uri).GetFileInfo(uri.AbsolutePath, false);

            if (fileInfo.IsDirectory)
            {
                foreach (string subFile in fileInfo.fileNameArray)
                {
                    UriBuilder builder = new UriBuilder(uri);
                    builder.Path = subFile;
                    foreach (HdfsFile file in ExpandFileOrDirectoryToFile(builder.Uri))
                    {
                        yield return file;
                    }
                }
            }
            else
            {
                yield return new HdfsFile
                {
                    path = uri,
                    length = fileInfo.Size,
                    blockSize = fileInfo.BlockSize
                };
            }
        }

        public override IEnumerable<IPEndPoint[]> GetBlockLocations(HdfsFile file)
        {
            HdfsFileInfo fileInfo = this.Instance(file.path).GetFileInfo(file.path.AbsolutePath, true);
            foreach (HdfsBlockInfo info in fileInfo.blockArray)
            {
                IPEndPoint[] endpoints = new IPEndPoint[info.Endpoints.Length];
                for (int i = 0; i < endpoints.Length; ++i)
                {
                    string[] parts = info.Endpoints[i].Split(':');
                    endpoints[i] = new IPEndPoint(IPAddress.Parse(parts[0]), Int32.Parse(parts[1]));
                }
                yield return endpoints;
            }
        }

        public override Stream GetDfsStreamReader(Uri requestUri, long offset, long length, int byteBufferSize, IPEndPoint dataNode)
        {
            return GetDfsStreamReader(requestUri, offset, length, byteBufferSize);
        }

        public override Stream GetDfsStreamReader(Uri dfsPath, long offset, long length, int byteBufferSize)
        {
            return new HdfsStreamReader(this.Instance(dfsPath), dfsPath.AbsolutePath, offset, length, byteBufferSize);
        }

        public override Stream GetDfsStreamWriter(Uri dfsPath)
        {
            return GetDfsStreamWriter(dfsPath, 1024 * 1024, -1);
        }

        public Stream GetDfsStreamWriter(Uri dfsPath, int bufferSize, long blockSize)
        {
            return new HdfsStreamWriter(this.Instance(dfsPath), dfsPath.AbsolutePath, this.user, bufferSize, blockSize);
        }

        public override void GetDirectoryContentSummary(Uri dfsPath, bool expandBlocks, ref long totalSize, ref int numberOfParts)
        {
            if (expandBlocks)
            {
                HdfsFileInfo info = this.Instance(dfsPath).GetFileInfo(dfsPath.AbsolutePath, true);
                totalSize = info.totalSize;
                numberOfParts = info.blockArray.Length;
            }
            else
            {
                long numberOfFiles;
                this.Instance(dfsPath).GetContentSummary(dfsPath.AbsolutePath, out totalSize, out numberOfFiles);
                numberOfParts = (int)numberOfFiles;
            }
        }

        public override bool GetFileStatus(Uri dfsPath, out long modificationTime, out long size)
        {
            if (this.Instance(dfsPath).IsFileExists(dfsPath.AbsolutePath))
            {
                HdfsFileInfo info = this.Instance(dfsPath).GetFileInfo(dfsPath.AbsolutePath, false);
                modificationTime = info.LastModified;
                size = info.Size;
                return true;
            }
            else
            {
                modificationTime = -1;
                size = -1;
                return false;
            }
        }

        public override bool IsFileExists(Uri dfsPath)
        {
            return this.Instance(dfsPath).IsFileExists(dfsPath.AbsolutePath);
        }

        public override bool IsThreadSafe
        {
            get { return false; }
        }

        public HdfsClient()
        {
            Microsoft.Research.Peloponnese.Hadoop.Initialize();
            this.instances = new Dictionary<NameNode, HdfsInstance>();
            this.user = GetDfsUser(null);
        }

        public HdfsClient(string user)
        {
            Microsoft.Research.Peloponnese.Hadoop.Initialize();
            this.instances = new Dictionary<NameNode, HdfsInstance>();
            this.user = GetDfsUser(user);
        }
    }
}
