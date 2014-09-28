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

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Microsoft.Research.Peloponnese.Shared;

namespace Microsoft.Research.Peloponnese.WebHdfs
{
    public class WebHdfsClient : HdfsClientBase
    {
        #region private helper classes

        private class WebHdfsReader : DfsStreamReader
        {
            // the web response, to be closed when we are done
            private HttpWebResponse response;
            // the stream of data coming out of the web response
            private Stream responseStream;

            // the number of bytes remaining to read
            private long bytesToRead;

            /// <summary>
            /// create a new GetRequest
            /// </summary>
            /// <param name="response">the response we are reading data out of</param>
            /// <param name="length">the number of bytes to read</param>
            public WebHdfsReader(HttpWebResponse response, long length, int byteBufferSize) : base(byteBufferSize)
            {
                this.response = response;
                this.responseStream = this.response.GetResponseStream();

                this.bytesToRead = length;
                if (this.bytesToRead == 0)
                {
                    // there's nothing to read, so immediately close the request
                    this.Dispose();
                }
            }

            /// <summary>
            /// read the next sequence of bytes from the response stream
            /// </summary>
            /// <param name="buffer">buffer to read data into</param>
            /// <param name="offset">offset in the buffer to place data into</param>
            /// <param name="count">maximum number of bytes to read</param>
            /// <returns>number of bytes read</returns>
            protected override int ReadInternal(byte[] buffer, int offset, int count)
            {
                int toRead = (int)Math.Min(this.bytesToRead, (long)count);
                if (toRead == 0)
                {
                    return 0;
                }

                int nRead = this.responseStream.Read(buffer, offset, toRead);
                if (nRead == 0)
                {
                    // we reached the end of the stream, because it was shorter than we realized
                    this.bytesToRead = 0;
                }

                this.bytesToRead -= nRead;
                if (this.bytesToRead == 0)
                {
                    // eagerly close the response stream once there is nothing more to read
                    this.Close();
                }

                return nRead;
            }


            /// <summary>
            /// close the stream: any further attempts to read will throw an exception
            /// </summary>
            public override void Close()
            {
                if (this.response != null)
                {
                    this.responseStream.Dispose();
                    this.response.Close();
                    this.responseStream = null;
                    this.response = null;
                }
            }
        }

        private class WebHdfsWriter : DfsStreamWriter
        {
            /// <summary>
            /// the web request, to be closed when we are done
            /// </summary>
            private HttpWebRequest request;
            /// <summary>
            /// the stream to write data to
            /// </summary>
            private Stream requestStream;

            /// <summary>
            /// the status code to expect once we close the request. For a CREATE this is HttpStatusCode.Created, for an
            /// APPEND it is HttpStatusCode.Ok
            /// </summary>
            private readonly HttpStatusCode desiredStatusCode;

            public override void Flush()
            {
                requestStream.Flush();
            }

            protected override void WriteInternal(byte[] buffer, int offset, int count)
            {
                this.requestStream.Write(buffer, offset, count);
            }

            public override void Close()
            {
                if (this.request != null)
                {
                    // first signal to the httpwebrequest that we have finished uploading bytes
                    this.requestStream.Dispose();

                    // now go get the response from the server and check the status code is correct
                    using (HttpWebResponse stored = request.GetResponse() as HttpWebResponse)
                    {
                        if (stored.StatusCode != this.desiredStatusCode)
                        {
                            throw new ApplicationException("Failed to upload HDFS file");
                        }
                    }

                    this.requestStream = null;
                    this.request = null;
                }
            }

            /// <summary>
            /// create a WriterStream
            /// </summary>
            /// <param name="request">web request to write data into</param>
            /// <param name="desiredStatusCode">status code that a successful request will return. this is
            /// HttpStatusCode.Created for CREATE and HttpStatusCode.Ok for APPEND</param>
            public WebHdfsWriter(HttpWebRequest request, HttpStatusCode desiredStatusCode)
            {
                this.request = request;
                this.requestStream = this.request.GetRequestStream();
                this.desiredStatusCode = desiredStatusCode;
            }
        }
        #endregion

        #region private member variables
        /// <summary>
        /// the hdfs user, or null to select a default user
        /// </summary>
        private readonly string user;
        /// <summary>
        /// the ip port that the namenode uses for webhdfs
        /// </summary>
        private readonly int webHdfsPort;
        #endregion

        #region constructor
        /// <summary>
        /// create a webhdfs client
        /// </summary>
        /// <param name="user">the hdfs user, or null for the default user</param>
        /// <param name="nameNode">the name node for the file system</param>
        /// <param name="port">the port used for the java protocol</param>
        /// <param name="webPort">the port used for the webhdfs protocol</param>
        /// <param name="readBufferSize">the size of the buffer used for byte-by-byte data reads</param>
        public WebHdfsClient(string user, int webPort)
        {
            // the base class looks as the environment to try to guess a default user
            this.user = GetDfsUser(user);
            this.webHdfsPort = webPort;
        }
        #endregion

        #region private helper methods
        /// <summary>
        /// construct a webhdfs uri
        /// </summary>
        /// <param name="uri">path of the file or directory being operated on</param>
        /// <param name="op">operation to perform</param>
        /// <returns>webhdfs uri for the operation</returns>
        private Uri MakeRESTUri(Uri uri, string op)
        {
            if (uri.Scheme.ToLower() == "hdfs")
            {
                UriBuilder builder = new UriBuilder(uri);
                builder.Scheme = "http";
                builder.Port = this.webHdfsPort;
                uri = builder.Uri;
            }

            return MakeRESTUri(uri, op, uri.Host, uri.Port);
        }

        /// <summary>
        /// construct a webhdfs uri for a specific node and port
        /// </summary>
        /// <param name="uri">path of the file or directory being operated on</param>
        /// <param name="op">operation to perform</param>
        /// <param name="host">host to contact</param>
        /// <param name="port">port to use</param>
        /// <returns>webhdfs uri for the operation</returns>
        private Uri MakeRESTUri(Uri uri, string op, string host, int port)
        {
            UriBuilder builder = new UriBuilder();
            builder.Host = host;
            if (port > 0)
            {
                builder.Port = port;
            }
            builder.Path = "webhdfs/v1/" + uri.AbsolutePath.TrimStart('/');

            if (user == null)
            {
                builder.Query = "op=" + op;
            }
            else
            {
                builder.Query = "user.name=" + user + "&op=" + op;
            }

            return builder.Uri;
        }

        /// <summary>
        /// perform a webhdfs GET request to the namenode, and parse the result as a JSON object
        /// </summary>
        /// <param name="uri">path of the file or directory to query</param>
        /// <param name="op">query operation to perform</param>
        /// <returns>query result as a JSON object</returns>
        private JObject GetJSon(Uri requestUri, string op)
        {
            // make the query URI
            Uri uri = MakeRESTUri(requestUri, op);

            HttpWebRequest request = HttpWebRequest.Create(uri) as HttpWebRequest;
            using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
            {
                using (Stream stream = response.GetResponseStream())
                {
                    using (StreamReader reader = new StreamReader(stream, Encoding.UTF8, true, 64 * 1024))
                    {
                        string jsonText = reader.ReadToEnd();
                        return JObject.Parse(jsonText);
                    }
                }
            }
        }

        /// <summary>
        /// Send a web request for a region of a file, directing it to a specific datanode if the correct one is known
        /// </summary>
        /// <param name="dataNode">the hostname of the datanode, or null if it is unknown</param>
        /// <param name="port">the port of the datanode, or -1 if it is unknown</param>
        /// <param name="requestUri">the file to read</param>
        /// <param name="offset">the offset to start reading data</param>
        /// <param name="length">the amount of data to read, which must not extend past the end of the file</param>
        /// <returns>a GetRequest wrapper to read bytes sequentially from the response</returns>
        public override Stream GetDfsStreamReader(Uri requestUri, long offset, long length, int byteBufferSize, IPEndPoint dataNode)
        {
            Uri uri;
            if (dataNode == null)
            {
                // make a normal webhdfs request that goes to the namenode and is redirected to an appropriate datanode
                uri = MakeRESTUri(requestUri, "OPEN&offset=" + offset + "&length=" + length);
            }
            else
            {
                string nameNodeAndPort = requestUri.Host;
                if (requestUri.Port > 0)
                {
                    nameNodeAndPort += ":" + requestUri.Port;
                }
                // make a webhdfs request directly to the datanode
                uri = MakeRESTUri(requestUri,
                    "OPEN&namenoderpcaddress=" + nameNodeAndPort +
                    "&offset=" + offset + "&length=" + length, dataNode.Address.ToString(), dataNode.Port);
            }

            // create the web request
            HttpWebRequest request = HttpWebRequest.Create(uri) as HttpWebRequest;
            // get the response, and wrap it in a GetRequest to return it
            return new WebHdfsReader(request.GetResponse() as HttpWebResponse, length, byteBufferSize);
        }

        /// <summary>
        /// create a directory. this will fail if the parent directories don't already exist, hence the public
        /// EnsureDirectory method
        /// </summary>
        /// <param name="directory">directory to create</param>
        private void MakeDirectory(Uri directory, bool worldWritable)
        {
            string op = "MKDIRS&permission=" + ((worldWritable) ? "0777" : "0775");
            Uri uri = MakeRESTUri(directory, op);

            HttpWebRequest request = HttpWebRequest.Create(uri) as HttpWebRequest;
            request.Method = "PUT";
            using (Stream rs = request.GetRequestStream())
            {
                // no data
            }
            using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
            {
                // no response
            }
        }
        #endregion

        public override void Rename(Uri dstPath, Uri srcPath)
        {
            Uri uri = MakeRESTUri(srcPath, "RENAME&destination=/" + dstPath.AbsolutePath);

            HttpWebRequest request = HttpWebRequest.Create(uri) as HttpWebRequest;
            request.Method = "PUT";
            using (Stream rs = request.GetRequestStream())
            {
                // no data
            }
            using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
            {
                using (Stream data = response.GetResponseStream())
                {
                    using (StreamReader sr = new StreamReader(data))
                    {
                        string jsonTxt = sr.ReadToEnd();

                        try
                        {
                            JObject json = JObject.Parse(jsonTxt);
                            string value = (string)json["boolean"];

                            if (value == "true")
                            {
                                return;
                            }
                            else
                            {
                                throw new ApplicationException("Rename failed: " + value);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new ApplicationException("Unable to parse WebHdfs response", e);
                        }
                    }
                }
            }
        }

        public override void DeleteDfsFile(Uri dfsPath, bool recursive = false)
        {
            string op = (recursive) ? "DELETE&recursive=true" : "DELETE";
            Uri uri = MakeRESTUri(dfsPath, op);

            HttpWebRequest request = HttpWebRequest.Create(uri) as HttpWebRequest;
            request.Method = "DELETE";
            using (Stream rs = request.GetRequestStream())
            {
                // no data
            }
            using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
            {
                // no response
            }
        }

        public override void Dispose()
        {
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

                MakeDirectory(dfsDir, worldWritable);
            }
        }

        public override IEnumerable<Uri> ExpandFileOrDirectory(Uri dfsPath)
        {
            JObject json;

            try
            {
                json = GetJSon(dfsPath, "LISTSTATUS");
            }
            catch (WebException e)
            {
                var resp = e.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    return new Uri[] { };
                }
                else
                {
                    throw e;
                }
            }

            try
            {
                JToken statuses = json["FileStatuses"]["FileStatus"];
                return statuses.Select(s => Combine(dfsPath, (string)s["pathSuffix"]));
            }
            catch (Exception e)
            {
                throw new ApplicationException("Failed to parse LISTSTATUS", e);
            }
        }

        public override IEnumerable<Uri> EnumerateSubdirectories(Uri dfsPath)
        {
            JObject json;

            try
            {
                json = GetJSon(dfsPath, "LISTSTATUS");
            }
            catch (WebException e)
            {
                var resp = e.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }
                else
                {
                    throw e;
                }
            }

            try
            {
                JToken statuses = json["FileStatuses"]["FileStatus"];
                return statuses.Where(s => (string)s["type"] == "DIRECTORY").Select(s => Combine(dfsPath, (string)s["pathSuffix"]));
            }
            catch (Exception e)
            {
                throw new ApplicationException("Failed to parse LISTSTATUS", e);
            }
        }

        public override Stream GetDfsStreamReader(Uri dfsPath, long offset, long length, int byteBufferSize)
        {
            return GetDfsStreamReader(dfsPath, offset, length, byteBufferSize, null);
        }

        public override Stream GetDfsStreamWriter(Uri dfsPath)
        {
            return GetDfsStreamWriter(dfsPath, -1);
        }

        public Stream GetDfsStreamWriter(Uri dfsPath, long blockSize)
        {
            string op = "CREATE";
            if (blockSize > 0)
            {
                op += "&blockSize=" + blockSize;
            }
            Uri uri = MakeRESTUri(dfsPath, op);

            // the webhdfs documentation specifies this dual request instead of using auto-redirects, because something
            // in the standard Hadoop stack doesn't understand redirection
            HttpWebRequest request = HttpWebRequest.Create(uri) as HttpWebRequest;
            request.AllowAutoRedirect = false;
            request.Method = "PUT";
            using (Stream rs = request.GetRequestStream())
            {
                // no data
            }
            using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
            {
                if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
                {
                    // the response header tells us the real URI to put the data, by writing directly to a data node
                    HttpWebRequest put = HttpWebRequest.Create(response.Headers["Location"]) as HttpWebRequest;
                    put.Method = "PUT";

                    // wrap the request in an object that we can use to verify the upload when it completes
                    return new WebHdfsWriter(put, HttpStatusCode.Created);
                }
                else
                {
                    throw new ApplicationException("Unexpected non-redirect to CREATE web request");
                }
            }
        }

        public override void GetDirectoryContentSummary(Uri dfsPath, bool expandBlocks, ref long totalSize, ref int numberOfParts)
        {
            if (expandBlocks)
            {
                long size = 0;
                int nParts = 0;
                foreach (HdfsFile file in ExpandFileOrDirectoryToFile(dfsPath))
                {
                    // HDFS files have blocks of equal size, except for the last block which may not be full
                    long numberOfBlocksInFile = (file.length + file.blockSize - 1) / file.blockSize;
                    size += file.length;
                    nParts += (int)numberOfBlocksInFile;
                }
                totalSize = size;
                numberOfParts = nParts;
            }
            else
            {
                JObject json = GetJSon(dfsPath, "GETCONTENTSUMMARY");

                try
                {
                    JToken summary = json["ContentSummary"];
                    totalSize = (long)summary["length"];
                    numberOfParts = (int)summary["fileCount"];
                }
                catch (Exception e)
                {
                    throw new ApplicationException("Unable to parse WebHdfs reponse.", e);
                }
            }
        }

        public override bool GetFileStatus(Uri dfsPath, out long modificationTime, out long size)
        {
            int blockSize;
            return GetFileStatus(dfsPath, out modificationTime, out size, out blockSize);
        }

        public bool GetFileStatus(Uri dfsPath, out long modificationTime, out long size, out int blockSize)
        {
            modificationTime = -1;
            size = -1;
            blockSize = -1;
            JObject json;

            try
            {
                json = GetJSon(dfsPath, "GETFILESTATUS");
            }
            catch (WebException e)
            {
                var resp = e.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }
                else
                {
                    throw e;
                }
            }

            try
            {
                JToken fs = json["FileStatus"];
                modificationTime = (long)fs["modificationTime"];
                size = (long)fs["length"];
                blockSize = (int)fs["blockSize"];
            }
            catch (Exception e)
            {
                throw new ApplicationException("Unable to parse WebHdfs reponse.", e);
            }

            return true;
        }

        public override bool IsFileExists(Uri dfsPath)
        {
            try
            {
                JObject dummy = this.GetJSon(dfsPath, "GETFILESTATUS");
                return true;
            }
            catch (WebException e)
            {
                var resp = e.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }
                else
                {
                    throw e;
                }
            }
        }

        public override bool IsThreadSafe
        {
            get { return true; }
        }

        /// <summary>
        /// expand a filename or directory recursively into the files contained within it
        /// </summary>
        /// <param name="path">file or directory name</param>
        /// <returns>list of files containing the input file, or files contained within the input directory</returns>
        public override IEnumerable<HdfsFile> ExpandFileOrDirectoryToFile(Uri uri)
        {
            string path = uri.AbsolutePath;

            JObject status = GetJSon(uri, "LISTSTATUS");
            // FileStatuses just contains an array called FileStatus
            foreach (JToken file in status["FileStatuses"]["FileStatus"])
            {
                // pathSuffix is combined with the incoming path to make the leaf name
                Uri entry = Combine(uri, (string)file["pathSuffix"]);

                string type = (string)file["type"];
                if (type == "FILE")
                {

                    yield return new HdfsFile
                    {
                        path = entry,
                        length = (long)file["length"],
                        blockSize = (long)file["blockSize"]
                    };
                }
                else if (type == "DIRECTORY")
                {
                    // recursively descend into the directory
                    foreach (HdfsFile subEntry in ExpandFileOrDirectoryToFile(entry))
                    {
                        yield return subEntry;
                    }
                }
                else
                {
                    // not sure what to do about this. Haven't been able to test on a real system since
                    // none of the HDFS clusters I have access to support symlinks
                    throw new NotImplementedException("Traversing symlinks not implemented");
                }
            }
        }

        /// <summary>
        /// determine the datanode locations of each block in a file
        /// </summary>
        /// <param name="file">the description of the file</param>
        /// <returns>a list of datanode IP address/port descriptions, one for each block</returns>
        public override IEnumerable<IPEndPoint[]> GetBlockLocations(HdfsFile file)
        {
            // HDFS files have blocks of equal size, except for the last block which may not be full
            long numberOfBlocksToRead = (file.length + file.blockSize - 1) / file.blockSize;
            // number of blocks to query at a time from the datanode; this limits the size of each http request
            // and the memory consumed by the JSON response
            long batchSizeToRead = file.blockSize * 4096L;

            // keep track of the offset within the file of the current batch of blocks
            long offset = 0;
            while (numberOfBlocksToRead > 0)
            {
                // make the operation request for this batch of blocks
                string op = "GET_BLOCK_LOCATIONS&offset=" + offset + "&length=" + batchSizeToRead;

                // and update the offset ready to read the next batch
                offset += batchSizeToRead;

                // get the details of this batch
                JObject json = GetJSon(file.path, op);
                JToken blocks = json["LocatedBlocks"]["locatedBlocks"];
                foreach (JToken block in blocks)
                {
                    // look up the ip address and webhdfs port of each location
                    yield return block["locations"]
                        .Select(l => new IPEndPoint(IPAddress.Parse((string)l["ipAddr"]), (int)l["infoPort"])).ToArray();
                    --numberOfBlocksToRead;
                }
            }
        }
    }
}
