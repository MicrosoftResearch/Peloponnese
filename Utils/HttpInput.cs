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
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.NotHttp
{
    internal interface NHInputReader
    {
        Task<int> ReadAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken);
        void Close();
    }

    internal class NHError
    {
        public HttpStatusCode status;
        public string message;

        public NHError()
        {
            status = HttpStatusCode.OK;
            message = "";
        }
    }

    internal class NHInputStream : Stream
    {
        private NHInputReader reader;

        public NHInputStream(NHInputReader r)
        {
            reader = r;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            return reader.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer, offset, count).Result;
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

        public override void Close()
        {
            reader.Close();
            base.Close();
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

        public override void Write(byte[] buffer, int offset, int count)
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

        public override void Flush()
        {
            throw new NotImplementedException();
        }
    }

    internal abstract class NHInputHandler : NHInputReader
    {
        private readonly ILogger logger;
        protected ILogger Logger { get { return logger; } }

        private readonly WebHeaderCollection headers;
        private readonly byte[] chunkAuxBytes;
        private readonly TaskCompletionSource<bool> closedTask;

        private long length;
        private long currentChunkRemaining;

        private Stream inputStream;
        private Stream socketStream;

        public NHInputHandler(ILogger l)
        {
            headers = new WebHeaderCollection();
            logger = l;
            inputStream = new NHInputStream(this);
            chunkAuxBytes = new byte[256];
            closedTask = new TaskCompletionSource<bool>();
        }

        public NameValueCollection Headers { get { return headers; } }
        internal WebHeaderCollection HeadersInternal { get { return headers; } }
        internal Task Completed { get { return closedTask.Task; } }

        protected abstract bool ParsePreamble(Uri serverAddress, StreamReader stream, NHError error);
        protected abstract Task<bool> MaybeContinue(Stream request, NHError error);

        private async Task<MemoryStream> FetchHeaders(Stream request)
        {
            MemoryStream headerDump = new MemoryStream();
            int currentLineLength = 0;
            bool seenCr = false;
            byte[] buffer = new byte[1];
            while (true)
            {
                int nRead = await request.ReadAsync(buffer, 0, 1);
                if (nRead == 0)
                {
                    if (headerDump.Position == 0)
                    {
                        return null;
                    }
                    throw new ApplicationException("Stream ended before reading header block");
                }

                ++currentLineLength;
                headerDump.WriteByte(buffer[0]);

                if (buffer[0] == '\n')
                {
                    if (seenCr && currentLineLength == 2)
                    {
                        headerDump.SetLength(headerDump.Position);
                        headerDump.Seek(0, SeekOrigin.Begin);
                        return headerDump;
                    }

                    currentLineLength = 0;
                }

                seenCr = (buffer[0] == '\r');
            }
        }

        internal async Task<bool> Parse(Uri serverAddress, Stream request, NHError error)
        {
            bool needsInput;

            try
            {
                // the following parsing methods return false if they read the stream, but didn't like the request,
                // in which case they keep reading to the end. If there is a parsing error that means the stream is
                // now unsynchronized, they throw an exception
                MemoryStream headerStream = await FetchHeaders(request);
                if (headerStream == null || headerStream.Length == 0)
                {
                    closedTask.SetResult(false);
                    return false;
                }

                using (StreamReader reader = new StreamReader(headerStream, Encoding.UTF8, false, 1024, true))
                {
                    needsInput = ParsePreamble(serverAddress, reader, error);

                    ParseHeaders(reader, error);
                }

                if (needsInput)
                {
                    needsInput = await MaybeContinue(request, error);
                }
            }
            catch (Exception e)
            {
                closedTask.SetResult(false);
                throw e;
            }

            if (needsInput) 
            {
                if (headers[HttpRequestHeader.TransferEncoding] == "chunked")
                {
                    length = -1;
                    currentChunkRemaining = -1;
                }
                else
                {
                    length = long.Parse(headers[HttpRequestHeader.ContentLength]);
                    currentChunkRemaining = length;
                }

                socketStream = request;
            }
            else
            {
                length = 0;
                currentChunkRemaining = 0;
                closedTask.SetResult(true);
            }

            return true;
        }

        private void AddHeader(string headerLine, NHError error)
        {
            int colonPos = headerLine.IndexOf(':');
            if (colonPos == -1)
            {
                throw new ApplicationException("Bad header field " + headerLine);
            }

            string key = headerLine.Substring(0, colonPos).Trim();
            string value = headerLine.Substring(colonPos + 1).Trim();

            try
            {
                headers.Add(key, value);
            }
            catch (Exception e)
            {
                logger.Log("Parsing header line " + headerLine + " got exception " + e.ToString());
                throw new ApplicationException("Bad header field " + headerLine + " threw exception", e);
            }
        }

        private void ParseHeaders(StreamReader reader, NHError error)
        {
            string currentHeader = null;
            while (true)
            {
                string headerLine = reader.ReadLine();
                if (headerLine.Length == 0)
                {
                    if (currentHeader == null)
                    {
                        throw new ApplicationException("No headers");
                    }

                    AddHeader(currentHeader, error);

                    return;
                }

                if (headerLine.StartsWith(" ") || headerLine.StartsWith("\t"))
                {
                    if (currentHeader == null)
                    {
                        throw new ApplicationException("Continuation header without preceding header");
                    }

                    currentHeader = currentHeader + headerLine;
                }
                else
                {
                    if (currentHeader != null)
                    {
                        AddHeader(currentHeader, error);
                    }
                    currentHeader = headerLine;
                }
            }
        }

        private async Task ReadNextChunkSize(System.Threading.CancellationToken cancellationToken)
        {
            int nRead = 0;
            while (nRead < chunkAuxBytes.Length)
            {
                int gotByte = await socketStream.ReadAsync(chunkAuxBytes, nRead, 1, cancellationToken);
                if (gotByte != 1)
                {
                    throw new ApplicationException("Failed to read all chunk preamble bytes");
                }
                ++nRead;

                if (chunkAuxBytes[nRead - 1] == '\n')
                {
                    if (nRead < 3 || chunkAuxBytes[nRead - 2] != '\r')
                    {
                        throw new ApplicationException("Bad chunk preamble bytes");
                    }

                    string countString = System.Text.Encoding.UTF8.GetString(chunkAuxBytes, 0, nRead - 2);
                    if (!long.TryParse(countString,
                                       System.Globalization.NumberStyles.AllowHexSpecifier,
                                       System.Globalization.NumberFormatInfo.InvariantInfo,
                                       out currentChunkRemaining) ||
                        currentChunkRemaining < 0)
                    {
                        throw new ApplicationException("Bad chunk preamble string " + countString);
                    }

                    if (currentChunkRemaining == 0)
                    {
                        await SkipChunkTrailer(cancellationToken);
                    }

                    return;
                }
            }

            throw new ApplicationException("Chunk preamble bytes too long without newline");
        }

        private async Task SkipChunkTrailer(System.Threading.CancellationToken cancellationToken)
        {
            bool gotHeader;
            do
            {
                gotHeader = false;
                bool readingLine = true;
                while (readingLine)
                {
                    int nRead = await socketStream.ReadAsync(chunkAuxBytes, 0, 1, cancellationToken);
                    if (nRead == 0)
                    {
                        throw new ApplicationException("Failed to read all chunking trailer bytes");
                    }

                    if (chunkAuxBytes[0] == '\r')
                    {
                        nRead = await socketStream.ReadAsync(chunkAuxBytes, 0, 1, cancellationToken);
                        if (nRead == 0)
                        {
                            throw new ApplicationException("Failed to read all chunking trailer bytes");
                        }

                        if (chunkAuxBytes[0] == '\n')
                        {
                            readingLine = false;
                        }
                        else
                        {
                            throw new ApplicationException("Got bad chunking trailer terminator");
                        }
                    }
                    else
                    {
                        gotHeader = true;
                    }
                }
            } while (gotHeader);
        }

        private async Task ConsumeChunkPostamble(System.Threading.CancellationToken cancellationToken)
        {
            int postBytes = 2;
            while (postBytes > 0)
            {
                int npr = await socketStream.ReadAsync(chunkAuxBytes, 0, postBytes, cancellationToken);
                if (npr == 0)
                {
                    throw new ApplicationException("Failed to read all chunk postamble bytes");
                }
                postBytes -= npr;
            }
            if (chunkAuxBytes[0] != '\r' || chunkAuxBytes[1] != '\n')
            {
                throw new ApplicationException("Got malformed chunk postamble bytes");
            }
        }

        public async Task<int> ReadAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            if (currentChunkRemaining < 0)
            {
                await ReadNextChunkSize(cancellationToken);
            }

            if (currentChunkRemaining == 0)
            {
                // we have read everything
                return 0;
            }

            int toRead = (int)Math.Min(currentChunkRemaining, (long)count);
            int nRead = await socketStream.ReadAsync(buffer, offset, toRead, cancellationToken);
            currentChunkRemaining -= nRead;

            if (currentChunkRemaining == 0 && length < 0)
            {
                await ConsumeChunkPostamble(cancellationToken);
                currentChunkRemaining = -1;
            }

            return nRead;
        }

        public async Task CloseAsync()
        {
            while (currentChunkRemaining != 0)
            {
                byte[] buffer = new byte[64 * 1024];
                int nRead = await ReadAsync(buffer, 0, buffer.Length, System.Threading.CancellationToken.None);
            }

            bool mustSetTask = false;
            lock (this)
            {
                if (socketStream != null)
                {
                    mustSetTask = true;
                    socketStream = null;
                }
            }

            if (mustSetTask)
            {
                closedTask.SetResult(true);
            }
        }

        public void Close()
        {
            CloseAsync().Wait();
        }

        public void Abandon()
        {
            // allow closing the request without throwing an exception when the stream has gone away
            bool mustSetTask = false;
            lock (this)
            {
                currentChunkRemaining = 0;
                if (socketStream != null)
                {
                    mustSetTask = true;
                    socketStream = null;
                }
            }

            if (mustSetTask)
            {
                closedTask.SetResult(true);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
        }

        public Encoding ContentEncoding
        {
            get { return Encoding.UTF8; }
        }

        public Stream InputStream
        {
            get { return inputStream; }
        }

        internal async Task BufferInput()
        {
            MemoryStream memoryStream = new MemoryStream();

            lock (this)
            {
                if (socketStream == null)
                {
                    inputStream = memoryStream;
                    return;
                }
            }

            await inputStream.CopyToAsync(memoryStream);
            memoryStream.SetLength(memoryStream.Position);
            memoryStream.Seek(0, SeekOrigin.Begin);
            inputStream = memoryStream;
            socketStream = null;
            closedTask.SetResult(true);
        }
    }
}
