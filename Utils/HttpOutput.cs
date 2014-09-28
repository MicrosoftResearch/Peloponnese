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
    internal interface NHOutputWriter
    {
        Task WriteAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken);
        Task FlushAsync(System.Threading.CancellationToken cancellationToken);
        Task WriteFinal(System.Threading.CancellationToken cancellationToken);
     }

    internal class NHOutputStream : Stream
    {
        private NHOutputWriter writer;

        public NHOutputStream(NHOutputWriter w)
        {
            writer = w;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            return writer.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            WriteAsync(buffer, offset, count).Wait();
        }

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

        public override Task FlushAsync(System.Threading.CancellationToken cancellationToken)
        {
            return writer.FlushAsync(cancellationToken);
        }

        public override void Flush()
        {
            FlushAsync().Wait();
        }

        public Task CloseAsync(System.Threading.CancellationToken cancellationToken)
        {
            return writer.WriteFinal(cancellationToken);
        }

        public override void Close()
        {
            CloseAsync(System.Threading.CancellationToken.None).Wait();
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
    }

    internal abstract class NHOutputHandler : NHOutputWriter, IDisposable
    {
        private static readonly byte[] final = System.Text.Encoding.UTF8.GetBytes("0\r\n\r\n");
        private static readonly byte[] trailer = System.Text.Encoding.UTF8.GetBytes("\r\n");

        private readonly ILogger logger;

        private WebHeaderCollection headers;
        private bool hasPayload;

        private NHOutputStream outputStream;
        private Stream socketStream;
        private bool closed;

        public NHOutputHandler(Stream sStream, ILogger l)
        {
            logger = l;
            headers = new WebHeaderCollection();
            hasPayload = true;
            socketStream = sStream;
            outputStream = new NHOutputStream(this);
            closed = false;
        }

        protected ILogger Logger { get { return logger; } }

        protected void SetStream(Stream sStream)
        {
            socketStream = sStream;
        }

        protected void SuppressPayload()
        {
            hasPayload = false;
        }

        protected bool SentHeaders { get { return (headers == null); } }

        public WebHeaderCollection HeadersInternal { get { return headers; } }

        public Stream OutputStream
        {
            get { return outputStream; }
        }

        protected abstract Task<byte[]> GetPreamble();
        protected abstract void StreamFaulted(Exception exception);
        protected abstract void ReportOutputCompleted();

        public void Abandon()
        {
            if (socketStream != null)
            {
                socketStream = null;
                ReportOutputCompleted();
            }
            outputStream = null;
            closed = true;
        }

        private void DealWithError(Exception exception)
        {
            Abandon();
            StreamFaulted(exception);
        }

        private async Task MaybeStartSending(System.Threading.CancellationToken cancellationToken)
        {
            if (headers != null)
            {
                // first null out headers so if there's an exception during the write we don't try doing
                // it again
                WebHeaderCollection headerCopy = headers;
                headers = null;

                byte[] preamble = await GetPreamble();
                await socketStream.WriteAsync(preamble, 0, preamble.Length, cancellationToken);

                if (hasPayload)
                {
                    headerCopy[HttpResponseHeader.TransferEncoding] = "chunked";
                }
                else
                {
                    headerCopy[HttpResponseHeader.ContentLength] = "0";
                }
                byte[] headerBytes = headerCopy.ToByteArray();
                await socketStream.WriteAsync(headerBytes, 0, headerBytes.Length, cancellationToken);

                if (!hasPayload)
                {
                    Abandon();
                }
            }
        }

        public async Task WriteAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            if (closed)
            {
                throw new ApplicationException("Writing to closed stream");
            }

            try
            {
                await MaybeStartSending(cancellationToken);

                if (!hasPayload)
                {
                    throw new ApplicationException("Can't write payload");
                }

                if (count > 0)
                {
                    string chunkLength = count.ToString("X") + "\r\n";
                    byte[] chunkHeader = System.Text.Encoding.UTF8.GetBytes(chunkLength);
                    await socketStream.WriteAsync(chunkHeader, 0, chunkHeader.Length, cancellationToken);
                    await socketStream.WriteAsync(buffer, offset, count, cancellationToken);
                    await socketStream.WriteAsync(trailer, 0, trailer.Length, cancellationToken);
                }
            }
            catch (Exception e)
            {
                DealWithError(e);
                throw e;
            }
        }

        public async Task FlushAsync(System.Threading.CancellationToken cancellationToken)
        {
            if (closed)
            {
                // already closed
                return;
            }

            try
            {
                // if no output was written, we haven't sent the headers so do it now
                await MaybeStartSending(cancellationToken);
                if (socketStream != null)
                {
                    await socketStream.FlushAsync(cancellationToken);
                }
            }
            catch (Exception e)
            {
                DealWithError(e);
                throw e;
            }
        }

        public async Task WriteFinal(System.Threading.CancellationToken cancellationToken)
        {
            if (closed)
            {
                // already closed
                return;
            }

            try
            {
                // if no output was written, we haven't sent the headers so do it now
                await MaybeStartSending(cancellationToken);
                // close the response
                if (socketStream != null)
                {
                    await socketStream.WriteAsync(final, 0, final.Length, cancellationToken);
                    await socketStream.FlushAsync(cancellationToken);
                }

                Abandon();
            }
            catch (Exception e)
            {
                DealWithError(e);
                throw e;
            }
        }

        public async Task CloseAsync()
        {
            if (closed)
            {
                // already closed
                return;
            }

            try
            {
                // this calls WriteFinal which calls Abandon which closes the stream
                await outputStream.CloseAsync(System.Threading.CancellationToken.None);
            }
            catch (Exception e)
            {
                DealWithError(e);
                throw e;
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
                CloseAsync().Wait();
            }
        }
    }
}
