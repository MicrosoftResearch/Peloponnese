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
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.NotHttpClient
{
    internal class NHClientRequest : NotHttp.NHOutputHandler, IHttpRequest
    {
        private readonly NotHttpClient client;
        private NotHttpConnection connection;
        private readonly Uri url;
        private string method;
        private int timeout;
        private int sequenceNumber;
        bool isOverlapped;
        bool gotRequestStream;

        public NHClientRequest(Uri uri, NotHttpClient c, bool isO, ILogger logger) : base(null, logger)
        {
            url = uri;
            client = c;
            method = "GET";
            timeout = System.Threading.Timeout.Infinite;
            sequenceNumber = -1;
            isOverlapped = isO;
            gotRequestStream = false;
        }

        public NameValueCollection Headers { get { return HeadersInternal; } }

        public Uri Url
        {
            get { return url; }
        }

        public int Timeout { set { timeout = value; } }

        public string Method
        {
            get
            {
                if (method == null)
                {
                    throw new ApplicationException("Can't get Method after writing any request data");
                }
                return method;
            }
            set
            {
                if (method == null)
                {
                    throw new ApplicationException("Can't set Method after writing any request data");
                }
                if (value == null)
                {
                    throw new ArgumentNullException();
                }
                method = value;
            }
        }

        public Stream GetRequestStream()
        {
            gotRequestStream = true;
            return OutputStream;
        }

        protected override void StreamFaulted(Exception exception)
        {
            if (connection != null)
            {
                connection.StreamFaulted(exception);
            }
        }

        protected override async Task<byte[]> GetPreamble()
        {
            if (url == null || method == null)
            {
                throw new ApplicationException("Must set URL before writing request data");
            }

            connection = await client.GetRequestConnection(this);

            SetStream(connection.Stream);

            string requestString;

            sequenceNumber = connection.NextSequenceNumber(this);
            if (isOverlapped)
            {
                requestString = String.Format("{0} {1} NOTHTTP/1.1 {2}\r\n", method, url.PathAndQuery, sequenceNumber);
            }
            else
            {
                requestString = String.Format("{0} {1} HTTP/1.1\r\n", method, url.PathAndQuery);
            }

            if (method != "POST" && method != "PUT")
            {
                SuppressPayload();
            }

            method = null;

            return System.Text.Encoding.UTF8.GetBytes(requestString);
        }

        protected override void ReportOutputCompleted()
        {
            if (connection != null)
            {
                connection.ReportRequestCompleted(this);
            }
        }

        public Task<IHttpResponse> GetResponseAsync()
        {
            if (!gotRequestStream)
            {
                using (Stream emptyRequest = GetRequestStream())
                {
                    // send an empty request
                }
            }

            if (method != null)
            {
                throw new ApplicationException("Can't get response before sending request");
            }

            return connection.WaitForResponse(sequenceNumber, timeout);
        }

        public IHttpResponse GetResponse()
        {
            return GetResponseAsync().Result;
        }
    }

    internal class NHClientResponse : NotHttp.NHInputHandler, IHttpResponse
    {
        private readonly NotHttpConnection connection;
        private int sequenceNumber;
        private HttpStatusCode status;
        private string statusDescription;

        internal NHClientResponse(NotHttpConnection c, int sn, ILogger logger) : base(logger)
        {
            connection = c;
            sequenceNumber = sn;
        }

        public HttpStatusCode StatusCode { get { return status; } }
        public string StatusDescription { get { return statusDescription; } }

        public Stream GetResponseStream()
        {
            return InputStream;
        }

        protected override bool ParsePreamble(Uri serverAddress, StreamReader reader, NotHttp.NHError error)
        {
            string header = reader.ReadLine();

            string[] parts = header.Split();

            if (parts.Length < 3)
            {
                throw new ApplicationException("Bad header string " + header);
            }

            int statusPart = 1;
            if (parts[0] == "NOTHTTP/1.1")
            {
                if (sequenceNumber != -1)
                {
                    throw new ApplicationException("Got overlapped response on sequential connection");
                }
                sequenceNumber = int.Parse(parts[1]);
                if (sequenceNumber < 0 || parts.Length < 4)
                {
                    throw new ApplicationException("Bad header string " + header);
                }
                statusPart = 2;
            }
            else if (parts[0] == "HTTP/1.1")
            {
                if (sequenceNumber == -1)
                {
                    throw new ApplicationException("Got sequential response on overlapped connection");
                }
            }
            else
            {
                throw new ApplicationException("Bad header string " + header);
            }

            status = (HttpStatusCode)int.Parse(parts[statusPart]);
            StringBuilder sb = new StringBuilder(parts[statusPart + 1]);
            for (int i=statusPart+2; i<parts.Length; ++i)
            {
                sb.Append(" ");
                sb.Append(parts[i]);
            }
            statusDescription = sb.ToString();

            return true;
        }

        protected override Task<bool> MaybeContinue(Stream request, NotHttp.NHError error)
        {
            return Task.FromResult<bool>(true);
        }

        internal int SequenceNumber { get { return sequenceNumber; } }
    }

    internal class NotHttpConnection
    {
        private readonly NHClientEndpoint endpoint;
        private readonly Dictionary<int, NHClientResponse> receivedResponses;
        private readonly Dictionary<int, TaskCompletionSource<IHttpResponse>> pendingClients;

        private Stream stream;
        private Task streamReaderFinished;
        private int sequenceNumber;
        private int outstandingRequests;
        private NHClientRequest currentRequest;
        private TaskCompletionSource<bool> idleTask;
        private Exception exception;

        public NotHttpConnection(NHClientEndpoint ep)
        {
            endpoint = ep;
            receivedResponses = new Dictionary<int, NHClientResponse>();
            pendingClients = new Dictionary<int, TaskCompletionSource<IHttpResponse>>();
            sequenceNumber = 0;
            outstandingRequests = 0;
            idleTask = new TaskCompletionSource<bool>();
        }

        public Stream Stream {
            get {
                lock (this)
                {
                    if (stream == null)
                    {
                        throw new ApplicationException("Connection is faulted", exception);
                    }

                    return stream;
                }
            }
        }

        public async Task Open(string hostName, int port)
        {
            if (hostName == Environment.MachineName)
            {
                hostName = "localhost";
            }

            try
            {
                Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, true);
                await Task.Factory.FromAsync(socket.BeginConnect(hostName, port, null, null), socket.EndConnect);
                stream = new NetworkStream(socket, true);
                endpoint.Logger.Log("Opened connection to " + hostName + ":" + port + " from " + socket.LocalEndPoint);
            }
            catch (Exception e)
            {
                endpoint.Logger.Log("Got exception opening stream: " + e.ToString());
                exception = e;
                return;
            }

            streamReaderFinished = ReadStream();
        }

        private void ThrowTCSExceptions<T>(IEnumerable<TaskCompletionSource<T>> completions, Exception e)
        {
            foreach (TaskCompletionSource<T> c in completions)
            {
                c.SetException(e);
            }
        }

        private void ReturnResult(TaskCompletionSource<IHttpResponse> caller, IHttpResponse response)
        {
            if ((int)response.StatusCode >= 400)
            {
                endpoint.Logger.Log("Passing http exception status " + response.StatusCode);
                if (!caller.TrySetException(new NotHttpException(response)))
                {
                    endpoint.Logger.Log("Ignoring exceptional response that has already timed out");
                }
            }
            else
            {
                if (!caller.TrySetResult(response))
                {
                    endpoint.Logger.Log("Ignoring response that has already timed out");
                }
            }
        }

        private Task<IHttpResponse> ReturnResult(IHttpResponse response)
        {
            if ((int)response.StatusCode >= 400)
            {
                TaskCompletionSource<IHttpResponse> tcs = new TaskCompletionSource<IHttpResponse>();
                ReturnResult(tcs, response);
                return tcs.Task;
            }
            else
            {
                return Task.FromResult(response);
            }
        }

        public Task WaitForExit()
        {
            lock (this)
            {
                StreamFaultedInternal(new ApplicationException("Client closed connection"));
            }

            return streamReaderFinished;
        }

        private void ExitOnIdle(TaskCompletionSource<bool> oldIdleTask)
        {
            lock (this)
            {
                if (oldIdleTask.Task.Status == TaskStatus.WaitingForActivation)
                {
                    if (idleTask != oldIdleTask || outstandingRequests > 0)
                    {
                        endpoint.Logger.Log("Error: idle connection mismatch");
                        throw new ApplicationException("Idle task mismatch");
                    }

                    // the idle delay passed without us being assigned a new request, so we
                    // will shut down
                    StreamFaultedInternal(new ApplicationException("Client closed idle connection"));
                }
            }
        }

        // called with lock(this)
        private void StreamFaultedInternal(Exception e)
        {
            if (stream == null)
            {
                endpoint.Logger.Log("Already-faulted stream: " + e.ToString());
                if (pendingClients.Count > 0)
                {
                    endpoint.Logger.Log("Error: pending clients should be empty");
                    throw new ApplicationException("Pending clients should be empty");
                }
            }
            else
            {
                endpoint.Logger.Log("Discarding faulted stream: " + e.ToString());
                try
                {
                    stream.Dispose();
                }
                catch (Exception ce)
                {
                    endpoint.Logger.Log("Exception disposing stream: " + ce.ToString());
                }

                stream = null;
                exception = e;

                List<TaskCompletionSource<IHttpResponse>> toThrow = new List<TaskCompletionSource<IHttpResponse>>();
                foreach (var p in pendingClients)
                {
                    toThrow.Add(p.Value);
                }
                pendingClients.Clear();

                // make this run asynchronously outside the lock
                Task.Run(() => ThrowTCSExceptions(toThrow, e));

                if (outstandingRequests == 0)
                {
                    idleTask.SetResult(true);
                }
            }
        }

        public void StreamFaulted(Exception e)
        {
            lock (this)
            {
                if (stream == null)
                {
                    // already faulted
                    return;
                }

                StreamFaultedInternal(e);

                if (!endpoint.IsOverlapped)
                {
                    currentRequest = null;
                }
            }

            if (!endpoint.IsOverlapped)
            {
                // take ourselves out of the endpoint's list of available connections; ReportConnectionReady
                // checks to see if we are faulted. For an overlapped connection this will have happened in
                // ReportRequestCompleted already
                endpoint.ReportConnectionReady(this);
            }
        }

        public Exception AssignRequest(NHClientRequest request)
        {
            lock (this)
            {
                if (currentRequest != null)
                {
                    endpoint.Logger.Log("Error: callers out of sync");
                    throw new ApplicationException("Callers out of sync");
                }

                // if exception is non-null the connection is faulted, and
                // will be discarded by the caller when this method returns
                if (exception == null)
                {
                    currentRequest = request;
                    if (outstandingRequests == 0)
                    {
                        idleTask.SetResult(false);
                    }
                    ++outstandingRequests;
                }

                return exception;
            }
        }

        public int NextSequenceNumber(NHClientRequest caller)
        {
            lock (this)
            {
                if (caller != currentRequest)
                {
                    endpoint.Logger.Log("Error: callers out of sync");
                    throw new ApplicationException("Callers out of sync");
                }

                // I don't believe in foo++ syntax obfuscation :)
                int sn = sequenceNumber;
                ++sequenceNumber;
                return sn;
            }
        }

        public void ReportRequestCompleted(NHClientRequest caller)
        {
            lock (this)
            {
                if (caller != currentRequest)
                {
                    endpoint.Logger.Log("Error: callers out of sync");
                    throw new ApplicationException("Callers out of sync");
                }

                if (endpoint.IsOverlapped)
                {
                    currentRequest = null;
                }
            }

            if (endpoint.IsOverlapped)
            {
                endpoint.ReportConnectionReady(this);
            }
        }

        private async Task ReadStream()
        {
            int responseSequenceNumber = -1;
 
            while (true)
            {
                Task<bool> idleBlocker = null;
                lock (this)
                {
                    if (outstandingRequests == 0)
                    {
                        // add ContinueWith so the continuation is asynchronous, since await generates
                        // a synchronous continuation
                        idleBlocker = idleTask.Task.ContinueWith((t) => t.Result);
                    }
                }

                if (idleBlocker != null)
                {
                    bool mustExit = await idleBlocker;
                    if (mustExit)
                    {
                        endpoint.Logger.Log("Reader exiting due to idle connection");
                        return;
                    }
                }

                if (!endpoint.IsOverlapped)
                {
                    // sequence number is 0 for the first response
                    ++responseSequenceNumber;
                }

                // for an overlapped connection we pass in responseSequenceNumber=-1 and the actual sequence
                // number is read from the protocol. For a sequential connection we pass in the actual sequence
                // number
                NHClientResponse response = new NHClientResponse(this, responseSequenceNumber, endpoint.Logger);
                try
                {
                    NotHttp.NHError error = new NotHttp.NHError();
                    bool gotRequest = await response.Parse(null, stream, error);
                    if (!gotRequest)
                    {
                        throw new ApplicationException("Server closed connection");
                    }

                    if (response.StatusCode != HttpStatusCode.OK || endpoint.IsOverlapped)
                    {
                        // fetch the whole response now, so we're ready to wait for the next message even if
                        // the caller isn't ready to read this one
                        await response.BufferInput();
                    }

                    lock (this)
                    {
                        TaskCompletionSource<IHttpResponse> tcs;
                        if (pendingClients.TryGetValue(response.SequenceNumber, out tcs))
                        {
                            // run this asynchronously outside the lock
                            Task abandon = Task.Run(() => ReturnResult(tcs, response));
                            pendingClients.Remove(response.SequenceNumber);
                        }
                        else
                        {
                            if (receivedResponses.ContainsKey(response.SequenceNumber))
                            {
                                StreamFaulted(new ApplicationException("Received duplicate sequence numbers " + response.SequenceNumber));
                                return;
                            }

                            receivedResponses.Add(response.SequenceNumber, response);
                        }
                    }

                    // wait until the caller has consumed all the payload data (if we buffered it above this
                    // falls through immediately)
                    await response.Completed;

                    string closeHeader = response.HeadersInternal[HttpRequestHeader.Connection];
                    if (closeHeader != null && closeHeader.ToLower() == "close")
                    {
                        endpoint.Logger.Log("Passing stream connection closed header");
                        StreamFaulted(new ApplicationException("Connection closed by server request"));
                        return;
                    }

                    lock (this)
                    {
                        if (!endpoint.IsOverlapped)
                        {
                            currentRequest = null;
                        }

                        --outstandingRequests;
                        if (outstandingRequests == 0)
                        {
                            idleTask = new TaskCompletionSource<bool>();
                            // capture member variable in local scope to give to closure
                            TaskCompletionSource<bool> currentIdleTask = idleTask;
                            Task abandon = Task.Delay(endpoint.IdleTimeout).ContinueWith((t) => ExitOnIdle(currentIdleTask));
                        }
                    }

                    if (!endpoint.IsOverlapped)
                    {
                        endpoint.ReportConnectionReady(this);
                    }
                }
                catch (Exception e)
                {
                    endpoint.Logger.Log("Passing exception status " + response.StatusCode);
                    StreamFaulted(e);
                    return;
                }
            }
        }

        private void TimeOutRequest(int responseSequenceNumber)
        {
            lock (this)
            {
                if (pendingClients.ContainsKey(responseSequenceNumber))
                {
                    // the request timed out before it was delivered
                    endpoint.Logger.Log("Request " + responseSequenceNumber + " timed out");
                    StreamFaulted(new ApplicationException("A request timed out"));
                }
            }
        }

        internal Task<IHttpResponse> WaitForResponse(int responseSequenceNumber, int timeout)
        {
            lock (this)
            {
                NHClientResponse response;
                if (receivedResponses.TryGetValue(responseSequenceNumber, out response))
                {
                    receivedResponses.Remove(responseSequenceNumber);
                    return ReturnResult(response);
                }

                if (stream == null)
                {
                    if (exception == null)
                    {
                        endpoint.Logger.Log("Error: Can't wait for a response before connecting");
                        throw new ApplicationException("Can't wait for a response before connecting");
                    }
                    throw exception;
                }

                if (pendingClients.ContainsKey(responseSequenceNumber))
                {
                    endpoint.Logger.Log("Error: Can't wait for sequence number " + responseSequenceNumber + " more than once");
                    throw new ApplicationException("Can't wait for sequence number " + responseSequenceNumber + " more than once");
                }

                TaskCompletionSource<IHttpResponse> tcs = new TaskCompletionSource<IHttpResponse>();
                pendingClients.Add(responseSequenceNumber, tcs);

                if (timeout != System.Threading.Timeout.Infinite)
                {
                    Task.Delay(timeout).ContinueWith((t) => TimeOutRequest(responseSequenceNumber));
                }

                return tcs.Task;
            }
        }
    }

    internal class NHClientEndpoint
    {
        private struct RequestWaiter
        {
            public readonly NHClientRequest request;
            public readonly TaskCompletionSource<NotHttpConnection> task;

            public RequestWaiter(NHClientRequest caller)
            {
                request = caller;
                task = new TaskCompletionSource<NotHttpConnection>();
            }
        }

        private readonly ILogger logger;
        private readonly string hostName;
        private readonly int port;
        private readonly bool isOverlapped;
        private readonly int maxConnections;
        private readonly LinkedList<RequestWaiter> requestWaiters;
        private readonly Stack<NotHttpConnection> idleConnections;
        private readonly int idleTimeout;
        private int openedConnections;

        public NHClientEndpoint(string host, int p, bool isO, int maxC, int idleT, ILogger l)
        {
            logger = l;
            hostName = host;
            port = p;
            isOverlapped = isO;
            maxConnections = maxC;
            requestWaiters = new LinkedList<RequestWaiter>();
            idleConnections = new Stack<NotHttpConnection>();
            idleTimeout = idleT;
            openedConnections = 0;
        }

        public ILogger Logger { get { return logger; } }
        public int IdleTimeout { get { return idleTimeout; } }
        public bool IsOverlapped { get { return isOverlapped; } }

        // Called with lock(this)
        private void MarkConnectionReady(NotHttpConnection connection, bool sacrificeWaiter)
        {
            if (requestWaiters.Count == 0)
            {
                idleConnections.Push(connection);
            }
            else
            {
                RequestWaiter waiter = requestWaiters.First();
                Exception exception = connection.AssignRequest(waiter.request);
                if (exception == null)
                {
                    requestWaiters.RemoveFirst();
                    // Run this asynchronously outside the lock
                    Task.Run(() => waiter.task.SetResult(connection));
                }
                else
                {
                    if (sacrificeWaiter)
                    {
                        // give the waiter the connection even if it's faulted.
                        // we do this on a failed open so that the client will see
                        // the error rather than just perpetually retrying the open
                        requestWaiters.RemoveFirst();
                        // do this asynchronously outside the lock
                        Task.Run(() => waiter.task.SetException(new ApplicationException("Stream open failure", exception)));
                    }

                    --openedConnections;
                    logger.Log("Removing faulted connection, " + openedConnections + " remaining: " + exception.Message);

                    ReOpenConnections();
                }
            }
        }

        private void ReOpenConnections()
        {
            // try to reopen as many as there are queued requests, within our budget for
            // maximum connections
            int desiredPendingOpens = Math.Min(requestWaiters.Count, maxConnections);
            while (openedConnections < desiredPendingOpens)
            {
                ++openedConnections;
                Task abandoned = Task.Run(() => OpenConnection());
            }
        }

        private async void OpenConnection()
        {
            logger.Log("Opening connection to " + hostName + ":" + port);
            NotHttpConnection connection = new NotHttpConnection(this);
            await connection.Open(hostName, port);

            lock (this)
            {
                MarkConnectionReady(connection, true);
            }
        }

        public async Task<NotHttpConnection> WaitForRequestSlot(NHClientRequest caller)
        {
            Task<NotHttpConnection> blockTask = null;

            lock (this)
            {
                // this is where we garbage collection connections that are faulted, so
                // go through the idle connections throwing them away if they
                // aren't good until we match one
                while (blockTask == null && idleConnections.Count > 0)
                {
                    NotHttpConnection connection = idleConnections.Pop();
                    Exception exception = connection.AssignRequest(caller);
                    if (exception == null)
                    {
                        blockTask = Task.FromResult(connection);
                    }
                    else
                    {
                        --openedConnections;
                        logger.Log("Removing faulted connection, " + openedConnections +" remaining: " + exception.Message);
                    }
                }

                if (blockTask == null)
                {
                    RequestWaiter waiter = new RequestWaiter(caller);
                    requestWaiters.AddLast(waiter);
                    blockTask = waiter.task.Task;
                }

                ReOpenConnections();
            }

            return await blockTask;
        }

        public void ReportConnectionReady(NotHttpConnection connection)
        {
            lock (this)
            {
                MarkConnectionReady(connection, false);
            }
        }
    }

    public class NotHttpException : Exception
    {
        public readonly IHttpResponse Response;

        public NotHttpException(IHttpResponse r) : base(MakeDescription(r))
        {
            Response = r;
        }

        private static string MakeDescription(IHttpResponse r)
        {
            return "Http request returned " + (int)r.StatusCode + ": " + r.StatusDescription;
        }
    }

    public class NotHttpClient
    {
        private readonly ILogger logger;
        private readonly bool isOverlapped;
        private readonly int maxConnections;
        private readonly int idleTimeout;
        private readonly Dictionary<string, Dictionary<int, NHClientEndpoint>> connections;

        public NotHttpClient(bool isO, int maxC, int idleT, ILogger l)
        {
            logger = l;
            isOverlapped = isO;
            maxConnections = maxC;
            idleTimeout = idleT;
            connections = new Dictionary<string, Dictionary<int, NHClientEndpoint>>();
        }

        internal async Task<NotHttpConnection> GetRequestConnection(NHClientRequest caller)
        {
            NHClientEndpoint endpoint;
            Task<NotHttpConnection> blockTask;
            lock (this)
            {
                Uri url = caller.Url;

                Dictionary<int, NHClientEndpoint> hostPorts;
                if (!connections.TryGetValue(url.Host, out hostPorts))
                {
                    hostPorts = new Dictionary<int, NHClientEndpoint>();
                    connections.Add(url.Host, hostPorts);
                }

                if (!hostPorts.TryGetValue(url.Port, out endpoint))
                {
                    endpoint = new NHClientEndpoint(url.Host, url.Port, isOverlapped, maxConnections, idleTimeout, logger);
                    hostPorts.Add(url.Port, endpoint);
                }

                blockTask = endpoint.WaitForRequestSlot(caller);
            }

            return await blockTask;
        }

        public IHttpRequest CreateRequest(string address)
        {
            return CreateRequest(new Uri(address));
        }

        public IHttpRequest CreateRequest(Uri uri)
        {
            return new NHClientRequest(uri, this, isOverlapped, logger);
        }
    }
}
