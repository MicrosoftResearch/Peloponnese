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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.NotHttpServer
{
    internal class NHServerRequest : NotHttp.NHInputHandler, IHttpRequest
    {
        private static readonly byte[] continueBytes = System.Text.Encoding.UTF8.GetBytes("HTTP/1.1 100 (Continue)\r\n\r\n");

        private readonly NHContext context;
        private Uri url;
        private string method;
        private NameValueCollection queryString;

        public NHServerRequest(NHContext c, ILogger logger) : base(logger)
        {
            context = c;
        }

        protected override bool ParsePreamble(Uri serverAddress, StreamReader reader, NotHttp.NHError error)
        {
            string request = reader.ReadLine();

            string[] parts = request.Split();

            if (parts[2] == "NOTHTTP/1.1")
            {
                context.SequenceNumber = int.Parse(parts[3]);
            }
            else if (parts[2] != "HTTP/1.1")
            {
                throw new ApplicationException("Unknown request string " + request);
            }

            string prefix = serverAddress.AbsolutePath;
            if (parts[1].StartsWith(prefix))
            {
                UriBuilder builder = new UriBuilder(serverAddress);
                int queryPos = parts[1].IndexOf('?');
                if (queryPos == -1)
                {
                    builder.Path = parts[1];
                    queryString = new NameValueCollection();
                }
                else
                {
                    builder.Path = parts[1].Substring(0, queryPos);
                    queryString = System.Web.HttpUtility.ParseQueryString(parts[1].Substring(queryPos+1));
                }

                url = builder.Uri;
            }
            else
            {
                error.status = HttpStatusCode.NotFound;
                error.message = "Bad request URI " + parts[1];
            }

            method = parts[0];

            return (method == "POST" || method == "PUT");
        }

        protected override async Task<bool> MaybeContinue(Stream request, NotHttp.NHError error)
        {
            if (HeadersInternal[HttpRequestHeader.Expect] != null && HeadersInternal[HttpRequestHeader.Expect].StartsWith("100"))
            {
                if (error.status == HttpStatusCode.OK)
                {
                    Logger.Log("Writing continue");
                    await request.WriteAsync(continueBytes, 0, continueBytes.Length);
                    await request.FlushAsync();
                    return true;
                }
                else
                {
                    // skip the payload
                    return false;
                }
            }
            else
            {
                return true;
            }
        }

        public Uri Url
        {
            get { return url; }
        }

        public string HttpMethod
        {
            get { return method; }
        }

        public NameValueCollection QueryString
        {
            get { return queryString; }
        }
    }

    internal class NHServerResponse : NotHttp.NHOutputHandler, IHttpResponse
    {
        private readonly NHContext context;

        private int statusCode;
        private string statusDescription;
        private bool holdingResponseSlot;

        public NHServerResponse(Stream stream, NHContext c, ILogger logger) : base(stream, logger)
        {
            context = c;
            statusCode = -1;
            statusDescription = null;
            holdingResponseSlot = false;
        }

        protected override void StreamFaulted(Exception exception)
        {
            context.Connection.Fault(exception);
        }

        public WebHeaderCollection Headers { get { return HeadersInternal; } }

        public int StatusCode
        {
            set
            {
                if (SentHeaders)
                {
                    throw new ApplicationException("StatusCode cannot be set after response stream is written to");
                }
                statusCode = value;
            }
        }

        public string StatusDescription
        {
            set
            {
                if (SentHeaders)
                {
                    throw new ApplicationException("StatusDescription cannot be set after response stream is written to");
                }
                if (value.Contains('\r') || value.Contains('\n'))
                {
                    throw new ArgumentException("StatusDescription cannot contain a newline character");
                }
                statusDescription = value;
            }
        }

        internal void PrepareForClose()
        {
            if (statusCode < 0 || statusDescription == null)
            {
                statusCode = (int)HttpStatusCode.InternalServerError;
                statusDescription = "Server error";
            }
        }

        protected override async Task<byte[]> GetPreamble()
        {
            if (statusCode < 0 || statusDescription == null)
            {
                throw new ApplicationException("Must set status code and description before writing or closing response");
            }

            string responseString;

            int sequenceNumber = context.SequenceNumber;
            if (sequenceNumber >= 0)
            {
                await context.Connection.WaitForResponseSlot(this);
                holdingResponseSlot = true;
                responseString = String.Format("NOTHTTP/1.1 {0} {1} {2}\r\n", sequenceNumber, statusCode, statusDescription);
            }
            else
            {
                responseString = String.Format("HTTP/1.1 {0} {1}\r\n", statusCode, statusDescription);
            }

            return System.Text.Encoding.UTF8.GetBytes(responseString);
        }

        protected override void ReportOutputCompleted()
        {
            if (holdingResponseSlot)
            {
                holdingResponseSlot = false;
                context.Connection.ReportResponseCompleted(this);
            }
        }
    }

    internal class NHContext : IHttpContext
    {
        private readonly NHServerConnection connection;
        private readonly Stream inputStream;
        private readonly AcceptMethod acceptor;
        private readonly ILogger logger;

        private readonly NHServerRequest request;
        private readonly NHServerResponse response;
        private int sequenceNumber;

        public NHContext(NHServerConnection c, Stream i, AcceptMethod a, ILogger l)
        {
            connection = c;
            inputStream = i;
            acceptor = a;
            logger = l;
            sequenceNumber = -1;
            request = new NHServerRequest(this, logger);
            response = new NHServerResponse(inputStream, this, logger);
        }

        public int SequenceNumber {
            get { return sequenceNumber; }
            internal set { sequenceNumber = value; }
        }

        public NHServerConnection Connection { get { return connection; } }

        private async Task AcceptTask()
        {
            try
            {
                await acceptor(this);

                response.PrepareForClose();

                await Task.WhenAll(request.CloseAsync(), response.CloseAsync());

                connection.DecrementOutstandingRequests();
            }
            catch (Exception e)
            {
                connection.DecrementOutstandingRequests();
                logger.Log("Acceptor method threw exception " + e.ToString());
                Abandon();
            }
        }

        private async Task<bool> Accept()
        {
            if (sequenceNumber >= 0)
            {
                if (connection.SetState(true))
                {
                    // get all the input we're going to, so we can start listening for the next request
                    try
                    {
                        await request.BufferInput();
                    }
                    catch (Exception e)
                    {
                        logger.Log("Buffer input got exception " + e.ToString());
                        return false;
                    }

                    connection.IncrementOutstandingRequests();

                    // we are doing overlapped responses so start the acceptor and return immediately
                    Task acceptorTask = Task.Run(() => AcceptTask());
                    return true;
                }
                else
                {
                    logger.Log("Server got NOTHTTP request on HTTP connection");
                    return false;
                }
            }
            else
            {
                if (connection.SetState(false))
                {
                    connection.IncrementOutstandingRequests();

                    // we are doing sequenced responses so block until the acceptor completes before returning
                    await AcceptTask();
                    return true;
                }
                else
                {
                    logger.Log("Server got HTTP request on NOTHTTP connection");
                    return false;
                }
            }
        }

        public async Task<bool> HandleRequest(Uri serverAddress)
        {
            bool gotRequest;
            NotHttp.NHError error = new NotHttp.NHError();
            try
            {
                gotRequest = await request.Parse(serverAddress, inputStream, error);
                if (gotRequest == false)
                {
                    // the connection was closed cleanly
                    return false;
                }
            }
            catch (Exception e)
            {
                // stream error parsing request; close connection without trying to send a response
                logger.Log("Server got exception parsing request: " + e.ToString());
                return false;
            }

            if (error.status == HttpStatusCode.OK)
            {
                return await Accept();
            }
            else
            {
                response.StatusCode = (int)error.status;
                response.StatusDescription = error.message;

                // make sure we consume all the client's input as well as sending the bad response
                await Task.WhenAll(response.CloseAsync(), request.CloseAsync());

                // keep the connection open if we managed to send a civilized error response
                return true;
            }
        }

        public IHttpRequest Request
        {
            get { return request; }
        }

        public IHttpResponse Response
        {
            get { return response; }
        }

        public void Abandon()
        {
            request.Abandon();
            response.Abandon();
        }
    }

    internal class NHServerConnection
    {
        private enum State
        {
            Unknown, Sequential, Overlapped
        }

        private readonly NetworkStream stream;
        private readonly EndPoint networkEndpoint;
        private readonly Uri serverAddress;
        private readonly AcceptMethod acceptor;
        private readonly ILogger logger;
        private readonly LinkedList<Task<NHServerResponse>> responseWaiters;

        private State state;
        private bool faulted;
        private int outstandingRequests;
        private TaskCompletionSource<bool> finished;

        public NHServerConnection(NetworkStream s, EndPoint remoteEndpoint, Uri sA, AcceptMethod a, ILogger l)
        {
            stream = s;
            networkEndpoint = remoteEndpoint;
            serverAddress = sA;
            acceptor = a;
            logger = l;

            state = State.Unknown;
            responseWaiters = new LinkedList<Task<NHServerResponse>>();
            faulted = false;
            outstandingRequests = 0;
            finished = null;

            logger.Log("Got connection from " + remoteEndpoint);
        }

        public void Fault(Exception exception)
        {
            lock (this)
            {
                if (!faulted)
                {
                    logger.Log("Faulting stream for " + networkEndpoint + ": " + exception.ToString());
                    faulted = true;
                    stream.Dispose();
                }
            }
        }

        public bool SetState(bool isOverlapped)
        {
            if (isOverlapped)
            {
                if (state == State.Sequential)
                {
                    // we were previously set to Sequential, so there's an error
                    return false;
                }
                else
                {
                    // the previous state was either Unknown or Overlapped
                    state = State.Overlapped;
                    return true;
                }
            }
            else
            {
                if (state == State.Overlapped)
                {
                    // we were previously set to Overlapped, so there's an error
                    return false;
                }
                else
                {
                    // the previous state was either Unknown or Sequential
                    state = State.Sequential;
                    return true;
                }
            }
        }

        public void IncrementOutstandingRequests()
        {
            lock (this)
            {
                ++outstandingRequests;
            }
        }

        public void DecrementOutstandingRequests()
        {
            lock (this)
            {
                --outstandingRequests;
                if (outstandingRequests == 0 && finished != null)
                {
                    finished.SetResult(true);
                }
            }
        }

        public async Task Run()
        {
            bool stillReading = true;
            do
            {
                NHContext context = new NHContext(this, stream, acceptor, logger);

                stillReading = await context.HandleRequest(serverAddress);
                if (!stillReading)
                {
                    context.Abandon();
                }
            } while (stillReading);

            logger.Log("Connection waiting for responses, " + networkEndpoint);

            lock (this)
            {
                finished = new TaskCompletionSource<bool>();
                if (outstandingRequests == 0)
                {
                    finished.SetResult(true);
                }
            }

            await finished.Task;

            logger.Log("Connection exiting for " + networkEndpoint);
        }

        public async Task WaitForResponseSlot(NHServerResponse caller)
        {
            Task<NHServerResponse> blockTask;

            lock (this)
            {
                if (faulted)
                {
                    throw new ApplicationException("Response stream faulted");
                }

                if (responseWaiters.Count == 0)
                {
                    // there's nobody to wait for
                    blockTask = null;
                }
                else
                {
                    // wait behind the current tail of the queue
                    blockTask = responseWaiters.Last();
                }

                responseWaiters.AddLast(new Task<NHServerResponse>(() => { return caller; }));
            }

            if (blockTask != null)
            {
                await blockTask;
            }

            lock (this)
            {
                if (faulted)
                {
                    throw new ApplicationException("Response stream faulted");
                }
            }
        }

        public void ReportResponseCompleted(NHServerResponse caller)
        {
            lock (this)
            {
                Task<NHServerResponse> completedTask = responseWaiters.First();
                responseWaiters.RemoveFirst();
                // wake up whomever is blocked
                completedTask.Start();
                if (completedTask.Result != caller)
                {
                    throw new ApplicationException("Mismatched responses");
                }
            }
        }
    }

    internal class NHServer : IHttpServer
    {
        private ILogger logger;
        private Socket[] sockets;
        private Uri serverAddress;

        public NHServer(ILogger l)
        {
            logger = l;
        }

        async void HandleConnection(Socket connection, AcceptMethod acceptor)
        {
            try
            {
                using (NetworkStream stream = new NetworkStream(connection, true))
                {
                    NHServerConnection connectionHandler =
                        new NHServerConnection(stream, connection.RemoteEndPoint, serverAddress, acceptor, logger);
                    await connectionHandler.Run();
                }
            }
            catch (Exception e)
            {
                logger.Log("Got exception " + e.ToString());
                // the connection went away
            }
        }

        void AcceptAll(Socket socket, AcceptMethod acceptor)
        {
            try
            {
                while (true)
                {
                    Socket connection = socket.Accept();
                    Task.Run(() => HandleConnection(connection, acceptor));
                }
            }
            catch (Exception e)
            {
                // the server was stopped
                logger.Log("Caught exception " + e.Message);
                return;
            }
        }

        bool Initialize(IPAddress address, int basePort, string prefix)
        {
            IPAddress[] addresses;

            if (address.Equals(IPAddress.Any))
            {
                // find all the IPv4 interface addresses to listen on
                addresses = System.Net.NetworkInformation.NetworkInterface
                    .GetAllNetworkInterfaces()
                    .Where(iface => iface.Supports(System.Net.NetworkInformation.NetworkInterfaceComponent.IPv4) &&
                                    iface.OperationalStatus == System.Net.NetworkInformation.OperationalStatus.Up)
                    .SelectMany(iface => iface.GetIPProperties()
                                              .UnicastAddresses
                                              .Select(addressProps => addressProps.Address)
                                              .Where(addr => addr.AddressFamily == AddressFamily.InterNetwork))
                    .ToArray();
            }
            else
            {
                addresses = new IPAddress[] { address };
            }

            for (int i = 0; i < 100; ++i)
            {
                int port = basePort + i;

                sockets = new Socket[addresses.Length];

                try
                {
                    for (int j=0; j<addresses.Length; ++j)
                    {
                        IPAddress addr = addresses[j];

                        logger.Log("NotHttp server trying to listen on " + addr.ToString() + ":" + port);

                        sockets[j] = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        sockets[j].SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, true);
                        IPEndPoint ep = new IPEndPoint(addr, port);
                        sockets[j].Bind(ep);
                        sockets[j].Listen(100);
                    }

                    if (address == IPAddress.Any)
                    {
                        logger.Log("Listening on all interfaces");
                        UriBuilder builder = new UriBuilder("http", Environment.MachineName, port, prefix);
                        serverAddress = builder.Uri;
                    }
                    else
                    {
                        UriBuilder builder = new UriBuilder("http", address.ToString(), port, prefix);
                        serverAddress = builder.Uri;
                    }

                    logger.Log("NotHttp server listening on " + serverAddress.AbsoluteUri);

                    return true;
                }
                catch (SocketException se)
                {
                    Stop();
                    if (se.ErrorCode == 10013 || se.ErrorCode == 10048)
                    {
                        // access denied: try the next available port
                        logger.Log("NotHttp server got error code " + se.ErrorCode + "; trying next port");
                    }
                    else
                    {
                        logger.Log("NotHttp server got exception " + se.ToString());
                        return false;
                    }
                }
                catch (Exception e)
                {
                    Stop();
                    logger.Log("NotHttp server got exception " + e.ToString());
                    return false;
                }
            }

            return false;
        }

        public void Start(AcceptMethod acceptor)
        {
            logger.Log("Starting server");
            foreach (Socket socket in sockets)
            {
                Task.Run(() => AcceptAll(socket, acceptor));
            }
        }

        public Uri Address
        {
            get { return serverAddress; }
        }

        public void Stop()
        {
            foreach (Socket socket in sockets)
            {
                if (socket != null)
                {
                    logger.Log("Closing socket " + socket.LocalEndPoint);
                    socket.Dispose();
                    logger.Log("Closed socket");
                }
            }

            sockets = null;
        }

        public static IHttpServer Create(IPAddress address, int port, string prefix, ILogger logger)
        {
            NHServer server = new NHServer(logger);
            if (server.Initialize(address, port, prefix))
            {
                return server;
            }
            else
            {
                return null;
            }
        }
    }
}
