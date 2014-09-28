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
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.NotHttpServer
{
    internal class SystemWrapper
    {
        private class SystemRequest : IHttpRequest
        {
            private HttpListenerRequest request;

            public SystemRequest(HttpListenerRequest req)
            {
                request = req;
            }

            public NameValueCollection Headers { get { return request.Headers; } }

            public Uri Url
            {
                get { return request.Url; }
            }

            public string HttpMethod
            {
                get { return request.HttpMethod; }
            }

            public Encoding ContentEncoding
            {
                get { return request.ContentEncoding; }
            }

            public System.Collections.Specialized.NameValueCollection QueryString
            {
                get { return request.QueryString; }
            }

            public System.IO.Stream InputStream
            {
                get { return request.InputStream; }
            }
        }

        private class SystemResponse : IHttpResponse
        {
            private HttpListenerResponse response;

            public SystemResponse(HttpListenerResponse resp)
            {
                response = resp;
            }

            public WebHeaderCollection Headers
            {
                get { return response.Headers; }
            }

            public int StatusCode
            {
                set { response.StatusCode = value; }
            }

            public string StatusDescription
            {
                set { response.StatusDescription = value; }
            }

            public System.IO.Stream OutputStream
            {
                get { return response.OutputStream; }
            }

            public Task CloseAsync()
            {
                response.Close();
                return Task.FromResult(true);
            }

            public void Close()
            {
                response.Close();
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
        }

        private class SystemContext : IHttpContext
        {
            private HttpListenerContext context;

            public SystemContext(HttpListenerContext ctx)
            {
                context = ctx;
            }

            public IHttpRequest Request { get { return new SystemRequest(context.Request); } }
            public IHttpResponse Response { get { return new SystemResponse(context.Response); } }
        }

        private class SystemServer : IHttpServer
        {
            private ILogger logger;
            private HttpListener server;
            private Uri address;
            private AcceptMethod accept;

            public Uri Address
            {
                get { return address; }
            }

            public SystemServer(ILogger l)
            {
                logger = l;
            }

            internal bool Initialize(System.Net.IPAddress ipAddr, int port, string prefix)
            {
                try
                {
                    server = new HttpListener();
                    string listenerAddress = String.Format("http://+:{0}{1}", port, prefix);
                    logger.Log("Trying system http server for " + listenerAddress);
                    server.Prefixes.Add(listenerAddress);
                    server.Start();

                    if (ipAddr == IPAddress.Any)
                    {
                        address = new Uri(String.Format("http://{0}:{1}{2}", Environment.MachineName, port, prefix));
                    }
                    else
                    {
                        address = new Uri(String.Format("http://{0}:{1}{2}", ipAddr.ToString(), port, prefix));
                    }

                    logger.Log("Got system http server for " + address.AbsoluteUri);

                    return true;
                }
                catch (HttpListenerException e)
                {
                    logger.Log("Failed to get system http server: " + e.Message);
                    server = null;
                    return false;
                }
            }

            public void Start(AcceptMethod a)
            {
                accept = a;
                logger.Log("Starting system http server");
                server.GetContextAsync().ContinueWith(x => AcceptContext(x.Result));
            }

            public void Stop()
            {
                logger.Log("Stopping system http server");
                server.Stop();
                logger.Log("Stopped system http server");
            }

            public void AcceptContext(HttpListenerContext ctx)
            {
                server.GetContextAsync().ContinueWith(x => AcceptContext(x.Result));

                try
                {
                    accept(new SystemContext(ctx));
                }
                catch (Exception e)
                {
                    logger.Log("Acceptor threw exception: " + e.ToString());
                }
            }
        }

        public static IHttpServer Create(IPAddress address, int port, string prefix, ILogger l)
        {
            SystemServer server = new SystemServer(l);
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
