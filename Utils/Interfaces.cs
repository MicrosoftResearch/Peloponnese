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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese
{
    /// <summary>
    /// this is the connection to the application's logging interface, supplied
    /// by the external application
    /// </summary>
    public interface ILogger
    {
        void Log(
            string entry,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1);
        void Stop();
    }

    public class Utils
    {
        public static string CmdLineEncode(string param)
        {
            return param
                .Replace("_", "_u")
                .Replace("=", "_e")
                .Replace("&", "_a")
                .Replace(">", "_g")
                .Replace("<", "_l")
                .Replace("%", "_p")
                .Replace("?", "_q");
        }

        public static string CmdLineDecode(string param)
        {
            return param
                .Replace("_q", "?")
                .Replace("_p", "%")
                .Replace("_l", "<")
                .Replace("_g", ">")
                .Replace("_a", "&")
                .Replace("_e", "=")
                .Replace("_u", "_");
        }

        /// <summary>
        /// Waits for a task to complete, and catches its exception if any
        /// </summary>
        /// <param name="task">The task to wait for</param>
        /// <param name="logger">If non-null, a logger to write the exception to</param>
        public static void DefuseTask(Task task, ILogger logger)
        {
            task.ContinueWith(async (t) =>
                {
                    try
                    {
                        await t;
                    }
                    catch (Exception e)
                    {
                        if (logger != null)
                        {
                            logger.Log("Caught exception from discarded task: " + e.ToString());
                        }
                    }
                });
        }

        /// <summary>
        /// A variant of WhenAny that can be used when some of the tasks can throw exceptions. The first
        /// task to complete is returned, as usual, and the remainder are wrapped so that any thrown
        /// exceptions are safely caught.
        /// </summary>
        /// <param name="logger">If non-null, any exceptions are logged to logger</param>
        /// <param name="tasks">The tasks to wait for</param>
        /// <returns></returns>
        public static async Task<Task> SafeWhenAny(ILogger logger, params Task[] tasks)
        {
            Task first = await Task.WhenAny(tasks);
            foreach (Task t in tasks)
            {
                if (t != first)
                {
                    DefuseTask(t, logger);
                }
            }
            return first;
        }

        /// <summary>
        /// A variant of WhenAny that can be used when some of the tasks can throw exceptions. The first
        /// task to complete is returned, as usual, and the remainder are wrapped so that any thrown
        /// exceptions are safely caught.
        /// </summary>
        /// <param name="logger">If non-null, any exceptions are logged to logger</param>
        /// <param name="tasks">The tasks to wait for</param>
        /// <returns></returns>
        public static async Task<Task<T>> SafeWhenAny<T>(ILogger logger, params Task<T>[] tasks)
        {
            Task<T> first = await Task.WhenAny(tasks);
            foreach (Task<T> t in tasks)
            {
                if (t != first)
                {
                    DefuseTask(t, logger);
                }
            }
            return first;
        }
    }
}

namespace Microsoft.Research.Peloponnese.NotHttpServer
{
    public interface IHttpRequest
    {
        Uri Url { get; }
        string HttpMethod { get; }
        NameValueCollection Headers { get; }
        Encoding ContentEncoding { get; }
        NameValueCollection QueryString { get; }
        Stream InputStream { get; }
    }

    public interface IHttpResponse : IDisposable
    {
        WebHeaderCollection Headers { get; }
        int StatusCode { set; }
        string StatusDescription { set; }
        Stream OutputStream { get; }
        Task CloseAsync();
    }

    public interface IHttpContext
    {
        IHttpRequest Request { get; }
        IHttpResponse Response { get; }
    }

    public delegate Task AcceptMethod(IHttpContext context);

    public interface IHttpServer
    {
        Uri Address { get; }
        void Start(AcceptMethod acceptor);
        void Stop();
    }

    public class Factory
    {
        public static IHttpServer Create(IPAddress address, int port, string prefix, bool requireNotHttp, Microsoft.Research.Peloponnese.ILogger logger)
        {
            if (!requireNotHttp)
            {
                IHttpServer server = SystemWrapper.Create(address, port, prefix, logger);
                if (server != null)
                {
                    return server;
                }
            }

            return NHServer.Create(address, port, prefix, logger);
        }
    }
}

namespace Microsoft.Research.Peloponnese.NotHttpClient
{
    public interface IHttpRequest
    {
        NameValueCollection Headers { get; }
        Uri Url { get; }
        string Method { get; set; }
        int Timeout { set; }
        Stream GetRequestStream();
        IHttpResponse GetResponse();
        Task<IHttpResponse> GetResponseAsync();
    }

    public interface IHttpResponse : IDisposable
    {
        NameValueCollection Headers { get; }
        HttpStatusCode StatusCode { get; }
        string StatusDescription { get; }
        Stream GetResponseStream();
        void Close();
        Task CloseAsync();
    }
}
