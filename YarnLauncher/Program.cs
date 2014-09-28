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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Xml;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese;
using Microsoft.Research.Peloponnese.NotHttpServer;
using Microsoft.Research.Peloponnese.ClusterUtils;
using Microsoft.Research.Peloponnese.Yarn;

namespace Microsoft.Research.Peloponnese.Launcher
{
    class Server
    {
        private ILogger logger;
        private string jarFile;
        private string yarnDirectory;

        public Server(string jF)
        {
            jarFile = jF;

            yarnDirectory = Environment.GetEnvironmentVariable("HADOOP_COMMON_HOME");

            if (yarnDirectory  == null)
            {
                throw new ApplicationException("No HADOOP_COMMON_HOME defined");
            }
        }

        public string Launch(XDocument config)
        {
            return NativeYarnClient.LaunchJob(config, this.jarFile, this.yarnDirectory, this.logger);
        }

        private async Task HandleConnection(IHttpContext context)
        {
            logger.Log("Got web connection");

            IHttpRequest req = context.Request;
            string id = null;
            string error = "";
            try
            {
                if (req.HttpMethod.ToUpper() == "POST")
                {
                    var config = XDocument.Load(req.InputStream);
                    id = this.Launch(config);
                }
            }
            catch (Exception e)
            {
                logger.Log("Connection got exception " + e.ToString());
                error = e.Message;
            }

            try
            {
                if (id == null)
                {
                    HttpStatusCode code = HttpStatusCode.BadRequest;
                    context.Request.InputStream.Close();

                    context.Response.StatusCode = (int)code;
                    context.Response.StatusDescription = code.ToString();
                    using (var sw = new StreamWriter(context.Response.OutputStream))
                    {
                        await sw.WriteAsync(error);
                    }
                    await context.Response.CloseAsync();
                }
                else
                {
                    HttpStatusCode code = HttpStatusCode.OK;
                    context.Request.InputStream.Close();

                    context.Response.StatusCode = (int)code;
                    context.Response.StatusDescription = "OK";
                    using (var sw = new StreamWriter(context.Response.OutputStream))
                    {
                        await sw.WriteAsync(id);
                    }
                    await context.Response.CloseAsync();
                }
            }
            catch (Exception e)
            {
                logger.Log("Exception while sending status " + e.ToString());
            }
        }

        private IPAddress GetIPAddress()
        {
            var choices = Dns.GetHostAddresses(Environment.MachineName);
            var ipv4 = choices.Where(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
            // think about adding support for multiple interfaces later
            return ipv4.First();
        }

        public bool Run(ILogger l, string prefix, int port)
        {
            logger = l;

            IPAddress address = GetIPAddress();
            IHttpServer server = NotHttpServer.Factory.Create(address, port, prefix, false, logger);
            if (server == null)
            {
                logger.Log("Failed to start server at " + address + ":" + port + ":" + prefix);
                return false;
            }
            else
            {
                string serverAddress = server.Address.AbsoluteUri;
                logger.Log("Started local process server at " + serverAddress);

                server.Start(HandleConnection);

                Task.Delay(Timeout.Infinite).Wait();
                return true;
            }

        }
    }

    class Program
    {
        static void Usage(ILogger logger)
        {
            var exeName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
            logger.Log(exeName + " <httpPrefix> <httpPort>");
        }

        static int Main(string[] args)
        {
            ILogger logger = new SimpleLogger("Yarnlauncher.log");

            if (args.Length != 0 && args.Length != 2)
            {
                Usage(logger);
                return 1;
            }

            var exeName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
            var runningDirectory = Path.GetDirectoryName(exeName);
            var jarFile = Path.Combine(runningDirectory, "Microsoft.Research.Peloponnese.YarnLauncher.jar");
            var server = new Server(jarFile);

            if (args.Length == 0) // read the XML document from stdin to launch job
            {
                string id = null;
                string error = "";
                int retVal = 0;
                try
                {
                    var config = XDocument.Load(Console.OpenStandardInput());
                    id = server.Launch(config);
                    if (id == null)
                    {
                        throw new ApplicationException("Did not receive application id from job submission.");
                    }
                    Console.WriteLine(id);
                }
                catch (Exception e)
                {
                    logger.Log("Connection got exception " + e.ToString());
                    error = e.Message;
                    retVal = 1;
                }
                return retVal;
            }
            else // listen on a port for jobs sumbited as xml docs
            {
                var prefix = args[0];
                int port;
                if (!int.TryParse(args[1], out port))
                {
                    Usage(logger);
                    return 1;
                }

                try
                {
                    server.Run(logger, prefix, port);
                }
                catch (Exception e)
                {
                    logger.Log("Unhandled exception: + " + e.ToString());
                }

                return 1;
            }
        }
    }
}
