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
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Microsoft.Research.Peloponnese.YarnMapLauncher
{
    internal class Resources
    {
        private class Resource
        {
            /// <summary>
            /// the leaf name of the remote file
            /// </summary>
            public Uri remoteName;
            /// <summary>
            /// the timestamp of the file, needed by YARN to reference a resource
            /// </summary>
            public long timeStamp;
            /// <summary>
            /// the size of the file, 
            /// </summary>
            public long size;
            /// <summary>
            /// whether or not the resource is shared between users
            /// </summary>
            public bool isPublic;
        }

        private Dictionary<string, Resource> resources;

        public Resources()
        {
            resources = new Dictionary<string, Resource>();
        }

        public void Add(Uri hdfsName, string localName, long timeStamp, long size, bool isPublic)
        {
            Resource existing;
            if (resources.TryGetValue(localName, out existing))
            {
                string existingLeaf = existing.remoteName.AbsolutePath.Split('/').Last();
                string newLeaf = hdfsName.AbsolutePath.Split('/').Last();
                if (existingLeaf == newLeaf)
                {
                    // this is a duplicate, so ignore it
                    return;
                }

                throw new ApplicationException("Duplicate local name " + localName + " bound to " + hdfsName + " and " + existing.remoteName);
            }

            var resource = new Resource();
            resource.remoteName = hdfsName;
            resource.timeStamp = timeStamp;
            resource.size = size;
            resource.isPublic = isPublic;
            resources.Add(localName, resource);
        }

        public string GetAsArguments()
        {
            var sb = new StringBuilder();

            foreach (var r in resources)
            {
                if (r.Value.isPublic)
                {
                    sb.Append(" --public ");
                }
                else
                {
                    sb.Append(" --private ");
                }
                sb.Append(r.Key);
                sb.Append(" ");
                sb.Append(r.Value.remoteName);
                sb.Append(" ");
                sb.Append(r.Value.size);
                sb.Append(" ");
                sb.Append(r.Value.timeStamp);
            }

            return sb.ToString();
        }
    }

    class Server
    {
        //private ILogger logger;
        private string jarFile;
        private string yarnDirectory;

        public Server(string jF)
        {
            jarFile = jF;

            yarnDirectory = Environment.GetEnvironmentVariable("HADOOP_COMMON_HOME");

            if (yarnDirectory == null)
            {
                throw new ApplicationException("No HADOOP_COMMON_HOME defined");
            }
        }

        private async Task<string> CopyStreamWithCatch(StreamReader src, string prefix)
        {
            string lastString = null;
            try
            {
                while (true)
                {
                    string line = await src.ReadLineAsync();
                    if (line == null)
                    {
                        return lastString;
                    }

                    lastString = line;
                    Console.Error.WriteLine(prefix + line);
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Got exception for " + prefix + ": " + e.ToString());
                return null;
            }
        }

        internal string Launch(XDocument config)
        {
            var resources = new Resources();

            var serverElement = config.Descendants("PeloponneseLauncher").Single();

            var applicationName = serverElement.Descendants("ApplicationName").Single().Value;
            applicationName = applicationName.Replace(@"""", @"\""");
            applicationName = @"""" + applicationName + @"""";

            var queueName = serverElement.Descendants("QueueName").Single().Value;

            var amMemoryElem = serverElement.Descendants("AmMemory").SingleOrDefault();
            string amMemory = "-1";
            if (amMemoryElem != null)
            {
                amMemory = amMemoryElem.Value;
            }

            var cmdLine = serverElement.Descendants("CommandLine").Single().Value;

            var resourcesElement = serverElement.Descendants("Resources").Single();
            foreach (var rg in resourcesElement.Descendants("ResourceGroup").Where(x => x.Attribute("type").Value == "hdfs"))
            {
                var remoteDirectory = new Uri(rg.Attribute("location").Value);
                var isPublic = (rg.Attribute("public") != null && rg.Attribute("public").Value == "true");

                foreach (var r in rg.Descendants("Resource"))
                {
                    string localName;
                    var lnAttribute = r.Attribute("localName");
                    if (lnAttribute == null)
                    {
                        localName = r.Value;
                    }
                    else
                    {
                        localName = lnAttribute.Value;
                    }

                    var remoteUri = new Uri(remoteDirectory, r.Value);
                    var timeStamp = long.Parse(r.Attribute("timestamp").Value);
                    var size = long.Parse(r.Attribute("size").Value);

                    resources.Add(remoteUri, localName, timeStamp, size, isPublic);
                }
            }

            string args = "";

            string resourceArgs = resources.GetAsArguments();
            string uniqueName = Guid.NewGuid().ToString();

            Console.Error.WriteLine("About to write resources to filename " + uniqueName);
            Console.Error.WriteLine("Resources: " + resourceArgs);

            File.WriteAllText(uniqueName, resourceArgs);

            args = args + " --argfile " + uniqueName;

            Console.Error.WriteLine("About to fork process");

            ProcessStartInfo psi = new ProcessStartInfo();

            psi.FileName = Path.Combine(yarnDirectory, "bin", "yarn.cmd");
            psi.Arguments = string.Format(@"jar {0} {1} {2} {3} {4} {5}", jarFile, applicationName, queueName, amMemory, cmdLine, args);
            
            Console.Error.WriteLine("Process filename " + psi.FileName);
            Console.Error.WriteLine("Process args " + psi.Arguments);

            //logger.Log("Yarn launcher arguments: " + psi.Arguments);

            psi.UseShellExecute = false;
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
            var process = Process.Start(psi);

            Task<string> waitStdOut = CopyStreamWithCatch(process.StandardOutput, "OUT: ");
            Task<string> waitStdErr = CopyStreamWithCatch(process.StandardError, "ERR: ");

            process.WaitForExit();

            string lastOut = waitStdOut.Result;
            string lastErr = waitStdErr.Result;

            Console.Error.WriteLine("Process exited " + process.ExitCode + " " + lastOut + " " + lastErr);

            if (process.ExitCode == 0)
            {
                return lastOut;
            }
            else
            {
                throw new ApplicationException("Launch command failed");
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.Error.WriteLine("Streaming launcher started");
            //var exeName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
            //var runningDirectory = Path.GetDirectoryName(exeName);

            var jarFile = Path.Combine(Environment.CurrentDirectory, "Microsoft.Research.Peloponnese.YarnLauncher.jar");
            Console.Error.WriteLine("Streaming launcher using jar file " + jarFile);
            var server = new Server(jarFile);

            string id = null;
            try
            {
                Console.Error.WriteLine("Streaming launcher reading stdin");
                var config = XDocument.Load(Console.In);
                Console.Error.WriteLine("Streaming launcher launching");
                id = server.Launch(config);
                if (id == null)
                {
                    throw new ApplicationException("Did not receive application id from job submission.");
                }
                Console.Error.WriteLine("Successfully submitted job with application id: {0}", id);
                Console.WriteLine(id);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Connection got exception " + e.ToString());
                Environment.Exit(1);
            }
            
        }
    }
}
