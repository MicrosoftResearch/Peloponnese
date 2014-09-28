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
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.Shared;
using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.Peloponnese.Yarn
{
    public class NativeYarnClient : ClusterClient
    {
        public class Resources
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

        static public string LaunchJob(XDocument config, string jarFile, string yarnDirectory, ILogger logger)
        {
            var resources = new NativeYarnClient.Resources();

            var serverElement = config.Descendants("PeloponneseLauncher").Single();

            var applicationName = serverElement.Descendants("ApplicationName").Single().Value;
            applicationName = applicationName.Replace(@"""", @"\""");
            applicationName = @"""" + applicationName + @"""";

            var queueName = serverElement.Descendants("QueueName").Single().Value;

            var amMemoryElem = serverElement.Descendants("AmMemory").SingleOrDefault();
            string amMemory = "-1";
            if (amMemoryElem != null)
            {
                amMemory =  amMemoryElem.Value;
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
            File.WriteAllText(uniqueName, resourceArgs);

            args = args + " --argfile " + uniqueName;

            ProcessStartInfo psi = new ProcessStartInfo();

            psi.FileName = Path.Combine(yarnDirectory, "bin", "yarn.cmd");
            psi.Arguments = string.Format(@"jar {0} {1} {2} {3} {4} {5}", jarFile, applicationName, queueName, amMemory, cmdLine, args);

            if (logger != null)
            {
                logger.Log("Yarn launcher arguments: " + psi.Arguments);
            }

            psi.UseShellExecute = false;
            psi.RedirectStandardOutput = true;
            var process = Process.Start(psi);

            var procOutput = process.StandardOutput;
            process.WaitForExit();

            if (process.ExitCode == 0)
            {
                string applicationId = "";
                // the java submission process will return the application id as the last line
                while (!procOutput.EndOfStream)
                {
                    applicationId = procOutput.ReadLine();
                }

                return applicationId;
            }
            else
            {
                throw new ApplicationException("Launch command failed");
            }
        }

        private readonly string rmNode;
        private readonly int wsPort;
        private readonly DfsClient fsClient;
        private readonly Uri baseUri;

        private readonly string launcherNode;
        private readonly int launcherPort;

        private readonly string launcherJarFile;
        private readonly string yarnDirectory;

        public string RMNode { get { return this.rmNode; } }
        public int WSPort { get { return this.wsPort; } }
        public DfsClient DfsClient { get { return this.fsClient; } }

        public NativeYarnClient(string headNode, int wsPort, DfsClient fsClient, Uri baseUri, string launcherJarFile, string yarnDirectory)
        {
            this.rmNode = headNode;
            this.wsPort = wsPort;
            this.fsClient = fsClient;
            this.baseUri = baseUri;

            this.launcherNode = null;
            this.launcherPort = -1;

            this.launcherJarFile = launcherJarFile;
            this.yarnDirectory = yarnDirectory;
        }

        public NativeYarnClient(string headNode, int wsPort, DfsClient fsClient, Uri baseUri, string launcherNode, int launcherPort)
        {
            this.rmNode = headNode;
            this.wsPort = wsPort;
            this.fsClient = fsClient;
            this.baseUri = baseUri;

            this.launcherNode = launcherNode;
            this.launcherPort = launcherPort;

            this.launcherJarFile = null;
            this.yarnDirectory = null;
        }

        public NativeYarnClient(string headNode, int wsPort, DfsClient fsClient)
        {
            this.rmNode = headNode;
            this.wsPort = wsPort;
            this.fsClient = fsClient;
            this.baseUri = null;

            this.launcherNode = null;
            this.launcherPort = -1;

            this.launcherJarFile = null;
            this.yarnDirectory = null;
        }

        public void Dispose()
        {
            this.fsClient.Dispose();
        }

        public Uri JobDirectoryTemplate {
            get { return this.fsClient.Combine(this.baseUri, "_BASELOCATION_", "_JOBID_"); }
        }

        private string SubmitToLauncher(XDocument config)
        {
            UriBuilder builder = new UriBuilder();

            builder.Host = this.launcherNode;
            builder.Port = this.launcherPort;
            builder.Path = "/yarnlauncher/";

            Uri launcherUri = builder.Uri;

            HttpWebRequest request = HttpWebRequest.Create(launcherUri) as HttpWebRequest;
            request.Method = "POST";

            try
            {
                using (Stream upload = request.GetRequestStream())
                {
                    using (XmlWriter xw = XmlWriter.Create(upload))
                    {
                        config.WriteTo(xw);
                    }
                }

                using (WebResponse response = request.GetResponse())
                {
                    using (Stream rs = response.GetResponseStream())
                    {
                        using (StreamReader sr = new StreamReader(rs))
                        {
                            return sr.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new ApplicationException("Failed to launch", e);
            }
        }

        private string SubmitViaJvm(XDocument config)
        {
            try
            {
                return LaunchJob(config, this.launcherJarFile, this.yarnDirectory, null);
            }
            catch (Exception e)
            {
                throw new ApplicationException("Failed to launch", e);
            }
        }

        public ClusterJob Submit(XDocument config, Uri errorLocation)
        {
            string applicationId;

            if (launcherNode == null)
            {
                if (this.launcherJarFile == null)
                {
                    throw new ApplicationException("Client not configured to submit jobs");
                }
                applicationId = SubmitViaJvm(config);
            }
            else
            {
                applicationId = SubmitToLauncher(config);
            }

            Uri jobDir = null;
            if (errorLocation != null)
            {
                jobDir = new Uri(errorLocation.AbsoluteUri.Replace("_JOBID_", applicationId));
            }
            return new NativeYarnJob(this, applicationId, jobDir);
        }

        public ClusterJob QueryJob(string applicationId, Uri jobDirectory)
        {
            return new NativeYarnJob(this, applicationId, jobDirectory);
        }
    }

    public class NativeYarnJob : ClusterJob
    {
        private NativeYarnClient client;
        private string applicationId;
        private Uri jobDir;
        private JobStatus status;
        private string errorMsg;

        internal NativeYarnJob(NativeYarnClient client, string id, Uri jobDir)
        {
            this.client = client;
            this.applicationId = id;
            this.jobDir = jobDir;
            this.status = JobStatus.Waiting;
        }

        private Uri GetRestServiceUri()
        {
            UriBuilder builder = new UriBuilder();
            builder.Host = this.client.RMNode;
            builder.Port = this.client.WSPort;
            builder.Path = "/ws/v1/cluster/apps/" + this.applicationId;
            return builder.Uri;
        }

        private void ProcessXmlData(XDocument xmlData)
        {
            // for now, just pull state and finalStatus out of xml response

            //State: The application state according to the ResourceManager - 
            //valid values are: NEW, SUBMITTED, ACCEPTED, RUNNING, FINISHING, FINISHED, FAILED, KILLED
            //
            //finalStatus: The final status of the application if finished - 
            //reported by the application itself - valid values are: UNDEFINED, SUCCEEDED, FAILED, KILLED

            var stateString = xmlData.Descendants("state").Single().Value;

            switch (stateString)
            {
                case "NEW":
                    this.status = JobStatus.NotSubmitted;
                    break;
                case "SUBMITTED":
                case "ACCEPTED":
                    this.status = JobStatus.Waiting;
                    break;
                case "RUNNING":
                    this.status = JobStatus.Running;
                    break;
                case "FINISHING":
                case "FINISHED":
                    var finalStatusString = xmlData.Descendants("finalStatus").Single().Value;
                    switch (finalStatusString)
                    {
                        case "UNDEFINED":
                            this.status = JobStatus.Success;
                            break;
                        case "SUCCEEDED":
                            this.status = JobStatus.Success;
                            break;
                        case "FAILED":
                            this.status = JobStatus.Failure;
                            break;
                        case "KILLED":
                            this.status = JobStatus.Cancelled;
                            break;
                        default:
                            throw new ApplicationException("Unexpected finalStatus from YARN Resource Manager");
                    }
                    break;
                case "FAILED":
                    this.status = JobStatus.Failure;
                    if (String.IsNullOrEmpty(this.errorMsg))
                    {
                        this.errorMsg = xmlData.Descendants("diagnostics").Single().Value.Trim();
                    }
                    break;
                case "KILLED":
                    this.status = JobStatus.Cancelled;
                    break;
                default:
                    throw new ApplicationException("Unexpected status from YARN Resource Manager");
            }
        }

        public JobStatus GetStatus()
        {
            if (this.status == JobStatus.Waiting || this.status == JobStatus.Running)
            {
                XDocument xmlData;

                HttpWebRequest request = HttpWebRequest.Create(GetRestServiceUri()) as HttpWebRequest;
                request.Accept = "application/xml";
                using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
                {
                    using (Stream data = response.GetResponseStream())
                    {
                        xmlData = XDocument.Load(data);
                    }
                }

                ProcessXmlData(xmlData);

                if (jobDir != null && !(this.status == JobStatus.Waiting || this.status == JobStatus.Running))
                {
                    // the job finished
                    TryToReadError();
                }
            }

            return this.status;
        }

        private void TryToReadError()
        {
            try
            {
                Uri path = this.client.DfsClient.Combine(this.jobDir, "error.txt");
                if (this.client.DfsClient.IsFileExists(path))
                {
                    using (Stream s = this.client.DfsClient.GetDfsStreamReader(path))
                    {
                        using (StreamReader sr = new StreamReader(s))
                        {
                            this.errorMsg = sr.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception)
            {
            }
        }

        public string ErrorMsg { get { return this.errorMsg; } }

        public string Id { get { return this.applicationId; } }

        public void Join()
        {
            while (this.status == JobStatus.Waiting || this.status == JobStatus.Running)
            {
                GetStatus();
                System.Threading.Thread.Sleep(1000);
            }
        }

        public void Kill()
        {
            throw new NotImplementedException();
        }
    }
}
