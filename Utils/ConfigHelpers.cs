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
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.Shared;

namespace Microsoft.Research.Peloponnese.ClusterUtils
{
    public class ConfigHelpers
    {
        private static string nugetPpmHome;

        private static IEnumerable<string> EnumeratePeloponneseWorkerFiles(string directory)
        {
            string[] workerFiles =
            {
                "Microsoft.Research.Peloponnese.wrapper.cmd"
            };

            return workerFiles.Select(f => Path.Combine(directory, f));
        }

        private static bool IsValidPpmLocation(string directory)
        {
            if (directory == null)
            {
                return false;
            }

            string[] requiredFiles =
            {
                Constants.PeloponneseExeName,
                Constants.PeloponneseExeName + ".config",
                "Microsoft.Research.Peloponnese.wrapper.cmd",
            };

            IEnumerable<string> requiredFilesLower = requiredFiles.Select(x => x.ToLower());

            IEnumerable<string> filesPresent =
                Directory.EnumerateFiles(directory, "*.exe", SearchOption.TopDirectoryOnly)
                .Concat(Directory.EnumerateFiles(directory, "*.config", SearchOption.TopDirectoryOnly))
                .Concat(Directory.EnumerateFiles(directory, "*.cmd", SearchOption.TopDirectoryOnly))
                .Select(x => Path.GetFileName(x).ToLower());

            return (filesPresent.Intersect(requiredFilesLower).Count() == requiredFilesLower.Count());
        }

        static ConfigHelpers()
        {
            string exePath = Path.GetDirectoryName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName);
            if (IsValidPpmLocation(exePath))
            {
                nugetPpmHome = exePath;
            }
            else
            {
                string workingDirectory = Directory.GetCurrentDirectory();
                if (IsValidPpmLocation(workingDirectory))
                {
                    nugetPpmHome = workingDirectory;
                }
            }
        }

        public static XElement MakeResourceElement(string localName, long modificationTime, long size,
                                                   DfsClient dfsClient, Uri dfsPath)
        {
            XElement resourceElement = new XElement("Resource");
            resourceElement.SetAttributeValue("localName", localName);
            resourceElement.SetAttributeValue("timestamp", modificationTime);
            resourceElement.SetAttributeValue("size", size);
            resourceElement.Value = dfsClient.GetClusterInternalUri(dfsPath).AbsoluteUri;

            return resourceElement;
        }

        public static XElement MakeDfsResourceFromRemote(DfsClient dfsClient, Uri resource, ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            long modificationTime;
            long size;
            bool b = dfsClient.GetFileStatus(resource, out modificationTime, out size);
            if (!b)
            {
                throw new ApplicationException("Unknown resource " + resource.AbsoluteUri);
            }

            string fileName = resource.AbsoluteUri.Substring(resource.AbsoluteUri.LastIndexOf('/') + 1);

            return MakeResourceElement(fileName, modificationTime, size, dfsClient, resource);
        }

        public static XElement MakeDfsResourceFromFile(string localPath, DfsClient dfsClient, Uri dfsDirectory,
                                                       ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            long modificationTime;
            long size;
            Uri dfsPath = ResourceFile.UploadResourceIfNeeded(dfsClient, localPath, dfsDirectory,
                                                              out modificationTime, out size, logger);

            string fileName = Path.GetFileName(localPath);

            return MakeResourceElement(fileName, modificationTime, size, dfsClient, dfsPath);
        }

        public static XElement MakeDfsResourceFromBuffer(string localName, byte[] buffer, DfsClient dfsClient, Uri dfsDirectory,
                                                         ILogger logger = null)
        {
            long modificationTime;
            long size;
            Uri dfsPath = ResourceFile.UploadResourceIfNeeded(dfsClient, localName, buffer, dfsDirectory,
                                                              out modificationTime, out size, logger);

            return MakeResourceElement(localName, modificationTime, size, dfsClient, dfsPath);
        }

        public static XElement MakeDfsResourceGroupElement(DfsClient dfsClient, Uri dfsDirectory, bool isPublic)
        {
            XElement groupElement = new XElement("ResourceGroup");

            groupElement.SetAttributeValue("type", "hdfs");
            groupElement.SetAttributeValue("location", dfsClient.GetClusterInternalUri(dfsDirectory));
            if (isPublic)
            {
                groupElement.SetAttributeValue("public", "true");
            }

            return groupElement;
        }       

        public static string GetPPMHome(string homeFromArgs)
        {
            string ppmHome;

            if (homeFromArgs == null)
            {
                ppmHome = Environment.GetEnvironmentVariable(Constants.PeloponneseHomeVar);
                if (ppmHome == null || !IsValidPpmLocation(ppmHome))
                {
                    ppmHome = nugetPpmHome;
                }
            }
            else
            {
                ppmHome = homeFromArgs;
            }

            return ppmHome;
        }

        public static bool RunningFromNugetPackage
        {
            get
            {
                return nugetPpmHome != null;
            }
        }

        private static XElement MakeProcessDetails(
            string commandLine, IEnumerable<string> commandLineArgs,
            string redirectEnvironmentVariable, string stdOutFilename, string stdErrFilename,
            IEnumerable<XElement> resources, Dictionary<string, string> environment)
        {
            var outerElement = new XElement("Process");

            var cmdLineElement = new XElement("CommandLine");
            if (redirectEnvironmentVariable != null)
            {
                cmdLineElement.SetAttributeValue(Constants.RedirectDirectoryAttribute, redirectEnvironmentVariable);
            }
            if (stdOutFilename != null)
            {
                cmdLineElement.SetAttributeValue(Constants.RedirectStdOutAttribute, stdOutFilename);
            }
            if (stdErrFilename != null)
            {
                cmdLineElement.SetAttributeValue(Constants.RedirectStdErrAttribute, stdErrFilename);
            }
            
            cmdLineElement.Value = commandLine;
            outerElement.Add(cmdLineElement);

            var cmdLineArgsElement = new XElement("CommandLineArguments");
            if (commandLineArgs != null)
            {
                foreach (var arg in commandLineArgs)
                {
                    var argElement = new XElement("Argument");
                    argElement.Value = arg;
                    cmdLineArgsElement.Add(argElement);
                }
            }
            outerElement.Add(cmdLineArgsElement);

            var envElement = new XElement("Environment");
            if (environment != null)
            {
                foreach (var e in environment)
                {
                    var varElement = new XElement("Variable");
                    varElement.SetAttributeValue("var", e.Key);
                    varElement.Value = e.Value;
                    envElement.Add(varElement);
                }
            }
            outerElement.Add(envElement);

            if (resources != null)
            {
                foreach (var r in resources)
                {
                    outerElement.Add(r);
                }
            }

            return outerElement;
        }

        /// <summary>
        /// Make a Peloponnese process group XML element
        /// </summary>
        /// <param name="name">The name of the process group</param>
        /// <param name="type">The type of group, currently "local" or "yarn"</param>
        /// <param name="numberOfVersions">The number of versions of each process to allow. If this is -1, the server
        /// will keep restarting processes as they fail. If it is k>0, the application will be terminated after k failures
        /// of any process.</param>
        /// <param name="numberOfProcesses">The number of processes to start in the group. If this is -1, yarn will start
        /// as many as it can; currently this is numberOfClusterComputers-1 since the AM consumes one computer.</param>
        /// <param name="exitController">If this is true, the application will exit immediately as soon as the processes
        /// in this group have completed.</param>
        /// <param name="commandLine">The commandline to use to start each process in the group</param>
        /// <param name="commandLineArgs">The arguments to be provided when starting each process in the group or 
        /// null if no arguments are required.</param>
        /// <param name="redirectEnvironmentVariable">If this is non-NULL it indicates an environment variable that
        /// should be consulted to determine where to redirect stderr and stdout. For example a yarn group may want
        /// to set this to LOG_DIRS and specify redirection for stdout and stderr in which case they will be written
        /// to the standard yarn container log directory.</param>
        /// <param name="stdOutFilename">If this is non-null the group will redirect stdout to a file of this name.
        /// If redirectEnvironmentVariable is null, or the indicated variable doesn't exist in the environment, the
        /// file will be opened in the process' working directory.</param>
        /// <param name="stdErrFilename">If this is non-null the group will redirect stderr to a file of this name.
        /// If redirectEnvironmentVariable is null, or the indicated variable doesn't exist in the environment, the
        /// file will be opened in the process' working directory.</param>
        /// <param name="resources">A set of resource group elements indicating the resources that need to be
        /// fetched before the process can be started.</param>
        /// <param name="environment">A set of environment variables to be set for the process.</param>
        /// <returns>The XML element describing the process group.</returns>
        public static XElement MakeProcessGroup(
            string name,  string type, int numberOfVersions, int numberOfProcesses, int workerMemoryInMB,
            bool exitController, string commandLine, IEnumerable<string> commandLineArgs,
            string redirectEnvironmentVariable, string stdOutFilename, string stdErrFilename,
            IEnumerable<XElement> resources, Dictionary<string, string> environment)
        {
            var outerElement = new XElement("ProcessGroup");
            outerElement.SetAttributeValue("name", name);
            if (exitController)
            {
                outerElement.SetAttributeValue("shutDownOnExit", "true");
            }

            outerElement.SetAttributeValue("type", type);
            if (type == "local")
            {
                outerElement.SetAttributeValue("numberOfVersions", numberOfVersions.ToString());
                outerElement.SetAttributeValue("numberOfProcesses", numberOfProcesses.ToString());
            }
            else if (type == "yarn")
            {
                outerElement.SetAttributeValue("maxFailuresPerNode", numberOfVersions.ToString());
                outerElement.SetAttributeValue("maxProcesses", numberOfProcesses.ToString());
                outerElement.SetAttributeValue("workerMemoryInMB", workerMemoryInMB.ToString()); 
            }

            var processElement = MakeProcessDetails(
                commandLine, commandLineArgs,
                redirectEnvironmentVariable, stdOutFilename, stdErrFilename,
                resources, environment);

            outerElement.Add(processElement);

            return outerElement;
        }

        public static XElement MakeResourceGroup(DfsClient dfsClient, Uri dfsDirectory,
                                                 bool isPublic, IEnumerable<string> localFiles,
                                                 ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            dfsClient.EnsureDirectory(dfsDirectory, isPublic);

            XElement groupElement = MakeDfsResourceGroupElement(dfsClient, dfsDirectory, isPublic);

            if (dfsClient.IsThreadSafe)
            {
                List<Task<XElement>> waiters = new List<Task<XElement>>();
                foreach (string filePath in localFiles)
                {
                    Task<XElement> waiter = Task.Run(() => MakeDfsResourceFromFile(filePath, dfsClient, dfsDirectory, logger));
                    waiters.Add(waiter);
                }

                try
                {
                    Task.WaitAll(waiters.ToArray());
                }
                catch (Exception e)
                {
                    throw new ApplicationException("Dfs resource make failed", e);
                }

                foreach (Task<XElement> t in waiters)
                {
                    groupElement.Add(t.Result);
                }
            }
            else
            {
                try
                {
                    foreach (string filePath in localFiles)
                    {
                        groupElement.Add(MakeDfsResourceFromFile(filePath, dfsClient, dfsDirectory, logger));
                    }
                }
                catch (Exception e)
                {
                    throw new ApplicationException("Dfs resource make failed", e);
                }
            }

            return groupElement;
        }

        public static Task<XElement> MakeResourceGroupAsync(DfsClient dfsClient, Uri dfsDirectory,
                                                            bool isPublic, IEnumerable<string> localFiles,
                                                            ILogger logger = null)
        {
            if (dfsClient.IsThreadSafe)
            {
                return Task.Run<XElement>(() => MakeResourceGroup(dfsClient, dfsDirectory, isPublic, localFiles, logger));
            }
            else
            {
                return Task.FromResult<XElement>(MakeResourceGroup(dfsClient, dfsDirectory, isPublic, localFiles, logger));
            }
        }

        public static XElement MakeRemoteResourceGroup(DfsClient dfsClient, Uri dfsDirectory,
                                                       bool isPublic, ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            XElement groupElement = MakeDfsResourceGroupElement(dfsClient, dfsDirectory, isPublic);

            IEnumerable<Uri> resources = dfsClient.ExpandFileOrDirectory(dfsDirectory);

            if (dfsClient.IsThreadSafe)
            {
                List<Task<XElement>> waiters = new List<Task<XElement>>();
                foreach (Uri resource in resources)
                {
                    Task<XElement> waiter = Task.Run(() => MakeDfsResourceFromRemote(dfsClient, resource, logger));
                    waiters.Add(waiter);
                }

                try
                {
                    Task.WaitAll(waiters.ToArray());
                }
                catch (Exception e)
                {
                    throw new ApplicationException("Dfs resource make failed", e);
                }

                foreach (Task<XElement> t in waiters)
                {
                    groupElement.Add(t.Result);
                }
            }
            else
            {
                try
                {
                    foreach (Uri resource in resources)
                    {
                        groupElement.Add(MakeDfsResourceFromRemote(dfsClient, resource, logger));
                    }
                }
                catch (Exception e)
                {
                    throw new ApplicationException("Dfs resource make failed", e);
                }
            }

            return groupElement;
        }

        public static XElement MakePeloponneseWorkerResourceGroup(DfsClient dfsClient, Uri stagingRoot, string ppmHome, ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            if (!IsValidPpmLocation(ppmHome))
            {
                throw new ApplicationException("Specified Peloponnese location " + ppmHome + " is missing some required files");
            }

            IEnumerable<string> ppmResourcePaths = EnumeratePeloponneseWorkerFiles(ppmHome);
            return MakeResourceGroup(dfsClient, dfsClient.Combine(stagingRoot, "peloponnese"), true, ppmResourcePaths, logger);
        }

        public static Task<XElement> MakePeloponneseWorkerResourceGroupAsync(DfsClient dfsClient, Uri stagingRoot, string ppmHome, ILogger logger = null)
        {
            if (dfsClient.IsThreadSafe)
            {
                return Task.Run<XElement>(() => { return MakePeloponneseWorkerResourceGroup(dfsClient, stagingRoot, ppmHome, logger); });
            }
            else
            {
                return Task.FromResult<XElement>(MakePeloponneseWorkerResourceGroup(dfsClient, stagingRoot, ppmHome, logger));
            }
        }

        public static IEnumerable<string> ListPeloponneseResources(string ppmHome)
        {
            string ppmExe = Path.Combine(ppmHome, Constants.PeloponneseExeName);
            return Shared.DependencyLister.Lister.ListDependencies(ppmExe)
                .Concat(EnumeratePeloponneseWorkerFiles(ppmHome));
        }

        public static XElement MakePeloponneseResourceGroup(DfsClient dfsClient, Uri stagingRoot, string ppmHome, ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            if (!IsValidPpmLocation(ppmHome))
            {
                throw new ApplicationException("Specified Peloponnese location " + ppmHome + " is missing some required files");
            }

            IEnumerable<string> ppmResourcePaths = ListPeloponneseResources(ppmHome);

            return MakeResourceGroup(dfsClient, dfsClient.Combine(stagingRoot, "peloponnese"), true, ppmResourcePaths, logger);
        }

        public static Task<XElement> MakePeloponneseResourceGroupAsync(DfsClient dfsClient, Uri stagingRoot, string ppmHome, ILogger logger = null)
        {
            if (dfsClient.IsThreadSafe)
            {
                return Task.Run<XElement>(() => { return MakePeloponneseResourceGroup(dfsClient, stagingRoot, ppmHome, logger); });
            }
            else
            {
                return Task.FromResult<XElement>(MakePeloponneseResourceGroup(dfsClient, stagingRoot, ppmHome, logger));
            }
        }

        public static XElement MakeConfigResource(DfsClient dfsClient, Uri dfsDirectory,
                                                  XDocument config, string localName, ILogger logger = null)
        {
            byte[] configBytes;
            using (MemoryStream ms = new MemoryStream())
            {
                using (XmlWriter writer = XmlWriter.Create(ms))
                {
                    config.WriteTo(writer);
                }
                configBytes = ms.ToArray();
            }

            return MakeDfsResourceFromBuffer(localName, configBytes, dfsClient, dfsDirectory, logger);
        }

        public static XElement MakeConfigResourceGroup(DfsClient dfsClient, Uri dfsDirectory,
                                                       XDocument config, string configName, ILogger logger = null)
        {
            dfsClient.EnsureDirectory(dfsDirectory, false);

            XElement resourceElement = MakeConfigResource(dfsClient, dfsDirectory, config, configName, logger);

            XElement groupElement = MakeDfsResourceGroupElement(dfsClient, dfsDirectory, false);
            groupElement.Add(resourceElement);

            return groupElement;
        }

        public static XDocument MakeLauncherConfig(
            string jobName, string configName, string queueName, int amMemory, IEnumerable<XElement> resources, string dfsTemplate = null)
        {
            var configDoc = new XDocument();

            var docElement = new XElement("PeloponneseConfig");

            var launcherElement = new XElement("PeloponneseLauncher");

            var appElement = new XElement("ApplicationName");
            appElement.Value = jobName;
            launcherElement.Add(appElement);

            if (queueName == null)
            {
                queueName = "default";
            }
            
            var queueElement = new XElement("QueueName");
            queueElement.Value = queueName;
            launcherElement.Add(queueElement);

            var amMemoryElement = new XElement("AmMemory");
            amMemoryElement.Value = amMemory.ToString();
            launcherElement.Add(amMemoryElement);

            string dfsArg = "";
            if (dfsTemplate != null)
            {
                dfsArg = Utils.CmdLineEncode(dfsTemplate) + " ";
            }

            var cmdElement = new XElement("CommandLine");
            cmdElement.Value = Constants.PeloponneseExeName + " " + dfsArg + configName + " \"" + Utils.CmdLineEncode(jobName) + "\"";
            launcherElement.Add(cmdElement);

            var resourceElement = new XElement("Resources");
            foreach (var t in resources)
            {
                resourceElement.Add(t);
            }
            launcherElement.Add(resourceElement);

            docElement.Add(launcherElement);

            configDoc.Add(docElement);

            return configDoc;
        }
    }
}
