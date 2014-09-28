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
using System.Xml.Linq;

namespace Microsoft.Research.Peloponnese
{
    /// <summary>
    /// process group manager for starting processes on a YARN cluster
    /// </summary>
    internal class YarnProcessManager : IProcessGroupManager
    {
        private ILogger logger;
        /// <summary>
        /// pointer to the service to use for callbacks
        /// </summary>
        private IServiceManager parent;
        /// <summary>
        /// name of the process group being managed
        /// </summary>
        private string groupName;
        /// <summary>
        /// the max number of processes to start, or -1 to use the whole cluster
        /// </summary>
        private int maxProcesses;
        /// <summary>
        /// the max number of times a process can fail on a given node before shutting down, or -1 to tolerate arbitrary failures
        /// </summary>
        private int maxFailuresPerNode;
        /// <summary>
        /// the max number of times processes can fail shutting down, or -1 to tolerate arbitrary failures
        /// </summary>
        private int maxTotalFailures;
        /// <summary>
        /// the amount of memory to request for each container
        /// </summary>
        private int workerMemoryInMB;
        /// <summary>
        /// descriptor of the process that each instance should start
        /// </summary>
        private ExeDetails processDetails;
        /// <summary>
        /// the bridge to the Java application master
        /// </summary>
        private Yarn.AMInstance appMaster;

        /// <summary>
        /// read the config and initialize. Can throw exceptions which will be cleanly caught by
        /// the parent
        /// </summary>
        /// <param name="p">parent to use for callbacks</param>
        /// <param name="name">name of this group in the service</param>
        /// <param name="config">element describing configuration parameters</param>
        public void Initialize(IServiceManager p, string name, XElement config)
        {
            parent = p;
            logger = parent.Logger;
            groupName = name;

            // read the target number of processes out of the config. This defaults to -1
            // if not otherwise specified, which means use all the machines in the cluster
            maxProcesses = -1;
            var nProcAttr = config.Attribute("maxProcesses");
            if (nProcAttr != null)
            {
                // don't worry about throwing exceptions if this is malformed
                maxProcesses = int.Parse(nProcAttr.Value);
            }

            // read the target number of failures out of the config. These default to -1
            // if not otherwise specified, which means tolerate arbitrary failures
            maxFailuresPerNode = -1;
            var nFPNAttr = config.Attribute("maxFailuresPerNode");
            if (nFPNAttr != null)
            {
                // don't worry about throwing exceptions if this is malformed
                maxFailuresPerNode = int.Parse(nFPNAttr.Value);
            }
            maxTotalFailures = -1;
            var nTFAttr = config.Attribute("maxTotalFailures");
            if (nTFAttr != null)
            {
                // don't worry about throwing exceptions if this is malformed
                maxTotalFailures = int.Parse(nTFAttr.Value);
            }
            // read the amount of memory to request per container from the config
            // it defaults to -1
            workerMemoryInMB = -1;
            var workerMemAttr = config.Attribute("workerMemoryInMB");
            if (workerMemAttr != null)
            {
                workerMemoryInMB = int.Parse(workerMemAttr.Value);
            }

            // read the descriptor that we will use to create physical processes.
            // don't worry about throwing exceptions if this isn't present or is
            // malformed
            var processElement = config.Descendants("Process").Single();
            processDetails = new ExeDetails();
            processDetails.ReadFromConfig(processElement, logger);

            foreach (var rg in processDetails.resources)
            {
                if (!(rg is HdfsResources))
                {
                    throw new ApplicationException("All YARN process resources must reside in HDFS: " + rg.ToString());
                }
            }
        }

        /// <summary>
        /// move into the shutdown state in which processes are no longer restarted when
        /// they exit. 
        /// </summary>
        public void StartShuttingDown()
        {
            if (appMaster != null)
            {
                appMaster.Shutdown();
            }
        }

        /// <summary>
        /// hard shut down any processes that have not exited. This is not yet implemented.
        /// </summary>
        /// <returns>an unblocked waiter</returns>
        public Task KillRunningProcessesAsync(bool success, string status)
        {
            if (appMaster != null)
            {
                appMaster.Exit(success, status);
            }
            return Task.FromResult(true);
        }

        /// <summary>
        /// the first method called after initialization. This starts up the process
        /// group, attempting to spawn all the required tasks
        /// </summary>
        /// <returns>a Task that completes when all the processes have been started</returns>
        public Task StartWorkerProcessesAsync()
        {
            string stdOut = processDetails.stdoutFile;
            if (stdOut == null)
            {
                stdOut = "stdout.txt";
            }
            string stdErr = processDetails.stderrFile;
            if (stdErr == null)
            {
                stdErr = "stderr.txt";
            }

            appMaster = new Yarn.AMInstance(
                parent.JobGuid.ToString(), parent.ServerAddress, groupName,
                stdOut, stdErr, maxProcesses, workerMemoryInMB, 
                processDetails.commandLine + " " + processDetails.commandLineArguments,
                maxFailuresPerNode, maxTotalFailures);

            Yarn.AMInstance.RegisterCallbacks(OnTargetChanged, OnRegisterProcess, OnProcessChanged, OnFailure);

            foreach (var rg in processDetails.resources)
            {
                var group = rg as HdfsResources;
                foreach (var resource in group.Files)
                {
                    var absoluteUri = new Uri(group.DirectoryURI, resource.remoteName).AbsoluteUri;
                    appMaster.AddResource(resource.localName, absoluteUri, resource.timestamp, resource.size, group.IsPublic);
                }
            }

            appMaster.Start();

            return Task.FromResult(true);
        }

        /// <summary>
        /// called by the app master when the number of cluster nodes is known
        /// </summary>
        /// <param name="target">the number of processes the app master is trying to start</param>
        public void OnTargetChanged(int target)
        {
            parent.OnManagerTargetChanged(groupName, target);
        }

        /// <summary>
        /// called by the app master when a new process starts
        /// process
        /// </summary>
        /// <param name="identifier">the unique id of the new process</param>
        public void OnRegisterProcess(string identifier, string hostName)
        {
            // let the service know it should be listening for a new registration
            parent.OnRegisterProcess(groupName, identifier);
        }

        /// <summary>
        /// called by the app master when the state of a process changes
        /// </summary>
        /// <param name="identifier">the unique id of the process</param>
        /// <param name="state">the new state</param>
        public void OnProcessChanged(string identifier, int state, int exitCode)
        {
            logger.Log("Got process state change to " + identifier + ": " + state);
            if (state == (int)Constants.YarnProcessState.Completed ||
                state == (int)Constants.YarnProcessState.Failed ||
                state == (int)Constants.YarnProcessState.StartFailed)
            {
                // let the service know it should remove this process from the table
                parent.OnProcessExited(groupName, identifier, exitCode);
            }
        }

        /// <summary>
        /// called by the app master when too many processes have failed
        /// </summary>
        public void OnFailure()
        {
            logger.Log("Got failure indication from app master");
            parent.OnManagerFailed(groupName, 1);
        }
    }
}
