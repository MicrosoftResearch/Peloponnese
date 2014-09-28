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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese
{
    public class Constants
    {
        /// <summary>
        /// the name of the Peloponnese executable
        /// </summary>
        public const string PeloponneseExeName = "Microsoft.Research.Peloponnese.PersistentProcessManager.exe";

        /// <summary>
        /// the environment variable used at a client to indicate where Peloponnese has been installed
        /// </summary>
        public const string PeloponneseHomeVar = "PELOPONNESE_HOME";

        /// <summary>
        /// the environment variable used to communicate the server's Uri to a
        /// spawned process. This is set by the ProcessGroupManager for each
        /// process it creates
        /// </summary>
        public const string EnvManagerServerUri = "PELOPONNESE_SERVER_URI";

        /// <summary>
        /// the environment variable used to communicate the GUID of the job
        /// that a spawned process belongs to. This is set by the ProcessGroupManager for each
        /// process it creates. The spawned process uses this GUID when it registers itself
        /// with the web server
        /// </summary>
        public const string EnvJobGuid = "PELOPONNESE_JOB_GUID";

        /// <summary>
        /// the environment variable used to communicate the name of the process group
        /// that a spawned process belongs to. This is set by the ProcessGroupManager for each
        /// process it creates. The spawned process uses this name when it registers itself
        /// with the web server
        /// </summary>
        public const string EnvProcessGroup = "PELOPONNESE_PROCESS_GROUP";

        /// <summary>
        /// the environment variable used to communicate the identifier for a spawned
        /// process. This is set by the ProcessGroupManager for each
        /// process it creates. Identifiers must be unique (within a given group) over
        /// the lifetime of the server. The spawned process uses this identifier when
        /// it registers itself with the web server
        /// </summary>
        public const string EnvProcessIdentifier = "PELOPONNESE_PROCESS_IDENTIFIER";

        /// <summary>
        /// the environment variable used to communicate the hostname for a spawned
        /// process. This is set by the ProcessGroupManager for each
        /// process it creates. The hostname corresponds to the computer the process
        /// is running on, and is used when reasoning about data locality to determine
        /// when data items lie on the same computer. It is set in this environment
        /// variable rather than simply queried by the process to allow for debugging
        /// scenarios in which processes on the same computer masquerade as having different
        /// data locality. The spawned process uses this identifier when it registers
        /// itself with the web server.
        /// </summary>
        public const string EnvProcessHostName = "PELOPONNESE_PROCESS_HOSTNAME";

        /// <summary>
        /// the environment variable used to communicate the rack a spawned
        /// process is running on. This may be set by the ProcessGroupManager for each
        /// process it creates. The rack is used as a second-level data locality
        /// hint; data on the same rack is assumed to be closer than data on different
        /// racks. The spawned process uses this identifier when it registers itself
        /// with the web server. If it is unset, the process uses a default rack name
        /// </summary>
        public const string EnvProcessRackName = "PELOPONNESE_PROCESS_RACKNAME";

        public const string RedirectDirectoryAttribute = "redirectdirectoryenvvariable";

        public const string RedirectStdOutAttribute = "redirectstdout";

        public const string RedirectStdErrAttribute = "redirectstderr";

        /// <summary>
        /// the numeric value sent by a YARN app master when a process changes state
        /// </summary>
        public enum YarnProcessState
        {
            /// <summary>
            /// should not be sent
            /// </summary>
            Unknown = 0,
            /// <summary>
            /// the process is in the YARN scheduling queue
            /// </summary>
            Scheduling = 1,
            /// <summary>
            /// the process has started running
            /// </summary>
            Running = 2,
            /// <summary>
            /// the process exited cleanly
            /// </summary>
            Completed = 3,
            /// <summary>
            /// the process failed
            /// </summary>
            Failed = 4,
            /// <summary>
            /// the process failed
            /// </summary>
            StartFailed = 5
        }

        /// <summary>
        /// the high level state of the service, returned as an attribute of the status
        /// XML element returned by the web server
        /// </summary>
        public enum ServiceState
        {
            /// <summary>
            /// the service is up and running, managing processes
            /// </summary>
            Running,
            /// <summary>
            /// the service is cleanly shutting down; processes that terminate are
            /// no longer being restarted
            /// </summary>
            ShuttingDown,
            /// <summary>
            /// all process groups have been told to terminate all processes
            /// </summary>
            Stopped
        }
    }
}
