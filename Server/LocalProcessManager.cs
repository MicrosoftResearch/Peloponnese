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
using System.Xml.Linq;

namespace Microsoft.Research.Peloponnese
{
    /// <summary>
    /// process group manager for starting processes on a local computer
    /// </summary>
    internal class LocalProcessManager : IProcessGroupManager
    {
        /// <summary>
        /// class encapsulating a local process to be started; this refers to a 'logical'
        /// process that is restarted with a new version when the underlying process exits
        /// </summary>
        private class LocalProcess
        {
            private ILogger logger;

            /// <summary>
            /// the index of the logical process in the parent's array of processes
            /// </summary>
            private int index;

            /// <summary>
            /// the current version of the physical process
            /// </summary>
            private int version;

            /// <summary>
            /// the unique identifier of the current version of the process. There is an invariant
            /// that outside the lock, identifier and systemProcess are either both null, or correspond
            /// to the same process.
            /// </summary>
            private string identifier;

            /// <summary>
            /// the current physical system process this logical process corresponds to, or
            /// null if there is no process running. There is an invariant that outside the lock,
            /// identifier and systemProcess are either both null, or correspond to the same process.
            /// </summary>
            private Process systemProcess;

            /// <summary>
            /// a pointer to the parent for callbacks
            /// </summary>
            private LocalProcessManager parent;

            /// <summary>
            /// create a new logical process
            /// </summary>
            /// <param name="p">a pointer to the parent, for callbacks</param>
            /// <param name="id">the index of the logical process in the parent's array of processes</param>
            public LocalProcess(LocalProcessManager p, int id)
            {
                parent = p;
                logger = parent.Logger;
                index = id;
                // always initialize to version 0; this will be incremented before the first process is started
                version = 0;
            }

            /// <summary>
            /// the event that is called by the system when a physical process exits
            /// </summary>
            /// <param name="obj">unused</param>
            /// <param name="args">unused</param>
            private void ProcessExited(object obj, EventArgs args)
            {
                Task.Run(() => ProcessExitedInternal());
            }

            /// <summary>
            /// the event that is called by the system when a physical process exits
            /// </summary>
            private void ProcessExitedInternal()
            {
                // variable to use outside the lock indicating the identifier of the
                // process that just exited, or null if the parent was already told
                // about the process exit (which will happen if the process is terminated
                // using the Stop method).
                string id = null;
                int exitCode = 1;

                lock (this)
                {
                    if (systemProcess != null)
                    {
                        id = identifier;
                        exitCode = systemProcess.ExitCode;

                        // discard the pointer to the process
                        systemProcess = null;
                        identifier = null;
                    }
                }

                // tell our parent that the current physical process has exited, in case it
                // wants to start a new version
                if (id != null)
                {
                    parent.OnProcessExited(index, id, exitCode, false);
                }
            }

            /// <summary>
            /// make the necessary datastructure for starting a new physical process
            /// </summary>
            /// <param name="wd">the new process' working directory</param>
            /// <param name="details">a descriptor indicating how to start the process</param>
            /// <param name="id">the process' unique identifier</param>
            /// <returns>the datastructure needed to start the process</returns>
            private ProcessStartInfo MakeStartInfo(string wd, ExeDetails details, string id)
            {
                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.CreateNoWindow = true;
                startInfo.UseShellExecute = false;
                startInfo.WorkingDirectory = wd;

                if (details.stdoutFile != null)
                {
                    startInfo.RedirectStandardOutput = true;
                }

                if (details.stderrFile != null)
                {
                    startInfo.RedirectStandardError = true;
                }
                
                startInfo.Arguments = details.commandLineArguments;

                if (Path.IsPathRooted(details.commandLine))
                {
                    // if the executable was specified as a full path, run it from there
                    startInfo.FileName = details.commandLine;
                }
                else
                {
                    // otherwise run it in the process' working directory
                    startInfo.FileName = Path.Combine(wd, details.commandLine);
                }

                // add all the environment variables that were specified in the config
                foreach (var e in details.environment)
                {
                    startInfo.EnvironmentVariables.Remove(e.Key);
                    startInfo.EnvironmentVariables.Add(e.Key, e.Value);
                }

                // add the Peloponnese-specific environment variables that will let the process contact the server
                // and register itself
                startInfo.EnvironmentVariables.Add(Constants.EnvManagerServerUri, parent.parent.ServerAddress);
                startInfo.EnvironmentVariables.Add(Constants.EnvJobGuid, parent.parent.JobGuid.ToString());
                startInfo.EnvironmentVariables.Add(Constants.EnvProcessGroup, parent.GroupName);
                startInfo.EnvironmentVariables.Add(Constants.EnvProcessIdentifier, id);
                startInfo.EnvironmentVariables.Add(Constants.EnvProcessHostName, Environment.MachineName);
                // for data locality purposes, every process in the group is assumed to be running on the
                // same rack
                startInfo.EnvironmentVariables.Add(Constants.EnvProcessRackName, "localrack");

                return startInfo;
            }

            /// <summary>
            /// start a new version of the physical process
            /// </summary>
            /// <param name="details">a descriptor of how to start the process</param>
            /// <returns>true if and only if the process was successfully started</returns>
            public async Task<bool> Start(ExeDetails details)
            {
                // copies of the identifier and version that can be used outside the lock
                string id;

                lock (this)
                {
                    // we should only be starting one version at a time
                    Debug.Assert(systemProcess == null);
                    Debug.Assert(identifier == null);

                    // increment the version before starting the process, then record the version
                    // for use outside the lock
                    ++version;
                    if (parent.NumberOfVersions > 0 && version > parent.NumberOfVersions)
                    {
                        // hack for now to stop the service running indefinitely creating local
                        // process directories if the config is broken and none of them will start
                        logger.Log("Local process " + index + " failed too many times: exiting");
                        parent.StartShuttingDown();
                        return false;
                    }

                    // create a new unique identifier for the process based on its index and version,
                    // then record the identifier for use outside the lock
                    id = String.Format("Process.{0,3:D3}.{1,3:D3}", index, version);
                }

                // let the parent know the new process' identifier before it is started. It will tell the service
                // which will then know to accept its registration once it starts
                parent.OnRegisterProcess(id);

                // try to actually start the process
                if (await StartInternal(details, id))
                {
                    return true;
                }
                else
                {
                    // this failed: let the parent know 
                    logger.Log("Start reporting process exit");
                    parent.OnProcessExited(index, id, 1, true);
                    return false;
                }
            }

            /// <summary>
            /// do the work of copying resources to the physical process' working directory, and
            /// starting it
            /// </summary>
            /// <param name="details">a descriptor of how to start the process</param>
            /// <param name="id">unique identifier for the process</param>
            /// <returns>true if and only if the process was started</returns>
            private async Task<bool> StartInternal(ExeDetails details, string id)
            {
                // make a working directory by combining the service's working directory
                // with the group name and unique process identifier
                var groupWd = Path.Combine(Directory.GetCurrentDirectory(), parent.GroupName);
                string wd = Path.Combine(groupWd, id);

                try
                {
                    // if there was already a directory of that name, try to delete it
                    Directory.Delete(wd, true);
                }
                catch (Exception e)
                {
                    if (!(e is DirectoryNotFoundException))
                    {
                        // if there's a directory there that we can't delete, don't even try to
                        // start the process because something bad is going on.
                        logger.Log("Failed to delete existing directory " + wd + ": " + e.Message);
                        return false;
                    }
                }

                try
                {
                    // make the working directory for the new process
                    Directory.CreateDirectory(wd);
                    logger.Log("Created working directory " + wd);
                }
                catch (Exception e)
                {
                    // if we can't make the working directory, don't try to start the process
                    logger.Log("Failed to create working directory " + wd + ": " + e.Message);
                    return false;
                }

                logger.Log("Copying resources to " + wd);
                // we will copy all the resource groups in parallel; this is the list of Tasks
                // to wait on
                var waiters = new List<Task<bool>>();
                foreach (var r in details.resources)
                {
                    waiters.Add(r.FetchToLocalDirectoryAsync(wd));
                }
                // the return values are an array of bools indicating for each group whether it
                // copied successfully or not
                var gotResourcesArray = await Task.WhenAll(waiters);
                // AND together all the return values
                var gotResources = gotResourcesArray.Aggregate(true, (a, b) => a && b);
                if (!gotResources)
                {
                    // at least one resource failed to copy: we can't start the process
                    logger.Log("Failed to copy resources to working directory " + wd);
                    return false;
                }

                ProcessStartInfo startInfo;
                try
                {
                    // make the actual datastructure for starting the process
                    startInfo = MakeStartInfo(wd, details, id);
                }
                catch (Exception e)
                {
                    logger.Log("Failed to make process start info for " + id + ": " + e.ToString());
                    return false;
                }

                Process newProcess;
                lock (this)
                {
                    // make a new system process and copy it into a local variable to use outside
                    // the lock. Once we exit the lock here, an asynchronous call to Stop() could try to kill
                    // the process
                    systemProcess = new Process();
                    systemProcess.StartInfo = startInfo;
                    systemProcess.EnableRaisingEvents = true;
                    systemProcess.Exited += new EventHandler(ProcessExited);
                    identifier = id;
                    newProcess = systemProcess;
                }

                logger.Log("Trying to start process " + parent.GroupName + ":" + id + " -- " + startInfo.FileName + " " + startInfo.Arguments);

                try
                {
                    newProcess.Start();
                    logger.Log("Process " + newProcess.Id + " started for " + parent.GroupName + ":" + id);

                    if (details.stdoutFile != null)
                    {
                        string stdOutDest = details.stdoutFile;
                        if (details.redirectDirectory != null)
                        {
                            stdOutDest = Path.Combine(details.redirectDirectory, stdOutDest);
                        }
                        Task copyTask = Task.Run(() => CopyStreamWithCatch(systemProcess.StandardOutput, stdOutDest, wd));
                    }

                    if (details.stderrFile != null)
                    {
                        string stdErrDest = details.stderrFile;
                        if (details.redirectDirectory != null)
                        {
                            stdErrDest = Path.Combine(details.redirectDirectory, stdErrDest);
                        }
                        Task copyTask = Task.Run(() => CopyStreamWithCatch(systemProcess.StandardError, stdErrDest, wd));
                    }

                    return true;
                }
                catch (Exception e)
                {
                    // if we didn't manage to start the process, get rid of the pointer to it; the parent
                    // will call parent.OnProcessExited for us
                    lock (this)
                    {
                        systemProcess = null;
                        identifier = null;
                    }

                    logger.Log("Process start failed for " + parent.GroupName + ":" + id + ": " + e.ToString());
                    return false;
                }
            }

            private async Task FlushRegularly(Stream stream, int intervalMs, Task interrupt)
            {
                try
                {
                    Task awoken;
                    do
                    {
                        awoken = await Task.WhenAny(new[] { Task.Delay(intervalMs), interrupt });
                        await stream.FlushAsync();
                    } while (awoken != interrupt);
                }
                catch (Exception e)
                {
                    logger.Log("Flusher caught exception " + e.ToString());
                }
            }

            private async void CopyStreamWithCatch(StreamReader src, string dstPath, string wd)
            {
                if (!Path.IsPathRooted(dstPath))
                {
                    dstPath = Path.Combine(wd, dstPath);
                }

                try
                {
                    using (Stream dst = new FileStream(dstPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
                    {
                        Task stopFlushing = new Task(() => { });
                        Task flushTask = Task.Run(() => FlushRegularly(dst, 1000, stopFlushing));
                        int nRead;
                        byte[] buffer = new byte[4 * 1024];
                        do
                        {
                            nRead = await src.BaseStream.ReadAsync(buffer, 0, buffer.Length);
                            if (nRead > 0)
                            {
                                await dst.WriteAsync(buffer, 0, nRead);
                            }
                        } while (nRead > 0);
                        stopFlushing.RunSynchronously();
                        await flushTask;
                    }
                }
                catch (Exception e)
                {
                    logger.Log("Copying stream to " + dstPath + " caught exception " + e.ToString());
                }
            }

            /// <summary>
            /// called by the parent to kill a running physical process
            /// </summary>
            public void Stop()
            {
                Process p = null;
                lock (this)
                {
                    p = systemProcess;
                }

                // there is supposed to be a running physical process
                if (p != null)
                {
                    try
                    {
                        p.Kill();
                        logger.Log("Stopped process " + identifier);
                    }
                    catch (Exception e)
                    {
                        logger.Log("Failed to stop process " + identifier + ": " + e.ToString());
                    }
                }

                // this will exit the lock with a non-null identifier if we should notify the parent that
                // the process has been killed
                string id = null;
                int exitCode = 1;

                lock (this)
                {
                    if (systemProcess != null)
                    {
                        id = identifier;

                        try
                        {
                            exitCode = systemProcess.ExitCode;
                        }
                        catch (Exception e)
                        {
                            logger.Log("Unable to read process exit code " + identifier + ": " + e.ToString());
                        }

                        // set the process pointer to null and notify the parent. Normally, when the running process
                        // is killed it will generate a call to the OnProcessExited event which will handle this, so in 
                        // theory we could skip this step here. But there's a race between setting systemProcess inside
                        // the lock in StartInternal and actually starting it outside the lock, that could cause Stop to
                        // kill the unstarted process and not generate an exit event, so we double check here for safety
                        systemProcess = null;
                        identifier = null;
                    }
                }

                if (id != null)
                {
                    parent.OnProcessExited(index, id, exitCode, false);
                }
            }
        }

        /// <summary>
        /// the logging interface
        /// </summary>
        private ILogger logger;
        /// <summary>
        /// pointer to the service to use for callbacks
        /// </summary>
        private IServiceManager parent;
        /// <summary>
        /// number of times each process should be started before giving up
        /// </summary>
        private int numberOfVersions;
        /// <summary>
        /// name of the process group being managed
        /// </summary>
        private string groupName;
        /// <summary>
        /// descriptor of the process that each instance should start
        /// </summary>
        private ExeDetails processDetails;
        /// <summary>
        /// array of logical process instances, each of which may be restarted multiple
        /// times as different physical processes
        /// </summary>
        private LocalProcess[] processes;
        /// <summary>
        /// flag indicating when the service is shutting down. When this is true, the process
        /// group should stop restarting processes that exit.
        /// </summary>
        bool shuttingDown;

        /// <summary>
        /// property so that LocalProcess objects can log
        /// </summary>
        public ILogger Logger { get { return logger; } }

        /// <summary>
        /// property so that LocalProcess objects can find out the number of versions
        /// processes
        /// </summary>
        public int NumberOfVersions { get { return numberOfVersions; } }

        /// <summary>
        /// property so that LocalProcess objects can find out the group name to tell to child
        /// processes
        /// </summary>
        public string GroupName { get { return groupName; } }

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
            shuttingDown = false;

            // read the target number of processes out of the config. This defaults to 1
            // if not otherwise specified
            int numberOfProcesses = 1;
            var nProcAttr = config.Attribute("numberOfProcesses");
            if (nProcAttr != null)
            {
                // don't worry about throwing exceptions if this is malformed
                numberOfProcesses = int.Parse(nProcAttr.Value);
            }

            // read the target number of restarts for each process out of the config. This defaults to 5
            // if not otherwise specified
            numberOfVersions = 5;
            var nRestartAttr = config.Attribute("numberOfVersions");
            if (nRestartAttr != null)
            {
                // don't worry about throwing exceptions if this is malformed
                numberOfVersions = int.Parse(nRestartAttr.Value);
            }

            // make a logical process object for each process we are managing
            processes = new LocalProcess[numberOfProcesses];
            for (int i = 0; i < processes.Length; ++i)
            {
                processes[i] = new LocalProcess(this, i);
            }

            // read the descriptor that we will use to create physical processes.
            // don't worry about throwing exceptions if this isn't present or is
            // malformed
            var processElement = config.Descendants("Process").Single();
            processDetails = new ExeDetails();
            processDetails.ReadFromConfig(processElement, logger);
        }

        /// <summary>
        /// move into the shutdown state in which processes are no longer restarted when
        /// they exit. 
        /// </summary>
        public void StartShuttingDown()
        {
            lock (this)
            {
                shuttingDown = true;
            }
        }

        /// <summary>
        /// hard shut down any processes that have not exited
        /// </summary>
        /// <returns>an unblocked waiter</returns>
        public Task KillRunningProcessesAsync(bool success, string status)
        {
            for (int i = 0; i < processes.Length; ++i)
            {
                processes[i].Stop();
            }

            // there doesn't seem to be a FromResult equivalent for Tasks that don't
            // return a value so this is the easiest way to return from a faux-async
            // method
            return Task.FromResult(true);
        }

        /// <summary>
        /// the first method called after initialization. This starts up the process
        /// group, attempting to spawn all the required tasks
        /// </summary>
        /// <returns>a Task that completes when all the processes have been started</returns>
        public async Task StartWorkerProcessesAsync()
        {
            // let the parent know how many tasks we're planning to start
            parent.OnManagerTargetChanged(groupName, processes.Length);

            // we'll start them all in parallel so make a list of Tasks to wait on
            var waiters = new List<Task<bool>>();
            for (int i=0; i<processes.Length; ++i)
            {
                waiters.Add(processes[i].Start(processDetails));
            }

            // finish when all the processes have started (or failed to start)
            var results = await Task.WhenAll(waiters);
            var succeeded = results.Aggregate(true, (a, b) => a && b);

            if (!succeeded)
            {
                // if any process failed to start, kill all the ones that succeeded
                await KillRunningProcessesAsync(false, "process failed to start");
                // let the parent know we've had a fatal error
                parent.OnManagerFailed(groupName, 1);
            }
        }

        /// <summary>
        /// called by a LocalProcess object when it creates the identifier for a new
        /// process
        /// </summary>
        /// <param name="identifier">the unique id of the new process</param>
        public void OnRegisterProcess(string identifier)
        {
            // let the service know it should be listening for a new registration
            parent.OnRegisterProcess(groupName, identifier);
        }

        /// <summary>
        /// called by a LocalProcess or self when a process should be removed from
        /// the list of registered processes
        /// </summary>
        /// <param name="index">the index of the process in the LocalProcess array</param>
        /// <param name="identifier">the unique id of the process</param>
        /// <param name="fatal">
        /// this is true if a process start failed, in which case it is considered to be
        /// an unrecoverable error
        /// </param>
        public async void OnProcessExited(int index, string identifier, int exitCode, bool fatal)
        {
            // let the service know it should remove this process from the table
            parent.OnProcessExited(groupName, identifier, exitCode);

            // variable to use outside the lock indicating whether we should start a
            // new version of the process
            bool restart;
            lock (this)
            {
                if (fatal)
                {
                    // if the error is unrecoverable we're going to be shutting down so
                    // stop everyone from spawning new versions
                    shuttingDown = true;
                }

                // if we're not shutting down, start a new version
                restart = !shuttingDown;
            }

            // even if the error was fatal don't do cleanup (call KillRunningProcessesAsync, etc.)
            // here, because that should only get done once all process starts have completed, to
            // avoid missing processes that are in the middle of starting

            if (restart)
            {
                // if we're not shutting down, start a new version
                bool succeeded = await processes[index].Start(processDetails);

                if (!succeeded)
                {
                    // the process failed to start, so kill everyone else and die
                    await KillRunningProcessesAsync(false, "process failed to start");
                    // let the parent know we've had a fatal error
                    parent.OnManagerFailed(groupName, 1);
                }
            }
        }
    }
}
