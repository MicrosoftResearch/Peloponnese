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
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.NotHttpServer;

using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Research.Peloponnese
{
    /// <summary>
    /// the parent class for the Peloponnese process manager service.
    /// 
    /// The service starts an http server that is used as a rendezvous for processes
    /// started by the service.
    /// 
    /// On startup the service reads an XML config that defines a number of 'Process Groups'
    /// each of which corresponds to a set of processes that should be started. Every process
    /// started in a given group will use the same executable. Process groups have different
    /// types, e.g. a local group that creates processes on the computer where the service
    /// is running; a YARN group that creates processes hosted in YARN containers on a cluster,
    /// etc. A job might e.g. have one group containing a single process that is the 'leader' of
    /// a distributed computation, and a second group containing multiple processes that are all
    /// 'workers' for the computation.
    /// 
    /// A process group has a 'target' number of processes that it intends to spawn. This may not
    /// be known in advance, e.g. since it may depend on the number of containers allocated by a
    /// cluster scheduler. The target may be updated over the lifetime of the service, e.g. if a
    /// cluster scheduler grows or shrinks the allocation of containers. Every time the target of
    /// number of processes in any process group is updated, the service increments an epoch counter.
    /// 
    /// Every process spawned by a process group has three environment variables set on startup:
    /// the Uri of the http server that the service has started; the name of the group the process
    /// belongs to; and a unique identifier for the process within that group. When the process
    /// starts up it is expected to connect to the supplied URI, identify itself using the group
    /// and identifier, and then post an (application-specific) XML element that encodes any
    /// additional information that members of the distributed computation may need, e.g. any
    /// private communication ports it has opened that other processes may want to connect to.
    /// Every time a process registers itself with the http server, or a process registration is
    /// removed because the process group has determined that the process has died, the service
    /// increments a version number. The version number is reset every time the epoch changes.
    /// 
    /// The service's http server supports GETs of status, which return an XML element
    /// representing the entire known state of the cluster: the epoch number, version
    /// number, target for each process group, and set of registered processes along with
    /// the application-specific XML element each supplied. When making a GET request a client
    /// may specify predicates to reduce the need for heartbeats. The basic form of the
    /// predicate is:
    ///   return status when (timeWaiting > timeout ||
    ///                       epoch > lastSeenEpoch ||
    ///                        (version > lastSeenVersion &&
    ///                         group1.numberOfRegisteredProcesses > threshold1 &&
    ///                         group2.numberOfRegisteredProcesses == threshold2 &&
    ///                         ...))
    /// For example, on startup a leader might wait until 75% of the workers have registered
    /// by examining the target number of processes in the worker group and setting the
    /// threshold accordingly. Subsequently it might want notifications every time any
    /// worker leaves or joins, in which case it would simply wait for the version to increase.
    /// Alternately an application such as MPI, that cannot make progress until all workers
    /// have registered, might wait for 100% of the workers to register, then wait until the version
    /// increases and 100% of the workers have registered, to avoid intermediate notifications
    /// when workers fail, until all have been restarted. In both cases, the application would
    /// want to be notified immediately if the epoch changes, since the target number of
    /// processes in the group needs to be re-read.
    /// 
    /// When a leader wishes to perform a clean shutdown, it POSTs a shutdown message to the http
    /// server. This informs the process groups that they should no longer restart processes that
    /// exit, allowing workers to exit cleanly. When all of them have exited, the service will stop.
    /// A group may have a flag set that indicates that, once the shutdown state is entered, the
    /// service should stop as soon as all the processes in that group have exited. For example, a
    /// leader might have the flag set, counting on a cluster resource manager to garbage collect
    /// all the worker processes.
    /// </summary>
    public class PersistentProcessManager : IServiceManager
    {
        /// <summary>
        /// class to hold a copy of the current status. Every time a process stops or
        /// starts, or a group manager changes its target, or the state of the service
        /// changes, the status is updated and a new Status object is created
        /// </summary>
        private class Status
        {
            /// <summary>
            /// create a new Status object encapsulating the state of the service to be
            /// communicated back to clients
            /// </summary>
            /// <param name="ss">the state of the service: Running, Shutting down, or Stopped</param>
            /// <param name="e">the current epoch</param>
            /// <param name="v">the current version within the epoch</param>
            /// <param name="s">a pre-cached XML element describing the state of the processes in the service</param>
            public Status(Constants.ServiceState ss, UInt64 e, UInt64 v, byte[] s)
            {
                state = ss;
                epoch = e;
                version = v;
                status = s;
            }

            /// <summary>
            /// the state of the service: Running, Shutting down, or Stopped
            /// </summary>
            public Constants.ServiceState state;
            /// <summary>
            /// the current epoch. This increments every time the target number of
            /// processes in any process group is updated
            /// </summary>
            public UInt64 epoch;
            /// <summary>
            /// the current version. This increments every time a process is registered
            /// or exits. It is reset every time the epoch is updated
            /// </summary>
            public UInt64 version;
            /// <summary>
            /// a buffer holding the rendered XML of the state of all the registered
            /// processes in the cluster
            /// </summary>
            public byte[] status;
        }

        /// <summary>
        /// helper class used when blocking on a predicate to become true before returning
        /// status. All requests with the same predicate share the same StatusWaiter and
        /// block on the task contained within it. Every time the status is updated, the
        /// predicate is evaluated on each StatusWaiter, and if it is true for a particular
        /// waiter, the Task is started and returns the new status to the waiters.
        /// </summary>
        private class StatusWaiter
        {
            /// <summary>
            /// create a new waiter to block on a predicate
            /// </summary>
            /// <param name="p">when this evaluates to true, the task will be started</param>
            public StatusWaiter(Func<bool> p)
            {
                // when the task is started it will return the response
                waiter = new TaskCompletionSource<Status>();
                predicate = p;
            }

            /// <summary>
            /// this is true if the predicate is true, and it's time to send the status
            /// </summary>
            public bool Ready { get { return predicate.Invoke(); } }

            /// <summary>
            /// get a handle to the task, so another request can wait on it
            /// </summary>
            public Task<Status> Waiter { get { return waiter.Task; } }

            /// <summary>
            /// supply a new status and unblock the task. This is called once the predicate
            /// evaluates to true
            /// </summary>
            /// <param name="r">the status to supply to waiting requests</param>
            public void Dispatch(Status r)
            {
                // do this asynchronously in case waiter.Task has a synchronous continuation
                Task.Run(() => waiter.SetResult(r));
            }

            /// <summary>
            /// a task that requests can block on, which will return a status
            /// that matches the predicate once it is started
            /// </summary>
            private TaskCompletionSource<Status> waiter;
            /// <summary>
            /// a predicate that is re-evaluated every time the status changes,
            /// to see if it's time to unblock the waiters yet
            /// </summary>
            private Func<bool> predicate;
        }

        /// <summary>
        /// this class holds details about a process group, and is updated as the
        /// process group calls into the IServiceManager interface, and when processes
        /// register themselves with the web server
        /// </summary>
        private class ProcessGroup
        {
            /// <summary>
            /// create a new object to hold information about a process group
            /// </summary>
            /// <param name="m">the manager that communicates with processes in the group</param>
            /// <param name="sge">
            /// if this is true, the entire service will exit during shutdown once all the processes
            /// in the group exit
            /// </param>
            public ProcessGroup(IProcessGroupManager m, bool sge)
            {
                registration = new Dictionary<string, XElement>();
                manager = m;
                // this will be updated when the group manager calls the IServiceManager interface
                targetNumberOfProcesses = 0;
                shutdownOnGroupExit = sge;
            }

            /// <summary>
            /// the number of processes in this group that have registered themselves via the web interface
            /// </summary>
            public int RegisteredMembers { get { return registration.Where(p => p.Value != null).Count(); } }

            /// <summary>
            /// metadata provided by processes in the group, to be returned in status requests. the key is the
            /// unique identifier assigned to a process, and the value is the metadata supplied when the process
            /// registers via the web server. Before the manager starts a process, it calls OnRegisterProcess and
            /// an entry is inserted in this dictionary with the process' identifier and a null XElement value;
            /// when the process subsequently contacts the web server the XElement it supplies is filled in.
            /// When the manager reports that the process has exited (via OnProcessExited) the dictionary entry is
            /// removed. If the process subsequently contacts the web server (e.g. because the manager had lost contact
            /// with it, but it hadn't really exited) it will be ignored.
            /// </summary>
            public Dictionary<string, XElement> registration;
            /// <summary>
            /// the manager in charge of starting and monitoring processes in this group
            /// </summary>
            public IProcessGroupManager manager;
            /// <summary>
            /// the number of processes the manager is trying to start. There is an invariant that managers obey,
            /// that the number of entries in the registration table will never exceed this target.
            /// </summary>
            public int targetNumberOfProcesses;
            /// <summary>
            /// if this is true, the entire service will exit during shutdown once all the processes in the
            /// group exit
            /// </summary>
            public bool shutdownOnGroupExit;
        }

        private ILogger logger;
        private int portNumber;
        private string httpPrefix;
        private string serverAddress;
        private Guid jobGuid;
        private IHttpServer server;
        private TaskCompletionSource<bool> shutdown;
        private int exitCode;
        private string exitString;

        private Shared.DfsClient dfsClient;

        private Dictionary<string,ProcessGroup> processes;
        private Status status;
        private Dictionary<string, StatusWaiter> waiters;

        public ILogger Logger { get { return logger; } }

        public string ServerAddress { get { return serverAddress; } }

        public Guid JobGuid { get { return jobGuid; } }

        private void InitializeProcessGroup(XElement element)
        {
            var name = element.Attribute("name").Value;
            var type = element.Attribute("type").Value;

            var shutdownOnGroupExit = false;
            var shutdownAttr = element.Attribute("shutDownOnExit");
            if (shutdownAttr != null && shutdownAttr.Value == "true")
            {
                shutdownOnGroupExit = true;
            }

            var manager = Factory.CreateManager(type);
            manager.Initialize(this, name, element);

            processes.Add(name, new ProcessGroup(manager, shutdownOnGroupExit));
        }

        public bool Initialize(string configName, Uri jobDirectoryUri, ILogger l)
        {
            logger = l;

            jobGuid = Guid.NewGuid();

            logger.Log("PersistentProcessManager initializing for job " + jobGuid.ToString());

            if (jobDirectoryUri != null)
            {
                if (jobDirectoryUri.Scheme == "azureblob")
                {
                    try
                    {
                        dfsClient = new Azure.AzureDfsClient(jobDirectoryUri);
                        dfsClient.EnsureDirectory(jobDirectoryUri, false);
                    }
                    catch (Exception e)
                    {
                        logger.Log("Failed to make Azure DFS client for " + jobDirectoryUri.AbsoluteUri + ": " + e.ToString());
                    }
                }
                else if (jobDirectoryUri.Scheme == "hdfs")
                {
                    try
                    {
                        dfsClient = new Hdfs.HdfsClient(Environment.UserName);
                        dfsClient.EnsureDirectory(jobDirectoryUri, false);
                    }
                    catch (Exception e)
                    {
                        logger.Log("Failed to make HDFS client for " + jobDirectoryUri.AbsoluteUri + ": " + e.ToString());
                    }
                }
            }

            exitCode = 0;
            exitString = "Successful execution";

            try
            {
                processes = new Dictionary<string, ProcessGroup>();

                Logger.Log("Loading XML config " + configName);
                var configDoc = XDocument.Load(configName);
                var outer = configDoc.Descendants("PeloponneseServer").Single();

                portNumber = int.Parse(outer.Descendants("Port").Single().Value);
                httpPrefix = outer.Descendants("Prefix").Single().Value;

                var process = outer.Descendants("ProcessGroup");
                foreach (var pg in process)
                {
                    InitializeProcessGroup(pg);
                }

                status = new Status(Constants.ServiceState.Running, 0, 0, new byte[0]);
                waiters = new Dictionary<string, StatusWaiter>();

                return true;
            }
            catch (Exception e)
            {
                Logger.Log("Failed to read config " + e.ToString());
            }

            return false;
        }

        private XElement MakeProcessElement(string processIdentifier, XElement status)
        {
            var element = new XElement("Process");
            element.SetAttributeValue("identifier", processIdentifier);
            element.Add(status);
            return element;
        }

        private XElement MakeProcessGroupElement(string groupName, ProcessGroup group)
        {
            var element = new XElement("ProcessGroup");
            element.SetAttributeValue("name", groupName);
            element.SetAttributeValue("targetNumberOfProcesses", group.targetNumberOfProcesses);
            foreach (var p in group.registration.Where(r => r.Value != null))
            {
                element.Add(MakeProcessElement(p.Key, p.Value));
            }
            return element;
        }

        private void UpdateStatus(Constants.ServiceState newState, UInt64 newEpoch, UInt64 newVersion)
        {
            var outerElement = new XElement("RegisteredProcesses");
            outerElement.SetAttributeValue("state", newState);
            outerElement.SetAttributeValue("epoch", newEpoch);
            outerElement.SetAttributeValue("version", newVersion);
            foreach (var pg in processes)
            {
                outerElement.Add(MakeProcessGroupElement(pg.Key, pg.Value));
            }

            var statusDoc = new XDocument();
            statusDoc.Add(outerElement);

            using (var ms = new MemoryStream())
            {
                using (var xw = System.Xml.XmlWriter.Create(ms))
                {
                    statusDoc.WriteTo(xw);
                }

                status = new Status(newState, newEpoch, newVersion, ms.ToArray());
            }

            foreach (var w in waiters.Where(w => w.Value.Ready))
            {
                w.Value.Dispatch(status);
            }

            var toRemove = waiters.Where(w => w.Value.Ready).Select(w => w.Key).ToArray();
            foreach (var k in toRemove)
            {
                waiters.Remove(k);
            }
        }

        private IPAddress GetIPAddress()
        {
            var choices = Dns.GetHostAddresses(Environment.MachineName);
            var ipv4 = choices.Where(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
            // think about adding support for multiple interfaces later
            return ipv4.First();
        }

        private bool StartServer()
        {
            IPAddress address = GetIPAddress();
            server = NotHttpServer.Factory.Create(address, portNumber, httpPrefix, true, logger);
            if (server == null)
            {
                Logger.Log("Failed to start server at " + address + ":" + portNumber + ":" + httpPrefix);
                return false;
            }
            else
            {
                serverAddress = server.Address.AbsoluteUri;
                Logger.Log("Started local process server at " + serverAddress);
                return true;
            }
        }

        private async Task RegisterComputer(IHttpContext context)
        {
            var request = context.Request;
            string guidString = null;
            string group = null;
            string identifier = null;
            var succeeded = false;

            XElement payload = null;
            string xmlString = null;
            try
            {
                guidString = request.QueryString["guid"];
                group = request.QueryString["group"];
                identifier = request.QueryString["identifier"];

                if (guidString == null || group == null || identifier == null)
                {
                    throw new ApplicationException("Must have guid, group and identifier elements in query");
                }

                Guid guid = Guid.Parse(guidString);
                if (!guid.Equals(jobGuid))
                {
                    throw new ApplicationException("Expected Guid " + jobGuid.ToString() + " got " + guid.ToString());
                }

                using (var sw = new StreamReader(request.InputStream, request.ContentEncoding))
                {
                    xmlString = await sw.ReadToEndAsync();
                }

                payload = XElement.Parse(xmlString);

                succeeded = true;
            }
            catch (Exception e)
            {
                Logger.Log("Http request read failed " + e.ToString());
            }

            if (succeeded)
            {
                lock (this)
                {
                    ProcessGroup pg;
                    if (processes.TryGetValue(group, out pg))
                    {
                        if (pg.registration.ContainsKey(identifier))
                        {
                            pg.registration[identifier] = payload;
                            UpdateStatus(status.state, status.epoch, status.version + 1);
                        }
                    }
                }

                Logger.Log("Registered remote computer " + group + ":" + identifier + " data " + xmlString);
            }

            try
            {
                if (succeeded)
                {
                    await ReportSuccess(context, true);
                }
                else
                {
                    await ReportError(context, HttpStatusCode.BadRequest, "malformed request");
                }
            }
            catch (Exception e)
            {
                Logger.Log("Http response failed " + e.ToString());
            }

            try
            {
                if (dfsClient != null && succeeded)
                {
                    Uri dstFile = dfsClient.Combine(jobDirectoryUri, group + "_" + identifier);
                    using (Stream writer = dfsClient.GetDfsStreamWriter(dstFile))
                    {
                        using (StreamWriter sw = new StreamWriter(writer))
                        {
                            sw.Write(xmlString);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Log("Registration upload failed " + e.ToString());
            }
        }

        private async Task StartCleanShutdown(IHttpContext context)
        {
            Logger.Log("Notified of initial shutdown");

            await ReportSuccess(context, true);

            TriggerCleanShutdown();
        }

        private async Task StartFullShutdown(IHttpContext context)
        {
            Logger.Log("Notified of final shutdown");

            await ReportSuccess(context, true);

            TriggerFullShutdown(0, "Service shut down externally");
        }

        private Expression EpochGreaterExpression(UInt64 lastSeenEpoch)
        {
            return
                Expression.GreaterThan(
                    Expression.Field(
                        Expression.Field(
                            Expression.Constant(this), "status"), "epoch"),
                    Expression.Constant(lastSeenEpoch));
        }

        private Expression VersionGreaterExpression(UInt64 lastSeenVersion)
        {
            return
                Expression.GreaterThan(
                    Expression.Field(
                        Expression.Field(
                            Expression.Constant(this), "status"), "version"),
                    Expression.Constant(lastSeenVersion));
        }

        private Expression ThresholdGreaterExpression(ProcessGroup group, int threshold)
        {
            return
                Expression.GreaterThan(
                    Expression.Property(
                        Expression.Constant(group), "RegisteredMembers"),
                    Expression.Constant(threshold));
        }

        private Expression ThresholdEqualExpression(ProcessGroup group, int threshold)
        {
            return
                Expression.Equal(
                    Expression.Property(
                        Expression.Constant(group), "RegisteredMembers"),
                    Expression.Constant(threshold));
        }

        private Expression CombineAnd(Expression basePredicate, Expression otherPredicate)
        {
            if (basePredicate == null)
            {
                return otherPredicate;
            }
            else
            {
                return Expression.AndAlso(basePredicate, otherPredicate);
            }
        }

        private Expression MakeVersionPredicate(IHttpRequest req)
        {
            Expression predicate = null;

            var version = req.QueryString["versionGreater"];
            if (version != null)
            {
                var oldVersion = UInt64.Parse(version);
                predicate = VersionGreaterExpression(oldVersion);
            }

            var tGreater = req.QueryString.GetValues("thresholdGreater");
            if (tGreater != null)
            {
                foreach (var tg in tGreater)
                {
                    var parts = tg.Split(':');
                    var groupName = parts[0];
                    var group = processes[groupName];
                    var size = int.Parse(parts[1]);
                    predicate = CombineAnd(predicate, ThresholdGreaterExpression(group, size));
                }
            }

            var tEquals = req.QueryString.GetValues("thresholdEquals");
            if (tEquals != null)
            {
                foreach (var te in tEquals)
                {
                    var parts = te.Split(':');
                    var groupName = parts[0];
                    var group = processes[groupName];
                    var size = int.Parse(parts[1]);
                    predicate = CombineAnd(predicate, ThresholdEqualExpression(group, size));
                }
            }

            return predicate;
        }

        private Status GetStatusImmediately()
        {
            lock (this)
            {
                return status;
            }
        }

        private Task<Status> MakeStatusWaiter(IHttpRequest req)
        {
            try
            {
                lock (this)
                {
                    var predicate = MakeVersionPredicate(req);

                    var epoch = req.QueryString["epochGreater"];
                    if (epoch != null)
                    {
                        var oldEpoch = UInt64.Parse(epoch);
                        if (predicate == null)
                        {
                            predicate = EpochGreaterExpression(oldEpoch);
                        }
                        else
                        {
                            predicate = Expression.OrElse(predicate, EpochGreaterExpression(oldEpoch));
                        }
                    }

                    if (predicate == null)
                    {
                        return Task.FromResult(status);
                    }

                    string signature = predicate.ToString();

                    Func<bool> func = Expression.Lambda(predicate).Compile() as Func<bool>;

                    if (func.Invoke())
                    {
                        return Task.FromResult(status);
                    }
                    else
                    {
                        StatusWaiter blocker;

                        if (!waiters.TryGetValue(signature, out blocker))
                        {
                            blocker = new StatusWaiter(func);
                            waiters.Add(signature, blocker);
                        }

                        Task<Status> waiterTask = blocker.Waiter;

                        var timeoutString = req.QueryString["timeout"];
                        int msTimeout;
                        if (timeoutString != null && int.TryParse(timeoutString, out msTimeout))
                        {
                            var timeoutTask = Task.Delay(msTimeout).ContinueWith((t) => GetStatusImmediately());
                            waiterTask = Utils.SafeWhenAny(logger, waiterTask, timeoutTask).ContinueWith((t) => t.Result.Result);
                        }

                        return waiterTask;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Log("Malformed http request: " + e.ToString());
                return Task.FromResult(null as Status);
            }
        }

        private async Task GetStatus(IHttpContext context)
        {
            var response = await MakeStatusWaiter(context.Request);

            try
            {
                if (response == null)
                {
                    await ReportError(context, HttpStatusCode.BadRequest, "malformed request");
                }
                else
                {
                    await ReportSuccess(context, false);
                    await context.Response.OutputStream.WriteAsync(response.status, 0, response.status.Length);
                    await context.Response.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Logger.Log("Http response failed " + e.ToString());
            }
        }

        private async Task ReportError(IHttpContext context, HttpStatusCode code, string error)
        {
            Logger.Log("Http request error " + code + " " + error);
            context.Request.InputStream.Close();

            context.Response.StatusCode = (int)code;
            context.Response.StatusDescription = code.ToString();
            using (var sw = new StreamWriter(context.Response.OutputStream))
            {
                sw.Write(error);
            }
            await context.Response.CloseAsync();
        }

        private async Task ReportSuccess(IHttpContext context, bool closeResponse)
        {
            context.Request.InputStream.Close();

            context.Response.StatusCode = (int)HttpStatusCode.OK;
            context.Response.StatusDescription = HttpStatusCode.OK.ToString();

            if (closeResponse)
            {
                await context.Response.CloseAsync();
            }
        }

        public async Task HandleRendezvous(IHttpContext context)
        {
            Logger.Log("Got web connection");

            var request = context.Request;

            var u = request.Url.AbsolutePath;
            if (u.StartsWith(httpPrefix))
            {
                var suffix = u.Substring(httpPrefix.Length);
                if (request.HttpMethod.ToUpper() == "POST")
                {
                    if (suffix == "register")
                    {
                        await RegisterComputer(context);
                    }
                    else if (suffix == "startshutdown")
                    {
                        await StartCleanShutdown(context);
                    }
                    else if (suffix == "shutdown")
                    {
                        await StartFullShutdown(context);
                    }
                    else
                    {
                        await ReportError(context, HttpStatusCode.BadRequest, "unknown POST request " + suffix);
                    }
                }
                else if (request.HttpMethod.ToUpper() == "GET")
                {
                    if (suffix == "status")
                    {
                        await GetStatus(context);
                    }
                    else
                    {
                        await ReportError(context, HttpStatusCode.BadRequest, "unknown GET request " + suffix);
                    }
                }
            }
            else
            {
                await ReportError(context, HttpStatusCode.BadRequest, "unknown URI");
            }
        }

        public bool Start()
        {
            shutdown = new TaskCompletionSource<bool>();

            if (!StartServer())
            {
                shutdown.SetResult(false);
                return false;
            }

            server.Start(HandleRendezvous);

            List<Task> waiters = new List<Task>();
            foreach (var pg in processes.Values)
            {
                waiters.Add(pg.manager.StartWorkerProcessesAsync());
            }

            Task.WaitAll(waiters.ToArray());

            return true;
        }

        private void TriggerCleanShutdown()
        {
            foreach (var pg in processes.Values)
            {
                pg.manager.StartShuttingDown();
            }

            lock (this)
            {
                UpdateStatus(Constants.ServiceState.ShuttingDown, status.epoch + 1, 1);
            }
        }

        private void TriggerFullShutdown(int code, string eString)
        {
            TriggerCleanShutdown();

            lock (shutdown)
            {
                shutdown.TrySetResult(true);

                if (exitCode == 0)
                {
                    if (code != 0)
                    {
                        exitCode = code;
                        exitString = eString;
                    }
                }
            }
        }

        public int WaitForShutdown()
        {
            shutdown.Task.Wait();
            return exitCode;
        }

        public void Stop()
        {
            List<Task> waiters = new List<Task>();
            foreach (var pg in processes.Values)
            {
                waiters.Add(pg.manager.KillRunningProcessesAsync(exitCode == 0, exitString));
            }

            lock (this)
            {
                UpdateStatus(Constants.ServiceState.Stopped, status.epoch + 1, 1);
            }

            if (server != null)
            {
                server.Stop();
            }

            server = null;

            Task.WaitAll(waiters.ToArray());

            if (dfsClient != null)
            {
                dfsClient.Dispose();
                dfsClient = null;
            }
        }

        public void OnManagerTargetChanged(string group, int target)
        {
            lock (this)
            {
                var processGroup = processes[group];
                processGroup.targetNumberOfProcesses = target;
                Logger.Log("Group manager " + group
                    + " changed its target number of processes to "
                    + target);
                UpdateStatus(status.state, status.epoch + 1, 1);
            }
        }

        public void OnRegisterProcess(string group, string identifier)
        {
            lock (this)
            {
                var processGroup = processes[group];

                Debug.Assert(!processGroup.registration.ContainsKey(identifier));
                processGroup.registration[identifier] = null;

                Logger.Log("Got new process to manage: " + group + ":" + identifier);
            }
        }

        public void OnProcessExited(string group, string identifier, int exitCode)
        {
            bool shutdown = false;

            lock (this)
            {
                var processGroup = processes[group];

                Debug.Assert(processGroup.registration.ContainsKey(identifier));
                processGroup.registration.Remove(identifier);

                Logger.Log("Process no longer active: " + group + ":" + identifier);

                UpdateStatus(status.state, status.epoch, status.version + 1);

                if (status.state == Constants.ServiceState.ShuttingDown)
                {
                    if ((processGroup.shutdownOnGroupExit &&
                         processGroup.registration.Count == 0))
                    {
                        Logger.Log("Stopping service since all processes in " + group + " have exited");
                        shutdown = true;
                    }
                    else if (processes.Sum(x => x.Value.registration.Count) == 0)
                    {
                        Logger.Log("Stopping service since all processes have exited");
                        shutdown = true;
                    }
                }
            }

            if (shutdown)
            {
                // this is the exitcode of the last process in the group to stop, which we will
                // hope is meaningful
                TriggerFullShutdown(exitCode, "Service completed with exitcode " + exitCode);
            }
        }

        public void OnManagerFailed(string group, int exitCode)
        {
            Logger.Log("Group manager for " + group + " failed: shutting down");
            TriggerFullShutdown(exitCode, group + " failed with exit code " + exitCode);
        }

        static void Usage(ILogger logger)
        {
            logger.Log("Usage: " + Process.GetCurrentProcess().ProcessName + " [--debugBreak] [<azureDirectoryUri>] configFile.xml");
        }

        static Uri jobDirectoryUri = null;

        static async Task HeartBeat(ILogger logger, PersistentProcessManager server, Task<int> exited, string jobName)
        {
            try
            {
                DateTime startTime = DateTime.UtcNow;

                if (jobDirectoryUri.Scheme != "azureblob")
                {
                    logger.Log("Can't send heartbeat to non-Azure dfs " + jobDirectoryUri.AbsoluteUri);
                    return;
                }

                string account, key, container, directoryName;
                Azure.Utils.FromAzureUri(jobDirectoryUri, out account, out key, out container, out directoryName);

                // get the full name of the blob from the job directory
                string heartbeatBlobName = directoryName + "heartbeat";
                string killBlobName = directoryName + "kill";

                using (Azure.AzureDfsClient client = new Azure.AzureDfsClient(account, key, container))
                {
                    CloudPageBlob killBlob = client.Container.GetPageBlobReference(killBlobName);
                    CloudPageBlob heartbeatBlob = client.Container.GetPageBlobReference(heartbeatBlobName);

                    await heartbeatBlob.CreateAsync(512);
                    heartbeatBlob.Metadata["status"] = "running";
                    heartbeatBlob.Metadata["starttime"] = startTime.ToString();
                    if (jobName != null)
                    {
                        jobName = jobName.Replace('\r', ' ').Replace('\n', ' ');
                        try
                        {
                            heartbeatBlob.Metadata["jobname"] = jobName;
                        }
                        catch (Exception e)
                        {
                            logger.Log("Got exception trying to set jobname metadata to '" + jobName + "': " + e.ToString());
                            heartbeatBlob.Metadata["jobname"] = "[got exception setting job name]";
                        }
                    }

                    while (true)
                    {
                        logger.Log("Uploading heartbeat properties");
                        heartbeatBlob.Metadata["heartbeat"] = DateTime.UtcNow.ToString();
                        await heartbeatBlob.SetMetadataAsync();
                        logger.Log("Heartbeat sleeping");

                        Task<int> t = await Task.WhenAny(exited, Task.Delay(1000).ContinueWith((d) => { return 259; }));

                        int exitCode = t.Result;
                        string status = null;

                        if (t == exited)
                        {
                            status = (exitCode == 0) ? "success" : "failure";
                        }
                        else if (killBlob.Exists())
                        {
                            exitCode = 1;
                            status = "killed";
                            server.TriggerFullShutdown(1, "job was cancelled");
                        }

                        if (status != null)
                        {
                            logger.Log("Uploading final heartbeat properties " + exitCode + " " + status);
                            heartbeatBlob.Metadata["status"] = status;
                            await heartbeatBlob.SetMetadataAsync();
                            logger.Log("Heartbeat exiting");
                            return;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.Log("Heartbeat got exception " + e.ToString());
            }
        }

        static int Run(string[] args, ILogger logger)
        {
            TaskCompletionSource<int> exited = null;
            Task heartBeat = null;

            var newArgs = new List<string>();
            for (int i = 0; i < args.Length; ++i)
            {
                var arg = args[i];
                if (arg == "--debugBreak")
                {
                    logger.Log("Waiting for debugger");
                    while (!Debugger.IsAttached)
                    {
                        Thread.Sleep(1000);
                    }
                    Debugger.Break();
                }
                else
                {
                    newArgs.Add(arg);
                }
            }
            args = newArgs.ToArray();

            var server = new PersistentProcessManager();

            int exitCode = 1;

            if (args.Length < 1)
            {
                Usage(logger);
            }
            else
            {
                string configFile, heartBeatUri = null, jobName = null;

                if (args.Length > 1)
                {
                    if (args.Length > 2)
                    {
                        jobName = Utils.CmdLineDecode(args[2]);
                    }
                    exited = new TaskCompletionSource<int>();
                    configFile = args[1];
                    heartBeatUri = Utils.CmdLineDecode(args[0]);
                }
                else
                {
                    configFile = args[0];
                }

                if (heartBeatUri != null)
                {
                    string appId = Yarn.Utils.ApplicationIdFromEnvironment();
                    try
                    {
                        jobDirectoryUri = new Uri(heartBeatUri.Replace("_JOBID_", appId));
                    }
                    catch (Exception)
                    {
                        logger.Log("Bad job directory URI " + heartBeatUri);
                    }
                }

                if (server.Initialize(configFile, jobDirectoryUri, logger))
                {
                    if (heartBeatUri != null)
                    {
                        heartBeat = HeartBeat(logger, server, exited.Task, jobName);
                    }

                    server.Start();
                    exitCode = server.WaitForShutdown();
                    server.Stop();
                }
            }

            if (heartBeat != null)
            {
                exited.SetResult(exitCode);
                heartBeat.Wait();
            }

            return exitCode;
        }

        static void WriteErrorFile(string errorString)
        {
            if (jobDirectoryUri != null)
            {
                try
                {
                    if (jobDirectoryUri.Scheme == "azureblob")
                    {
                        using (Azure.AzureDfsClient azureClient = new Azure.AzureDfsClient(jobDirectoryUri))
                        {
                            azureClient.EnsureDirectory(jobDirectoryUri, false);
                            Uri path = azureClient.Combine(jobDirectoryUri, "ppmError.txt");
                            azureClient.PutDfsFile(path, System.Text.Encoding.UTF8.GetBytes(errorString));
                        }
                    }
                    else if (jobDirectoryUri.Scheme == "hdfs")
                    {
                        using (Hdfs.HdfsClient hdfsClient = new Hdfs.HdfsClient(Environment.UserName))
                        {
                            hdfsClient.EnsureDirectory(jobDirectoryUri, false);
                            Uri path = hdfsClient.Combine(jobDirectoryUri, "ppmError.txt");
                            hdfsClient.PutDfsFile(path, System.Text.Encoding.UTF8.GetBytes(errorString));
                        }
                    }
                }
                catch (Exception ee)
                {
                    Console.Error.WriteLine("Got exception uploading error " + errorString + "\nException: " + ee.ToString());
                }
            }
        }

        static void ExceptionHandler(object sender, UnhandledExceptionEventArgs args)
        {
            int result = 1;
            Exception e = args.ExceptionObject as Exception;
            string errorString = "Unknown exception";
            if (e != null)
            {
                result = System.Runtime.InteropServices.Marshal.GetHRForException(e);
                errorString = e.ToString();
            }

            WriteErrorFile(errorString);

            // We need to Exit, since other threads are likely to still be running.
            Environment.Exit(result);
        }

        internal class ConfigDependency : Shared.AssemblyDependencyAttribute
        {
            public ConfigDependency()
                : base(Constants.PeloponneseExeName +  ".config", false)
            {
            }
        }

        static int Main(string[] args)
        {
            SimpleLogger logger = new SimpleLogger("Peloponnese.log");

            int exitCode = 1;

            // Set unhandled exception handler
            AppDomain currentDomain = AppDomain.CurrentDomain;
            currentDomain.UnhandledException += new UnhandledExceptionEventHandler(ExceptionHandler);

            try
            {
                exitCode = Run(args, logger);
            }
            catch (Exception e)
            {
                WriteErrorFile(e.ToString());
                logger.Log("Application exception:\n" + e.ToString());
            }

            logger.Stop();

            return exitCode;
        }
    }
}
