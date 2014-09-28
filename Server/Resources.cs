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
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Microsoft.Research.Peloponnese
{
    /// <summary>
    /// this interface is implemented by a process group manager, and used by the service
    /// manager to control the process group. The group communicates its actions back
    /// to the service manager using the <c>IServiceManager</c> interface passed to
    /// <c>Initialize</c>.
    ///
    /// Each group manager's config is specified in a ProcessGroup element that has at least
    /// the attributes name and type. These two attributes are read by the service manager
    /// and the type is used to decide on the concrete type of IProcessGroupManager to
    /// initialize. Other than these two attributes, the ProcessGroup element can contain
    /// any group-specific configuration information required. For example, a group of type
    /// local has an additional attribute indicating the number of local processes to spawn;
    /// a YARN manager might indicate the target number of containers per cluster computer,
    /// the maximum number of containers, etc.
    ///
    /// By convention the ProcessGroup element contains a child element of type Process
    /// defining the executable that will be run by all spawned processes. The Process element
    /// can be parsed by the <c>ExeDetails</c> class which reads the command line, any
    /// necessary environment variables, and the locations of resources---files that must
    /// be copied to the spawned process' local directory before it is started.
    /// </summary>
    interface IProcessGroupManager
    {
        /// <summary>
        /// the first method called on the IProcessGroupManager. It may signal errors by throwing
        /// an exception which will be safely caught by the parent.
        /// </summary>
        /// <param name="parent">
        /// the IServiceManager that callbacks should be delivered to
        /// </param>
        /// <param name="groupName">
        /// the name that the group should use to identify itself to the
        /// IServiceManager, and that it should assign (via <c>Constants.EnvProcessGroup</c>) to
        /// spawned processes
        /// </param>
        /// <param name="config">
        /// the ProcessGroup XML element read from the parent's config that
        /// should be used to initialize the group
        /// </param>
        void Initialize(IServiceManager parent, string groupName, XElement config);

        /// <summary>
        /// the method called by the parent when it is time to start spawning processes. The
        /// lifecycle of callbacks that should happen after this call is described in the
        /// documentation for <c>IServiceManager</c>. A process group may implement this as
        /// an async method if it contains blocking calls, however it is also fine to return
        /// (synchronously) immediately and perform all callbacks on queued worker threads.
        /// Any exceptions thrown will cause the service to terminate.
        /// </summary>
        Task StartWorkerProcessesAsync();

        /// <summary>
        /// the parent calls this method when the service is shutting down cleanly. After this
        /// method returns, the parent will tell clients that the service is shutting down, and
        /// spawned processes may start exiting. Spawned processes should not be restarted on
        /// exit after this call is received. Any exceptions thrown will cause the service to
        /// terminate.
        /// </summary>
        void StartShuttingDown();

        /// <summary>
        /// the parent calls this method when the service is stopping. On normal clean shutdown
        /// the spawned processes may already have exited before this call is received, but any
        /// that have not should be forcibly killed. The service may exit when this calls returns
        /// (i.e. when its Task completes) so the manager should make sure all processes have
        /// actually received a kill signal before returning. Any exceptions thrown will cause the
        /// service to terminate.
        /// </summary>
        /// <param name="success">true if the manager should signal success</param>
        /// <param name="status">a brief description of the exit status</param>
        Task KillRunningProcessesAsync(bool success, string status);
    }

    /// <summary>
    /// this is the interface a process group manager uses to communicate with its parent after
    /// <c>IProcessGroupManager.StartWorkerProcessesAsync</c> is called. All methods are thread
    /// safe. The group manager must implement a state machine that makes calls according to the
    /// following discipline:
    ///
    /// <c>OnManagerTargetChanged</c> is called before any calls to <c>OnRegisterProcess</c> and
    /// indicates the target number of processes that the manager is trying to spawn.
    ///
    /// <c>OnRegisterProcess</c> is called when a new process is registered; after this call the
    /// parent will listen for a registration POST to its web server from a process with the
    /// indicated group and identifier. Each identifier specified in a call to
    /// <c>OnRegisterProcess</c> must be unique over the lifetime of the group.
    /// 
    /// <c>OnProcessExited</c> is called when a process has died and should be removed from the
    /// parent's table of known processes. The identifier must be one that was passed to a previous
    /// call to <c>OnRegisterProcess</c> but has not been included in any previous call to
    /// <c>OnProcessExited</c>.
    /// 
    /// The number of processes in the manager's table is equal to the number of identifiers that
    /// have been passed in calls to <c>OnRegisterProcess</c> and not in calls to <c>OnProcessExited</c>.
    /// The number of processes must never exceed the number passed in <c>OnManagerTargetChanged</c>,
    /// so e.g. if the manager reduces the number of processes in its target it must wait until some
    /// of the surplus exit before notifying the parent that the target has changed.
    /// 
    /// <c>OnManagerFailed</c> should be called when there is a fatal error in the group manager (e.g. lost
    /// connection to the cluster) and causes the service to shut down immediately.
    /// </summary>
    interface IServiceManager
    {
        /// <summary>
        /// this property is used to get a handle to the logging subsystem
        /// </summary>
        ILogger Logger { get; }

        /// <summary>
        /// this property is used by a group manager to find out the Uri that the parent service is
        /// listening on. This Uri should be passed to spawned processes in the
        /// <c>Constants.EnvManagerServerUri</c> environment variable.
        /// </summary>
        string ServerAddress { get; }

        /// <summary>
        /// this property is used by a group manager to find out the Guid of the job. The Guid
        /// should be passed to spawned processes in the <c>Constants.EnvJobGuid</c> environment variable.
        /// </summary>
        Guid JobGuid { get; }

        /// <summary>
        /// called whenever the target number of spawned processes changes. The number of active
        /// processes (the number that have been registered and have not yet exited) must never
        /// exceed the most recently specified target
        /// </summary>
        /// <param name="groupName">the name of the group manager making the call</param>
        /// <param name="target">the number of processes the group is trying to spawn</param>
        void OnManagerTargetChanged(string groupName, int target);

        /// <summary>
        /// called whenever a new process is spawned. Puts the process in the parent's table
        /// after which the parent will accept a registration POST from the process. This method
        /// must be called <i>before</i> the process is started to avoid a race where the process
        /// attempts to POST its registration before the parent is willing to accept it
        /// </summary>
        /// <param name="groupName">the name of the group manager making the call</param>
        /// <param name="identifier">
        /// the identifier for the process, which must be unique within the process group over
        /// the lifetime of the service
        /// </param>
        void OnRegisterProcess(string groupName, string identifier);

        /// <summary>
        /// called whenever a process should be removed from the parent's table, either because it
        /// is known to have exited, or is in the process of being killed.
        /// </summary>
        /// <param name="groupName">the name of the group manager making the call</param>
        /// <param name="identifier">
        /// the identifier for the process, which must have been passed in to <c>OnRegisterProcess</c>
        /// previously, and not subsequently passed to <c>OnProcessExited</c>
        /// </param>
        /// <param name="exitCode">the exit code for the process</param>
        void OnProcessExited(string groupName, string identifier, int exitCode);

        /// <summary>
        /// called when the manager suffers a fatal error. This causes the service to exit.
        /// </summary>
        /// <param name="groupName">the name of the group manager making the call</param>
        /// <param name="exitCode">the exit code to return on failure</param>
        void OnManagerFailed(string groupName, int exitCode);
    }

    /// <summary>
    /// a resource group is a set of files that must be copied to a process' working directory
    /// before the process is started. Different concrete instances of resource groups are
    /// used to identify files stored in different places, e.g. on the local computer, in HDFS,
    /// in Azure blob storage, etc.
    /// </summary>
    public interface IResourceGroup
    {
        /// <summary>
        /// read any configuration information necessary to determine the location of the files
        /// and their names. Each resource group in an <c>ExeDetails</c> structure is identified
        /// by a ResourceGroup XML element with attribute type indicating the concrete type of
        /// the group. That XML element is passed to ReadFromConfig. It may signal errors by throwing
        /// an exception which will be safely caught by the parent.
        /// </summary>
        /// <param name="config">
        /// the ResourceGroup element describing the resources to be fetched
        /// </param>
        void ReadFromConfig(XElement config);

        /// <summary>
        /// copy all files to the local directory indicated. This may be implemented as an
        /// async call in which case it will be overlapped with copies of other resource
        /// groups. Returns true on success; false if any copy failed. If the call returns
        /// false the process will not be started. Any exceptions thrown will cause the
        /// service to terminate.
        /// </summary>
        /// <param name="localDirectory">
        /// the absolute path to the local directory where the files should be placed
        /// </param>
        /// <returns>true if and only if all resources in the group are successfully copied</returns>
        Task<bool> FetchToLocalDirectoryAsync(string localDirectory);
    }

    /// <summary>
    /// static methods for creating concrete resource groups and process groups depending on
    /// type strings read from the XML config
    /// </summary>
    class Factory
    {
        /// <summary>
        /// instantiate a concrete resource group based on a type string read from XML
        /// </summary>
        /// <param name="type">the name of the resource type, can be "local" or <i>TBD</i></param>
        /// <returns>the concrete resource group or null if the type is unknown</returns>
        public static IResourceGroup CreateResource(string type, ILogger logger)
        {
            if (type == "local")
            {
                return new LocalResources(logger);
            }
            else if (type == "hdfs")
            {
                return new HdfsResources(logger);
            }

            return null;
        }

        /// <summary>
        /// instantiate a concrete process group manager based on a type string read from XML
        /// </summary>
        /// <param name="type">the name of the process group type, can be "local" or <i>TBD</i></param>
        /// <returns>the concrete process group or null if the type is unknown</returns>
        public static IProcessGroupManager CreateManager(string type)
        {
            if (type == "local")
            {
                return new LocalProcessManager();
            }
            else if (type == "yarn")
            {
                return new YarnProcessManager();
            }

            return null;
        }
    }

    /// <summary>
    /// some helper tools for managing resources
    /// </summary>
    public class ResourceTools
    {
        /// <summary>
        /// compute the MD5 hash of a local file
        /// </summary>
        /// <param name="file">the path to the local file</param>
        /// <returns>a string encoding the hash of the file contents</returns>
        static public string MakeHash(string file)
        {
            var exeBytes = File.ReadAllBytes(file);
            var hash = new MD5CryptoServiceProvider().ComputeHash(exeBytes);
            var sb = new StringBuilder();
            for (int i = 0; i < hash.Length; ++i)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// a resource group consisting of files in a directory that can be read by the local computer
    /// using standard filesystem calls
    /// </summary>
    public class LocalResources : IResourceGroup
    {
        ILogger logger;

        /// <summary>
        /// the list of pathnames for the files
        /// </summary>
        List<string> resources;

        public LocalResources(ILogger l)
        {
            logger = l;
        }

        /// <summary>
        /// get the details of the file names and their location from an XML element
        /// </summary>
        /// <param name="config">the XML element containing config information</param>
        public void ReadFromConfig(XElement config)
        {
            resources = new List<string>();

            // the relative or absolute path prefix that identifies the location of the
            // files
            string localDirectory = config.Attribute("location").Value;

            // each filename is stored in the value of a separate Resource element. The
            // path for the resource is found by concatenating localDirectory with the leaf
            // name, so store the full path in the member variable.
            foreach (var e in config.Descendants("Resource"))
            {
                resources.Add(Path.Combine(localDirectory, e.Value));
            }
        }

        /// <summary>
        /// asynchronously copy a file to a target directory
        /// </summary>
        /// <param name="srcPath">the path of the file to be copied</param>
        /// <param name="dstDirectory">the destination directory</param>
        /// <returns>true if and only if the copy succeeded</returns>
        private async Task<bool> CopyFileAsync(string srcPath, string dstDirectory)
        {
            // find the leaf name from the path, and thus construct the path name
            // of the destination file
            var leafName = Path.GetFileName(srcPath);
            var dstPath = Path.Combine(dstDirectory, leafName);

            logger.Log("Copying local " + srcPath + " to " + dstPath);

            try
            {
                // open the source file
                using (var src = new FileStream(srcPath, FileMode.Open, FileAccess.Read))
                {
                    // open the destination file
                    using (var dst = new FileStream(dstPath, FileMode.Create, FileAccess.Write))
                    {
                        // do the asynchronous copy
                        await src.CopyToAsync(dst);
                    }
                }

                // if no exception was thrown, the copy succeeded
                return true;
            }
            catch (Exception e)
            {
                logger.Log("Copy local " + srcPath + " to " + dstPath + " failed with " + e.Message);
            }

            // an exception was thrown; the copy failed
            return false;
        }

        /// <summary>
        /// asynchronously copy all the resources to a local directory
        /// </summary>
        /// <param name="target">the path of the destination directory</param>
        /// <returns>true if and only if all the copies succeeded</returns>
        public async Task<bool> FetchToLocalDirectoryAsync(string target)
        {
            // start all the copies overlapped, returning an array of Task<bool> to
            // wait for
            var waiters = resources.Select(r => CopyFileAsync(r, target)).ToArray();

            // wait until all the copies finish
            var results = await Task.WhenAll(waiters);

            // return true if and only if all the copies returned true
            return results.Aggregate(true, (a, b) => a && b);
        }
    }

    /// <summary>
    /// a resource group consisting of files in an HDFS directory
    /// </summary>
    public class HdfsResources : IResourceGroup
    {
        /// <summary>
        /// helper class with the metadata about an HDFS file
        /// </summary>
        public class HdfsIdentifier
        {
            /// <summary>
            /// the leaf name of the remote file
            /// </summary>
            public string remoteName;
            /// <summary>
            /// the leaf name of the local file
            /// </summary>
            public string localName;
            /// <summary>
            /// the timestamp of the file, needed by YARN to reference a resource
            /// </summary>
            public long timestamp;
            /// <summary>
            /// the size of the file, 
            /// </summary>
            public long size;
        }

        ILogger logger;
        /// <summary>
        /// the HDFS directory where the files are located
        /// </summary>
        Uri directoryURI;
        /// <summary>
        /// the list of local names for the files
        /// </summary>
        List<HdfsIdentifier> files;
        /// <summary>
        /// this is true if the resources are shared across applications
        /// </summary>
        bool isPublic;

        /// <summary>
        /// gets the parent HDFS directory of the resource group
        /// </summary>
        public Uri DirectoryURI { get { return directoryURI; } }

        /// <summary>
        /// gets all the files referenced by the resource group
        /// </summary>
        public List<HdfsIdentifier> Files { get { return files; } }

        /// <summary>
        /// this is true if the resources are shared across applications
        /// </summary>
        public bool IsPublic { get { return isPublic; } }

        /// <summary>
        /// make a new empty resource group to be filled in using ReadFromConfig
        /// </summary>
        public HdfsResources(ILogger l)
        {
            logger = l;
        }

        /// <summary>
        /// make a new empty resource group. files can be added using EnsureResource
        /// </summary>
        /// <param name="directory">the HDFS directory represented by the group</param>
        /// <param name="isP">true if the HDFS directory contains public files</param>
        public HdfsResources(Uri directory, bool isP)
        {
            directoryURI = directory;
            isPublic = isP;
            files = new List<HdfsIdentifier>();
        }

        /// <summary>
        /// get the details of the file names and their location from an XML element
        /// </summary>
        /// <param name="config">the XML element containing config information</param>
        public void ReadFromConfig(XElement config)
        {
            files = new List<HdfsIdentifier>();

            // the relative or absolute path prefix that identifies the location of the
            // files
            directoryURI = new Uri(config.Attribute("location").Value);

            isPublic = (config.Attribute("public") != null && config.Attribute("public").Value == "true");

            bool needsLookup = false;

            // the metadata for each file is stored in a separate Resource element.
            foreach (var e in config.Descendants("Resource"))
            {
                var file = new HdfsIdentifier();

                var lnAttribute = e.Attribute("localName");
                if (lnAttribute == null)
                {
                    file.localName = e.Value;
                }
                else
                {
                    file.localName = lnAttribute.Value;
                }

                file.remoteName = e.Value;

                if (e.Attribute("timestamp") == null || e.Attribute("size") == null)
                {
                    needsLookup = true;
                    file.timestamp = -1;
                    file.size = -1;
                }
                else
                {
                    file.timestamp = long.Parse(e.Attribute("timestamp").Value);
                    file.size = long.Parse(e.Attribute("size").Value);
                }

                files.Add(file);
            }

            if (needsLookup)
            {
                using (var instance = new Hdfs.HdfsInstance(directoryURI))
                {
                    foreach (var r in files.Where(f => f.timestamp < 0 || f.size < 0))
                    {
                        var fileUri = new Uri(directoryURI, r.remoteName);
                        var info = instance.GetFileInfo(fileUri.AbsoluteUri, false);
                        r.timestamp = info.LastModified;
                        r.size = info.Size;
                    }
                }
            }
        }

        /// <summary>
        /// add the local file to the list of resources, adding it to HDFS decorated
        /// with its hash if necessary
        /// </summary>
        /// <param name="fileName">leaf name of the local file</param>
        /// <param name="localDirectory">local directory containing the file</param>
        public void EnsureResource(string fileName, string localDirectory)
        {
            var filePath = Path.Combine(localDirectory, fileName);

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Can't find resource", fileName);
            }

            var hash = ResourceTools.MakeHash(filePath);

            var remoteName = fileName + "." + hash;
            var hdfsFile = new Uri(directoryURI, remoteName).AbsoluteUri;
            using (var hdfs = new Hdfs.HdfsInstance(directoryURI))
            {
                if (!hdfs.IsFileExists(hdfsFile))
                {
                    hdfs.UploadAll(filePath, hdfsFile);
                }

                var info = hdfs.GetFileInfo(hdfsFile, false);

                var file = new HdfsIdentifier();
                file.remoteName = remoteName;
                file.localName = fileName;
                file.size = info.Size;
                file.timestamp = info.LastModified;

                files.Add(file);
            }
        }

        /// <summary>
        /// renders the resource group to an XML element that can be parsed later using
        /// ReadFromConfig
        /// </summary>
        /// <returns>the XML element corresponding to the group</returns>
        public XElement ToConfig()
        {
            var groupElement = new XElement("ResourceGroup");
            groupElement.SetAttributeValue("type", "hdfs");
            groupElement.SetAttributeValue("location", directoryURI.AbsoluteUri);
            if (isPublic)
            {
                groupElement.SetAttributeValue("public", "true");
            }

            foreach (var f in files)
            {
                var resourceElement = new XElement("Resource");
                if (f.localName != f.remoteName)
                {
                    resourceElement.SetAttributeValue("localName", f.localName);
                }
                resourceElement.SetAttributeValue("timestamp", f.timestamp);
                resourceElement.SetAttributeValue("size", f.size);
                resourceElement.Value = f.remoteName;

                groupElement.Add(resourceElement);
            }

            return groupElement;
        }

        /// <summary>
        /// asynchronously copy a file to a target directory
        /// </summary>
        /// <param name="instance">the HDFS instance of the file system</param>
        /// <param name="leafName">the leaf name of the file to be copied</param>
        /// <param name="dstDirectory">the destination directory</param>
        /// <returns>true if and only if the copy succeeded</returns>
        private async Task<bool> CopyFileAsync(Hdfs.HdfsInstance instance, string remoteName, string localName, string dstDirectory)
        {
            // find the leaf name from the path, and thus construct the path name
            // of the destination file
            var dstPath = Path.Combine(dstDirectory, localName);
            var srcURI = new Uri(directoryURI, remoteName);

            logger.Log("Copying HDFS " + srcURI.AbsoluteUri + " to " + dstPath);

            try
            {
                await Task.Run(() => instance.DownloadAll(srcURI.AbsoluteUri, dstPath));

                // if no exception was thrown, the copy succeeded
                return true;
            }
            catch (Exception e)
            {
                logger.Log("Copy hdfs " + srcURI + " to " + dstPath + " failed with " + e.Message);
            }

            // an exception was thrown; the copy failed
            return false;
        }

        /// <summary>
        /// asynchronously copy all the resources to a local directory
        /// </summary>
        /// <param name="target">the path of the destination directory</param>
        /// <returns>true if and only if all the copies succeeded</returns>
        public async Task<bool> FetchToLocalDirectoryAsync(string target)
        {
            try
            {
                using (var instance = new Hdfs.HdfsInstance(directoryURI))
                {
                    // start all the copies overlapped, returning an array of Task<bool> to
                    // wait for
                    var waiters = files.Select(r => CopyFileAsync(instance, r.remoteName, r.localName, target)).ToArray();

                    // wait until all the copies finish
                    var results = await Task.WhenAll(waiters);

                    // return true if and only if all the copies returned true
                    return results.Aggregate(true, (a, b) => a && b);
                }
            }
            catch (Exception e)
            {
                logger.Log("Got exception copying HDFS files: " + e.ToString());
                return false;
            }
        }
    }

    /// <summary>
    /// helper class for Process Groups, defining a process to be spawned whose configuration
    /// can be read from a standard Process XML element
    /// </summary>
    internal class ExeDetails
    {
        /// <summary>
        /// the command line to be used to start the process
        /// </summary>
        public string commandLine;

        public string commandLineArguments;

        /// <summary>
        /// directory to redirect stdout/stderr to, or null if
        /// we should use the working directory
        /// </summary>
        public string redirectDirectory;

        /// <summary>
        /// file to redirect stdout to, or null if no redirection is required
        /// </summary>
        public string stdoutFile;

        /// <summary>
        /// file to redirect stderr to, or null if no redirection is required
        /// </summary>
        public string stderrFile;

        /// <summary>
        /// a set of environment variables the process needs
        /// </summary>
        public Dictionary<string, string> environment;

        /// <summary>
        /// a set of resource groups defining files that must be copied to the process' working
        /// directory before it can be started
        /// </summary>
        public List<IResourceGroup> resources;

        /// <summary>
        /// read the process details from an XML element. Any exceptions thrown will be safely
        /// caught by the parent.
        /// </summary>
        /// <param name="config">the XML element containing the config</param>
        public void ReadFromConfig(XContainer config, ILogger logger)
        {
            // get the command line
            XElement cmdElement = config.Descendants("CommandLine").Single();

            XAttribute redirElement = cmdElement.Attribute(Constants.RedirectDirectoryAttribute);
            if (redirElement != null)
            {
                string envVariable = redirElement.Value;
                redirectDirectory = Environment.GetEnvironmentVariable(envVariable);
                if (redirectDirectory == null)
                {
                    logger.Log("Can't read variable " + envVariable + " for redirect directory");
                }
                else
                {
                    // deal with comma-separated YARN LOG_DIRS
                    redirectDirectory = redirectDirectory.Split(',').First().Trim();
                }
            }

            XAttribute stdoutElement = cmdElement.Attribute(Constants.RedirectStdOutAttribute);
            if (stdoutElement != null)
            {
                stdoutFile = stdoutElement.Value;
            }

            XAttribute stderrElement = cmdElement.Attribute(Constants.RedirectStdErrAttribute);
            if (stderrElement != null)
            {
                stderrFile = stderrElement.Value;
            }

            commandLine = cmdElement.Value;
            // get the command line arguments
            commandLineArguments = string.Join(" ", config.Descendants("CommandLineArguments")
                                       .Single().Descendants("Argument").Select(x => QuoteStringIfNecessary(x.Value)));

            // get the set of environment variables
            environment = new Dictionary<string, string>();
            foreach (var e in config.Descendants("Environment").Single().Descendants("Variable"))
            {
                environment.Add(e.Attribute("var").Value, e.Value);
            }

            // get the set of resource groups
            resources = new List<IResourceGroup>();
            foreach (var e in config.Descendants("ResourceGroup"))
            {
                // the type attribute determines what concrete resource group type
                // is used to describe the resources
                var rc = Factory.CreateResource(e.Attribute("type").Value, logger);

                // don't worry about throwing a null dereference exception if the type is
                // unknown
                rc.ReadFromConfig(e);

                // save the resource group so it can be used later to fetch the files
                resources.Add(rc);
            }
        }

        private string QuoteStringIfNecessary(string p)
        {
            if (!p.Contains('"'))
            {
                p = string.Format("\"{0}\"", p);
            }
            return p;
        }
    }
}
