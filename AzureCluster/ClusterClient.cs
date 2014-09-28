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
using Microsoft.Hadoop.Client;
using Microsoft.WindowsAzure.Management.HDInsight;

using Microsoft.Research.Peloponnese.Shared;
using Microsoft.Research.Peloponnese.Azure;

using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Research.Peloponnese.ClusterUtils
{
    public class AzureYarnClient : ClusterClient
    {
        public string SubscriptionId { get; private set; }
        public string ClusterName { get; private set; }
        private JobSubmissionCertificateCredential credentials;
        public IJobSubmissionClient JobClient { get; private set; }

        private AzureDfsClient dfsClient;
        private Uri baseUri;
        public DfsClient DfsClient { get { return this.dfsClient; } }
        private readonly string peloponneseHome;

        public AzureYarnClient(AzureSubscriptions subscriptions, AzureDfsClient dfsClient, Uri baseUri,
                               string ppmHome, string clusterName = null)
        {
            this.dfsClient = dfsClient;
            this.baseUri = baseUri;
            this.peloponneseHome = ppmHome;

            IEnumerable<AzureCluster> clusters = subscriptions.GetClustersAsync().Result;

            AzureCluster cluster;
            if (clusterName == null)
            {
                if (clusters.Count() != 1)
                {
                    throw new ArgumentException("A cluster name must be provided if there is not exactly one configured HDInsight cluster.", "clusterName");
                }
                cluster = clusters.Single();
            }
            else
            {
                IEnumerable<AzureCluster> matching = clusters.Where(c => c.Name == clusterName);
                if (matching.Count() == 0)
                {
                    throw new ArgumentException("Cluster " + clusterName + " not attached to a Powershell subscription or specified manuall", "clusterName");
                }
                cluster = matching.First();
            }

            ClusterName = cluster.Name;
            SubscriptionId = cluster.SubscriptionId;

            Guid subscriptionGuid = new Guid(SubscriptionId);

            HDInsightCertificateCredential sCred = new HDInsightCertificateCredential(subscriptionGuid, cluster.Certificate);
            IHDInsightClient sClient = HDInsightClient.Connect(sCred);

            credentials = new JobSubmissionCertificateCredential(sCred, ClusterName);
            JobClient = JobSubmissionClientFactory.Connect(credentials);
        }

        public void Dispose()
        {
        }

        public Uri JobDirectoryTemplate
        {
            get {
                return Azure.Utils.ToAzureUri(
                    this.dfsClient.AccountName, "_BASELOCATION_", "_JOBID_/", null, this.dfsClient.AccountKey);
            }
        }

        public ClusterJob Submit(XDocument config, Uri errorLocation)
        {
            // upload config xml doc to blob 
            // launch single task map only job to submit xonfig xml to yarn launcher
            // get job output which will be the application id
            Uri userDfs = this.DfsClient.Combine(this.baseUri, "user", Environment.UserName, "staging");
            Guid jobGuid = Guid.NewGuid();

            StreamingMapReduceJobCreateParameters jobParams = new StreamingMapReduceJobCreateParameters();
            jobParams.Defines.Add("mapreduce.job.reduces", "0");
            jobParams.Defines.Add("mapreduce.job.maps", "1");
            jobParams.JobName = "YarnMapLauncher";
            jobParams.Mapper = "Microsoft.Research.Peloponnese.YarnMapLauncher.exe";
            Uri outputDir = this.DfsClient.Combine(userDfs, jobGuid.ToString()); 
            //dfsClient removes the leading slash, but hadoop assumes job output is relative to user working directory  
            jobParams.Output = this.dfsClient.GetClusterInternalUri(outputDir).AbsoluteUri;
            
            using (MemoryStream upload = new MemoryStream())
            {
                XmlWriterSettings settings = new XmlWriterSettings();
                settings.Encoding = new UTF8Encoding(false);
                using (XmlWriter xw = XmlWriter.Create(upload, settings))
                {
                    config.WriteTo(xw);
                }
                XElement elem = ConfigHelpers.MakeDfsResourceFromBuffer(jobGuid + ".xml", upload.ToArray(), this.DfsClient, userDfs);
                jobParams.Input = elem.Value;
            }

            Uri peloponneseDfs = this.DfsClient.Combine(this.baseUri, "staging", "peloponnese");
            string[] files = { jobParams.Mapper, "Microsoft.Research.Peloponnese.YarnLauncher.jar" };

            foreach (string file in files)
            {
                XElement elem = ConfigHelpers.MakeDfsResourceFromFile(Path.Combine(peloponneseHome, file),
                                                                      this.DfsClient, peloponneseDfs);
                // hash mark in resources allows a target name to be specified for the symlink
                jobParams.Files.Add(string.Format("{0}#{1}", elem.Value, file)); 
            }
            
            var launchJob = JobClient.CreateStreamingJob(jobParams);
            return new AzureYarnJob(this, errorLocation, outputDir, launchJob);
        }
    }

    public class AzureYarnJob : ClusterJob
    {

        private AzureYarnClient client;
        private JobCreationResults launchJob;
        private Uri outputDir;
        private Uri errorLocation;
        private string applicationId;
        private JobStatus status;
        private string errorMessage;
        private TaskCompletionSource<JobStatus> completion;
        private TaskCompletionSource<bool> started;

        internal AzureYarnJob(AzureYarnClient client, Uri errorLocation, Uri outputDir, JobCreationResults launchJob)
        {
            this.client = client;
            this.errorLocation = errorLocation;
            this.outputDir = outputDir;
            this.launchJob = launchJob;
            this.status = JobStatus.Waiting;
            this.completion = new TaskCompletionSource<JobStatus>();
            this.started = new TaskCompletionSource<bool>();

            Task.Run(() => MonitorJob());
        }

        private void SetException(Exception exception)
        {
            lock (this)
            {
                this.status = JobStatus.Failure;
                this.errorMessage = exception.Message;
            }

            this.completion.SetException(exception);
            this.started.TrySetResult(false);
        }

        private async Task<bool> WaitForStreamingLaunch()
        {
            var details = await this.client.JobClient.WaitForJobCompletionAsync(launchJob, TimeSpan.MaxValue, CancellationToken.None);
            if (details.StatusCode != JobStatusCode.Completed)
            {
                this.SetException(new ApplicationException("Streaming job to launch job failed: " + details.ErrorCode));
                return false;
            }

            // TODO: Investigate why GetJobOutput doesn't work.  Consider writing application id to stderr log as well
            /*
            using (Stream rs = JobClient.GetJobOutput(details.JobId))
            {
                using (StreamReader sr = new StreamReader(rs))
                {
                    string applicationId = sr.ReadToEnd();
                    if (String.IsNullOrWhiteSpace(applicationId))
                    {
                        throw new ApplicationException("Failed to launch job via Map only task - Empty output");
                    }
                    //TODO: Clean up staged job files with dfsClient                    
                    return new AzureYarnJob(this, applicationId);
                }
            }
             */

            //The job id will be in part-00000 in the output directory
            try
            {
                using (Stream s = this.client.DfsClient.GetDfsStreamReader(this.client.DfsClient.Combine(this.outputDir, "part-00000")))
                {
                    using (StreamReader sr = new StreamReader(s))
                    {
                        this.applicationId = (await sr.ReadToEndAsync()).Trim();
                        if (this.errorLocation != null)
                        {
                            this.errorLocation = new Uri(this.errorLocation.AbsoluteUri.Replace("_JOBID_", applicationId));
                        }
                    }
                }
            }
            catch (Exception e)
            {
                this.SetException(new ApplicationException("Failed to read streaming job output", e));
                return false;
            }
            //TODO: Clean up staged job files with dfsClient                    

            return true;
        }

        private async Task<bool> WaitForHeartBeat(CloudBlobContainer container, string blobName)
        {
            DateTime startWait = DateTime.UtcNow;

            while (true)
            {
                try
                {
                    CloudPageBlob blob = container.GetPageBlobReference(blobName);
                    if (blob.Exists())
                    {
                        return true;
                    }

                    DateTime endWait = DateTime.UtcNow;
                    TimeSpan duration = endWait - startWait;
                    if (duration.TotalSeconds > 2 * 60)
                    {
                        this.SetException(new ApplicationException("Job never wrote a heartbeat"));
                        return false;
                    }

                    await Task.Delay(2 * 1000);
                }
                catch (Exception e)
                {
                    this.SetException(new ApplicationException("Failed waiting for heartbeat", e));
                    return false;
                }
            }
        }

        private async Task<bool> WaitForStatus(CloudBlobContainer container, string blobName)
        {
            DateTime startWait = DateTime.UtcNow;

            while (true)
            {
                try
                {
                    CloudPageBlob blob = container.GetPageBlobReference(blobName);
                    blob.FetchAttributes();
                    if (blob.Metadata.ContainsKey("status") && blob.Metadata.ContainsKey("heartbeat"))
                    {
                        return true;
                    }

                    DateTime endWait = DateTime.UtcNow;
                    TimeSpan duration = endWait - startWait;
                    if (duration.TotalSeconds > 20)
                    {
                        this.SetException(new ApplicationException("Job never put attributes in heartbeat"));
                        return false;
                    }

                    await Task.Delay(1000);
                }
                catch (Exception e)
                {
                    this.SetException(new ApplicationException("Failed waiting for heartbeat attributes", e));
                    return false;
                }
            }
        }

        private async Task<JobStatus> WaitForExit(CloudBlobContainer container, string blobName)
        {
            try
            {
                TimeSpan offset;

                {
                    CloudPageBlob blob = container.GetPageBlobReference(blobName);
                    blob.FetchAttributes();
                    DateTime lastValue = DateTime.Parse(blob.Metadata["heartbeat"]);
                    offset = DateTime.UtcNow - lastValue;
                }

                while (true)
                {
                    CloudPageBlob blob = container.GetPageBlobReference(blobName);
                    blob.FetchAttributes();
                    if (blob.Metadata["status"] == "success")
                    {
                        return JobStatus.Success;
                    }
                    else if (blob.Metadata["status"] == "failure")
                    {
                        return JobStatus.Failure;
                    }
                    else if (blob.Metadata["status"] == "killed")
                    {
                        return JobStatus.Cancelled;
                    }

                    DateTime newValue = DateTime.Parse(blob.Metadata["heartbeat"]);
                    DateTime adjusted = newValue + offset;

                    TimeSpan staleness = DateTime.UtcNow - adjusted;

                    if (staleness.TotalSeconds > 20)
                    {
                        this.SetException(new ApplicationException("Job stopped writing heartbeats"));
                        return JobStatus.Cancelled;
                    }

                    await Task.Delay(2 * 1000);
                }
            }
            catch (Exception e)
            {
                this.SetException(new ApplicationException("Failed waiting for completion", e));
                return JobStatus.Cancelled;
            }
        }

        private async Task<bool> WaitForCompletion()
        {
            // at this point we know the job directory, so will be able to send a kill command if we need to
            this.started.SetResult(true);

            string account, key, container, blob;
            Azure.Utils.FromAzureUri(errorLocation, out account, out key, out container, out blob);

            using (AzureDfsClient dfs = new AzureDfsClient(account, key, container))
            {
                string heartBeat = blob + "heartbeat";

                bool started = await WaitForHeartBeat(dfs.Container, heartBeat);

                lock (this)
                {
                    if (started)
                    {
                        this.status = JobStatus.Running;
                    }
                    else
                    {
                        return false;
                    }
                }

                if (!await WaitForStatus(dfs.Container, heartBeat))
                {
                    return false;
                }

                JobStatus finished = await WaitForExit(dfs.Container, heartBeat);

                lock (this)
                {
                    if (finished == JobStatus.Success)
                    {
                        this.status = JobStatus.Success;
                        return true;
                    }
                    else if (finished == JobStatus.Cancelled)
                    {
                        this.status = JobStatus.Cancelled;
                        return true;
                    }
                }

                // there was a failure; try to read an error description
                string error = null;
                try
                {
                    using (Stream s = dfs.GetDfsStreamReader(dfs.Combine(errorLocation, "error.txt")))
                    {
                        using (StreamReader sr = new StreamReader(s))
                        {
                            error = await sr.ReadToEndAsync();
                        }
                    }
                }
                catch (Exception)
                {
                }

                lock (this)
                {
                    this.status = JobStatus.Failure;
                    this.errorMessage = error;
                }
            }

            return true;
        }

        private async Task MonitorJob()
        {
            bool started = await WaitForStreamingLaunch();

            if (!started)
            {
                return;
            }

            if (this.errorLocation == null)
            {
                lock (this)
                {
                    // we can't monitor, so just pretend it succeeded
                    this.status = JobStatus.Success;
                    this.started.SetResult(true);
                    this.completion.SetResult(this.status);
                }

                return;
            }

            bool finished = await WaitForCompletion();

            if (finished)
            {
                this.completion.SetResult(this.status);
            }
        }

        public JobStatus GetStatus()
        {
            lock (this)
            {
                return this.status;
            }
        }

        public string ErrorMsg {
            get {
                lock (this)
                {
                    return this.errorMessage;
                }
            }
        }

        public string Id { get { return this.applicationId; } }

        public void Join()
        {
            this.completion.Task.Wait();
        }

        public void Kill()
        {
            bool killStreaming = false;

            lock (this)
            {
                switch (this.status)
                {
                    case JobStatus.NotSubmitted:
                        throw new ApplicationException("Can't kill a job before it is submitted");

                    case JobStatus.Waiting:
                        killStreaming = true;
                        break;

                    case JobStatus.Running:
                        break;

                    case JobStatus.Success:
                    case JobStatus.Failure:
                    case JobStatus.Cancelled:
                        return;
                }
            }

            if (killStreaming)
            {
                JobStatusCode streamingStatus = this.client.JobClient.GetJob(launchJob.JobId).StatusCode;
                if (streamingStatus == JobStatusCode.Initializing)
                {
                    this.client.JobClient.StopJob(launchJob.JobId);
                }
                else if (streamingStatus == JobStatusCode.Running)
                {
                    JobDetails details = this.client.JobClient.WaitForJobCompletion(launchJob, TimeSpan.MaxValue, CancellationToken.None);
                    if (details.StatusCode != JobStatusCode.Completed)
                    {
                        // nothing to do
                        return;
                    }
                    bool mustKill = this.started.Task.Result;
                }
            }

            Azure.Utils.KillJob(errorLocation);
        }

        private JobStatus PpnJobStatusFromHdpJobStatus(JobStatusCode hadoopJobCode)
        {
            switch (hadoopJobCode)
            {
                case JobStatusCode.Canceled:
                    return JobStatus.Cancelled;
                case JobStatusCode.Completed:
                    return JobStatus.Success;
                case JobStatusCode.Failed:
                    return JobStatus.Failure;
                case JobStatusCode.Initializing:
                    return JobStatus.Waiting;
                case JobStatusCode.Running:
                    return JobStatus.Running;
                case JobStatusCode.Unknown:
                    throw new ApplicationException("'Unknown' state for job running on cluster");
            }
            throw new ApplicationException("Undefined state in JobStatusCode not implemented in Peloponnese JobStatus enum.");
        }
    }
}
