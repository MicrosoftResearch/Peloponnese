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

package com.microsoft.research.peloponnese;

//import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import java.io.IOException;
import java.lang.StringBuilder;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records; 

public class AppMaster
{
	private Log log;
	private YarnConfiguration yarnConf;
	private final String jobGuid;
	private final String serverAddress;
	private final String groupName;
	private final String stdOutName;
	private final String stdErrName;
	private ApplicationAttemptId appAttemptID;

	private AtomicBoolean shuttingDown;
	private AtomicBoolean exiting;
	private AtomicBoolean scheduleProcesses;

	private final int maxFailuresPerNode;
	private final int maxTotalFailures;
	private AtomicInteger totalFailures;
	private ConcurrentMap<String, FailureCount> failureCount;
	private final int maxStartFailures;
	private AtomicInteger startFailures;
	private ConcurrentMap<String, FailureCount> startFailureCount;
	
	private int outstandingContainerAsk;
	private int fulfilledContainerAsk;
	private int nextContainerAsk;

	private int maxNodes;
	private int clusterNodeCount = -1;
	private ConcurrentMap<ContainerId, ProcessInfo> runningContainers;

	private String appMasterHostName;
	private int maxMemory;
	private final String processCmdLine;
	private ByteBuffer allTokens;
	private HashMap<String,LocalResource> resources;

	private AMRMClientAsync<ContainerRequest> amRMClient;
	private NMClientAsync nmClientAsync;
	private NMCallbackHandler containerListener;
	
	private static int YTS_NA = 0;
	private static int YTS_Scheduling = 1;
	private static int YTS_Running = 2;
	private static int YTS_Completed = 3;
	private static int YTS_Failed = 4;
	private static int YTS_StartFailed = 5;

	private native void UpdateTargetNumberOfNodes(int target);
	private native void RegisterProcess(String processId, String nodeName); 
	private native void SendProcessState(String processId, int state, int exitCode);
	private native void ReportFailure();
	
	private class FailureCount
	{
		public int count;
	}

	private class ProcessInfo 
	{
		public final String processId;
		public final String nodeName;

		public ProcessInfo(String pid, String node) {
			processId = pid;
			nodeName = node;
		}
	}
	
	private int onContainerFailedOrCompleted(ContainerId cid, int containerState, int exitStatus)
	{
		int numberToRestart = 0;
		// Need to notify graph manager of current state
		ProcessInfo pi = runningContainers.remove(cid);
		if (pi == null) {
			log.warn("Couldn't find container " + cid + " to remove from running map");
		}
		else
		{
			log.info("Removed container " + cid + " from running map");
			SendProcessState(pi.processId, containerState, exitStatus);

			if (!shuttingDown.get()) {
				if (containerState == YTS_StartFailed)
				{
					synchronized (startFailureCount)
					{
						FailureCount failed = failureCount.get(pi.nodeName);
						if (failed == null)
						{
							failed = new FailureCount();
							failed.count = 0;
							failureCount.put(pi.nodeName, failed);
						}
						++failed.count;
						log.info("Node " + pi.nodeName + " has had " + failed.count + " start failures");
					}
					int newStartFailures = startFailures.incrementAndGet();
					log.info("There have been " + newStartFailures + " start failures");

					if ((maxStartFailures > 0 && newStartFailures >= maxStartFailures))
					{
						log.info("Too many start failures");
						ReportFailure();
						numberToRestart = -1;
					}
					else
					{
						// start another container to replace this one
						numberToRestart = 1;
					}
				}
				else
				{
					int numberOfFailures;
					synchronized (failureCount)
					{
						FailureCount failed = failureCount.get(pi.nodeName);
						if (failed == null)
						{
							failed = new FailureCount();
							failed.count = 0;
							failureCount.put(pi.nodeName, failed);
						}
						++failed.count;
						numberOfFailures = failed.count;
						log.info("Node " + pi.nodeName + " has had " + failed.count + " failures");
					}
					int newTotalFailures = totalFailures.incrementAndGet();
					log.info("There have been " + newTotalFailures + " failures");

					if ((maxFailuresPerNode > 0 && numberOfFailures >= maxFailuresPerNode) ||
							(maxTotalFailures > 0 && newTotalFailures >= maxTotalFailures))
					{
						log.info("Too many failures");
						ReportFailure();
						numberToRestart = -1;
					}
					else
					{
						// start another container to replace this one
						numberToRestart = 1;
					}
				}
			}
		}
		
		return numberToRestart;
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler
	{
		private synchronized int maybeGetAsk() {
			if (outstandingContainerAsk == 0)
			{
				outstandingContainerAsk = nextContainerAsk;
				nextContainerAsk = 0;
				return outstandingContainerAsk;
			}
			else
			{
				return 0;
			}
		}
		
		private synchronized void fulfilPreviousAsk(int fulfilled) {
			assert(fulfilled <= outstandingContainerAsk);
			if (fulfilled > 0)
			{
				log.info("Removing requests for " + fulfilled + " requests");

				for (int i=0; i<fulfilled; ++i)
				{
					amRMClient.removeContainerRequest(MakeContainerRequest());
				}
				
				outstandingContainerAsk -= fulfilled;
			}
		}
		
		private synchronized void boxcarAskRequest(int numberToAsk) {
			nextContainerAsk += numberToAsk;
			log.info("Postponing ask for " + numberToAsk + " containers: total now " + nextContainerAsk);
		}

		@Override
		public float getProgress() {
			// is this the first time getProgress has been called? If so, find out how
			// many cluster nodes there are, and schedule processes accordingly
			if (scheduleProcesses.compareAndSet(true, false)) {
				//don't schedule a process where the graph manager is running
				int numProcessesToStart = amRMClient.getClusterNodeCount() - 1;
				if (maxNodes > 0)
				{
					numProcessesToStart = Math.min(numProcessesToStart, maxNodes);
				}
				log.info("There are " + amRMClient.getClusterNodeCount()
						+ " nodes in the cluster. maxNodes = " + maxNodes);
				UpdateTargetNumberOfNodes(numProcessesToStart);
				boxcarAskRequest(numProcessesToStart);
			}

			int oldNodeCount = clusterNodeCount;
			clusterNodeCount = amRMClient.getClusterNodeCount();
			if (clusterNodeCount != oldNodeCount) {
				log.info("There are now " + clusterNodeCount + " available nodes on the cluster.");
			}
			
			int toSchedule = maybeGetAsk();
			
			if (toSchedule > 0)
			{
				log.info("Requesting " + toSchedule + " processes");
				scheduleProcess(toSchedule);
			}

			return 0.01f; // NYI
		}

		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			log.info("Got response from RM for container ask, allocatedCnt="
					+ allocatedContainers.size());
			
			fulfilPreviousAsk(allocatedContainers.size());

			for (final Container allocatedContainer : allocatedContainers) {
				launchContainer(allocatedContainer, containerListener);
			}
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			
			int toSchedule = 0;

			for (ContainerStatus containerStatus : completedContainers) {
				ContainerId cid = containerStatus.getContainerId();
				log.info("Got container status for containerID= " 
						+ cid + ", state=" + containerStatus.getState()     
						+ ", exitStatus=" + containerStatus.getExitStatus() 
						+ ", diagnostics=" + containerStatus.getDiagnostics());

				int containerState = 0;
				if (containerStatus.getState() == ContainerState.COMPLETE) {
					if (containerStatus.getExitStatus() == 0) {
						containerState = YTS_Completed;
					} else {
						containerState = YTS_Failed;                    
					}
				} else {
					log.error("Container finished without a COMPLETE status. containerID=" + cid);
					containerState = YTS_Failed;
				}

				int restartThis = onContainerFailedOrCompleted(cid, containerState, containerStatus.getExitStatus());
				if (restartThis < 0)
				{
					toSchedule = -1;
				}
				else if (toSchedule >= 0)
				{
					toSchedule += restartThis;
				}
			}
			
			if (toSchedule > 0)
			{
				boxcarAskRequest(toSchedule);
			}
		}

		@Override
		public void onError(Throwable arg0) {
			log.error("Got error from async client");
		}

		@Override
		public void onNodesUpdated(List<NodeReport> arg0) {
		}

		@Override
		public void onShutdownRequest() {
			log.error("Got shutdown request from async client");
		}
		
		private ContainerRequest MakeContainerRequest()
		{
			Priority priority = Records.newRecord(Priority.class);
			priority.setPriority(1);

			Resource capability = Records.newRecord(Resource.class);
			capability.setMemory(maxMemory);

			return new ContainerRequest(capability, null, null, priority);
		}

		public void scheduleProcess(int numProcesses) 
		{
			log.info("Scheduling " + numProcesses + " processes.");

			for (int i=0; i<numProcesses; ++i)
			{
				amRMClient.addContainerRequest(MakeContainerRequest());
			}
		}

		private void launchContainer(Container container, NMCallbackHandler cm)
		{
			log.info("Setting up container launch container for containerid=" + container.getId());
			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
			
			String pid = groupName + ".Container" + String.format("%1$06d", container.getId().getId());
			String hostName = container.getNodeId().getHost();

			ProcessInfo pi = new ProcessInfo(pid, hostName);

			// set environment variables
			Map<String, String> processEnv = new HashMap<String, String>();
			processEnv.put("PELOPONNESE_JOB_GUID", jobGuid);
			processEnv.put("PELOPONNESE_SERVER_URI", serverAddress);
			processEnv.put("PELOPONNESE_PROCESS_GROUP", groupName); 
			processEnv.put("PELOPONNESE_PROCESS_IDENTIFIER", pid);
			processEnv.put("PELOPONNESE_PROCESS_HOSTNAME", hostName);

			ctx.setEnvironment(processEnv);
			ctx.setLocalResources(resources);

			String commandLine = "microsoft.research.peloponnese.wrapper.cmd " + stdOutName + " " + stdErrName + " " + processCmdLine;
			log.info("Launching a container with command line '" +  
					processCmdLine + "'" + " for process " + pi.processId +
					" on host " + pi.nodeName);

			List<String> commands = new ArrayList<String>();
			commands.add(commandLine);
			ctx.setCommands(commands);

			ctx.setTokens(allTokens);
						
			RegisterProcess(pid, hostName);

			log.info("Adding container " + container.getId() + " to running map");
			runningContainers.put(container.getId(), pi);

			containerListener.addContainer(container.getId(), container);
			nmClientAsync.startContainerAsync(container, ctx);
		}
	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler
	{
		private ConcurrentMap<ContainerId, Container> containers =
				new ConcurrentHashMap<ContainerId, Container>();
		private RMCallbackHandler rmHandler;
		
		public NMCallbackHandler(RMCallbackHandler rmH)
		{
			this.rmHandler = rmH;
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> arg1) {
			log.info("Started container " + containerId);
			Container container = containers.get(containerId);
			if (container != null && !exiting.get()) {
				nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
			}
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId,
				ContainerStatus containerStatus) {
			log.info("Container " + containerId + " is in the " +  containerStatus.getState() + " state");
			if (containerStatus.getState() == ContainerState.RUNNING) {
				ProcessInfo pi = runningContainers.get(containerId);
				if (pi != null) {
					log.info("Calling SendProcessState()");
					// exit code 259 is STILL_ACTIVE
					SendProcessState(pi.processId, YTS_Running, 259);
					log.info("Returned from SendProcessState()");
				}
				else
				{
					log.warn("No running container entry");
				}
			} else {
				log.warn("May not send running state");
			}
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			log.info("Container " + containerId + " is stopped");
			containers.remove(containerId);
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable e) {
			log.error("Failed to query the status of container " + containerId);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable e) {
			log.error("Failed to start container " + containerId + " " + e.toString());
			containers.remove(containerId);
			
			amRMClient.releaseAssignedContainer(containerId);
			
			int restartThis = onContainerFailedOrCompleted(containerId, YTS_StartFailed, 1);
			
			log.info("Restart count is " + restartThis);
			
			if (restartThis > 0)
			{
				rmHandler.boxcarAskRequest(restartThis);
			}
			
			log.info("Finished error handler");
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable e) {
			log.error("Failed to stop container " + containerId);
		}
	}

	static {
		Log slog = LogFactory.getLog("AppMaster");
		slog.info("About to load Microsoft.Research.Peloponnese.HadoopBridge library");
		System.loadLibrary("Microsoft.Research.Peloponnese.HadoopBridge");
		slog.info("Loaded Microsoft.Research.Peloponnese.HadoopBridge library");
	}

    public AppMaster(String jGuid, String sAddr, String gName, int maxN, int maxMem, String cmdLine, String stdOut, String stdErr, int maxFPerNode, int maxF) throws IOException
	{
		log = LogFactory.getLog("AppMaster");
		log.info("In AppMaster constructor");
		
		jobGuid = jGuid;
		serverAddress = sAddr;
		groupName = gName;
		stdOutName = stdOut;
		stdErrName = stdErr;

		exiting = new AtomicBoolean(false);
		shuttingDown = new AtomicBoolean(false);
		scheduleProcesses = new AtomicBoolean(true);

		runningContainers =  new ConcurrentHashMap<ContainerId, ProcessInfo>();
		resources = new HashMap<String, LocalResource>();

		maxFailuresPerNode = maxFPerNode;
		maxTotalFailures = maxF;
		failureCount = new ConcurrentHashMap<String, FailureCount>();
		totalFailures = new AtomicInteger(0);
		
		maxStartFailures = Math.max(10, maxN*2);
		startFailureCount = new ConcurrentHashMap<String, FailureCount>();
		startFailures = new AtomicInteger(0);
		
		outstandingContainerAsk = 0;
		fulfilledContainerAsk = 0;
		nextContainerAsk = 0;

		Map<String, String> envs = System.getenv();
		String containerIdString = envs.get(Environment.CONTAINER_ID.name());

		if (containerIdString == null) {
			// container id should always be set in the env by the framework 
			StringBuilder sb = new StringBuilder(4096);
			for(Map.Entry<String, String> entry : envs.entrySet())
			{
				sb.append("\n\tKey: '");
				sb.append(entry.getKey());
				sb.append("'\tValue: '");
				sb.append(entry.getValue());
				sb.append("'");
			}

			log.error("Couldn't find container id in environment strings.  Environment: " + sb); 
			throw new IllegalArgumentException("ContainerId not set in the environment");
		}

		appMasterHostName = envs.get("COMPUTERNAME");  // WINDOWS ONLY
		if (appMasterHostName == null) {
		    throw new IllegalArgumentException("COMPUTERNAME not set in the environment");
		}

		ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
		appAttemptID = containerId.getApplicationAttemptId();

		maxNodes = maxN;
		maxMemory = maxMem;
		log.info("Command line max memory= " + maxMemory);
		processCmdLine = cmdLine;

		yarnConf = new YarnConfiguration();

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
	}

	public void start() throws YarnException, IOException
	{
		log.isDebugEnabled();
		RMCallbackHandler allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(yarnConf);
		amRMClient.start();

		containerListener = new NMCallbackHandler(allocListener);
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(yarnConf);
		nmClientAsync.start();

		log.info("Registering AppMaster");
		RegisterApplicationMasterResponse response = amRMClient
				.registerApplicationMaster(appMasterHostName, -1, "");
		log.info("AppMaster registered");
		int maxResourceMemory = response.getMaximumResourceCapability().getMemory();
		if (maxMemory != -1) {
		    maxMemory = Math.min(maxMemory, maxResourceMemory);
		} else {
		    maxMemory = maxResourceMemory;
		}
		int maxNodeMemory = yarnConf.getInt("yarn.nodemanager.resource.memory-mb", maxMemory);
		maxMemory = Math.min(maxMemory, maxNodeMemory);
		log.info("Max memory " + maxMemory);
		//int halfMemory = (maxMemory / 2) + 1024;
		//maxMemory = Math.min(maxMemory, halfMemory);		
		//log.info("Max memory reduced to " + maxMemory);
	}
	
	public void addResource(String localName, String remoteName, long timeStamp, long size, boolean isPublic)
	{
        try
        {
            URI remoteUri = new URI(remoteName);
            if (remoteUri.getAuthority() == null)
            {
            	String defaultFS = yarnConf.get("fs.defaultFS");
            	log.info("Rewriting naked resource " + remoteName);
                remoteName = defaultFS + remoteUri.getPath();
                log.info("Replaced by " + remoteName);
            }
        }
        catch (java.net.URISyntaxException e)
        {
            log.info("Malformed URI " + remoteName);
        }
            
		Path remotePath = new Path(remoteName);

		LocalResource ctResource = Records.newRecord(LocalResource.class);
		ctResource.setType(LocalResourceType.FILE);
		if (isPublic)
		{
			ctResource.setVisibility(LocalResourceVisibility.PUBLIC);   
		}
		else
		{
			ctResource.setVisibility(LocalResourceVisibility.PRIVATE);   
		}
		ctResource.setResource(ConverterUtils.getYarnUrlFromPath(remotePath));
		ctResource.setTimestamp(timeStamp);
		ctResource.setSize(size);

		log.info("Adding resource " + localName + "=" + remotePath.toUri());
		resources.put(localName, ctResource);
	}
	
	public void shutdown()
	{
		shuttingDown.set(true);
	}

	class StopContainerThread extends Thread {
		public void run() {
			log.info("Stopping containers");
			nmClientAsync.stop();
		}
	}

	public void exit(boolean immediateShutdown, boolean success, String status)
	{ 
		shutdown();
		// using an atomicboolean here doesn't really work, because it seems that the nmClientAsync gets confused
		// if it gets any calls after we call stop, and this atomicboolean introduces a race that could allow
		// the client to get more calls to launch or request status even after it is set.
		exiting.set(true);

		StopContainerThread stopThread = new StopContainerThread();
		stopThread.start();
		log.info("Sleeping to wait for containers to stop");
		try
		{
			stopThread.join(30000);
		}
		catch (InterruptedException e)
		{
			log.info("Got interrupted exception " + e.toString());
		}

		log.info("Telling RM we are shutting down");

		try {
			amRMClient.unregisterApplicationMaster(
					(success) ? FinalApplicationStatus.SUCCEEDED : FinalApplicationStatus.FAILED,
							status, null);
		} catch (YarnException ex) {
			log.error("Failed to unregister application", ex);
		} catch (IOException e) {
			log.error("Failed to unregister application", e);
		}

		amRMClient.stop();

		log.info("Client stopped");
	}
}
