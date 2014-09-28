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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class YarnApp 
{
    public static void ShowUsage(Log log)
    {
        log.error("Usage: YarnApp {--public <remoteName=localName=size=timestamp | --private <remoteName=localName=size=timeStamp}* <appName> <queueName> <amMemMb> <exeName> [<args>]");
        System.exit(1);
    }
    
    public static void SetResource(
            Log log,
            String remoteName, String localName,
            long timeStamp, long size,
            boolean isPublic,
            Map<String, LocalResource> localResources,
            String defaultFS)
    {
        try
        {
            URI remoteUri = new URI(remoteName);
            if (remoteUri.getAuthority() == null)
            {
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

        LocalResource amResource = Records.newRecord(LocalResource.class);
        amResource.setType(LocalResourceType.FILE);
        if (isPublic)
        {
            amResource.setVisibility(LocalResourceVisibility.PUBLIC);   
        }
        else
        {
            amResource.setVisibility(LocalResourceVisibility.PRIVATE);   
        }
        amResource.setResource(ConverterUtils.getYarnUrlFromPath(remotePath)); 
        amResource.setTimestamp(timeStamp);
        amResource.setSize(size);
        log.info("Adding resource " + remotePath.toUri() + "=" + localName + "=" + timeStamp + "=" + size);
        localResources.put(localName, amResource);    
    }

    private static void consumeArgs( String[] args, List<String> strippedArgs,
                                     Map<String, LocalResource> localResources, String defaultFs,
                                     Map<String, String> processEnv, Log log )
    {
        for (int i=0; i < args.length; ++i)
        {
            if (args[i].equals("--public"))
            {
                try
                {
                    ++i;
                    String localName = args[i];
                    ++i;
                    String remoteName = args[i];
                    ++i;
                    long size = Long.parseLong(args[i]);
                    ++i;
                    long timeStamp = Long.parseLong(args[i]);
                    SetResource(log, remoteName, localName, timeStamp, size, true, localResources, defaultFs);
                }
                catch (Exception e)
                {
                    ShowUsage(log);
                }
            }
            else if (args[i].equals("--private"))
            {
                try
                {
                    ++i;
                    String localName = args[i];
                    ++i;
                    String remoteName = args[i];
                    ++i;
                    long size = Long.parseLong(args[i]);
                    ++i;
                    long timeStamp = Long.parseLong(args[i]);
                    SetResource(log, remoteName, localName, timeStamp, size, false, localResources, defaultFs);
                }
                catch (Exception e)
                {
                    ShowUsage(log);
                }
            }
            else if (args[i].equals("--argfile"))
            {
                try
                {
                    ++i;
                    java.nio.file.Path p = java.nio.file.Paths.get(args[i]);
                    byte[] bytes = java.nio.file.Files.readAllBytes(p);
                    String file = new String(bytes);
                    String[] fileArgs = file.split("\\s");
                    consumeArgs(fileArgs, strippedArgs, localResources, defaultFs, processEnv, log);
                }
                catch (Exception e)
                {
                    ShowUsage(log);
                }
            }
            else
            {
                strippedArgs.add(args[i]);
            }
        }
    }

    public static void main( String[] args ) throws YarnException, IOException
    {
        Log log = LogFactory.getLog("PeloponneseYarnClient");

        YarnConfiguration yarnConf = new YarnConfiguration();

        // set the environment variable to enable log upload if desired
        Map<String, String> processEnv = new HashMap<String, String>();

        String defaultFs = yarnConf.get("fs.defaultFS");

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        
        List<String> strippedArgs = new ArrayList<String>();

        consumeArgs(args, strippedArgs, localResources, defaultFs, processEnv, log);

        if (strippedArgs.size() < 4)
        {
            ShowUsage(log);
        }
        
        String appName = strippedArgs.get(0);
	String queueName = strippedArgs.get(1);

        String serverArgs = strippedArgs.get(3);
        for (int i=4; i<strippedArgs.size(); ++i)
        {
            serverArgs = serverArgs + " " + strippedArgs.get(i);
        }

        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConf);
        client.start();

        YarnClientApplication app = client.createApplication();
        GetNewApplicationResponse response = app.getNewApplicationResponse();

        ApplicationId appId = response.getApplicationId();
        log.info("Got ApplicationId: " + appId);
        log.info("Max Resource Capability: " + response.getMaximumResourceCapability().getMemory());

        // request the min amount of memory, which should schedule the am on its own node
        int amMemory = response.getMaximumResourceCapability().getMemory();
	log.info("maxResource memory= " + amMemory);
        int maxNodeMemory = yarnConf.getInt("yarn.nodemanager.resource.memory-mb", amMemory);
	log.info("maxNodeMemory= " + maxNodeMemory);
	log.info("amMemory= " + amMemory);
	int cmdLineAmMemory = Integer.parseInt(strippedArgs.get(2));
	if (cmdLineAmMemory != -1) {
	    amMemory = Math.min(cmdLineAmMemory, amMemory);
	}
	log.info("cmdLineAmMemory= " + cmdLineAmMemory);
        amMemory = Math.min(amMemory, maxNodeMemory);

        log.info("Got maxMemory=" + amMemory);

        //int halfMemory = (amMemory / 2) + 1024;
        //amMemory = Math.min(amMemory, halfMemory);              
        //log.info("Max memory reduced to " + amMemory);

        log.info("Creating the ApplicationSubmissionContext");

        // Create a new ApplicationSubmissionContext
        ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
        log.info("Setting the ApplicationId");
        // set the ApplicationId 
        appContext.setApplicationId(response.getApplicationId());
        // set the application name
        appContext.setApplicationName(appName);
        // set the queue to the given queue
        appContext.setQueue(queueName);
	log.info("Submitting application to the queue: " + queueName);
        log.info("Getting a ContainerLaunchContext");
        // Create a new container launch context for the AM's container
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        log.info("Got a ContainerLaunchContext");

        // Set the local resources into the launch context    
        amContainer.setEnvironment(processEnv);
        amContainer.setLocalResources(localResources);

        // Construct the command to be executed on the launched container 
        String command = "microsoft.research.peloponnese.wrapper.cmd peloponnese-stdout.txt peloponnese-stderr.txt " + serverArgs;
        log.info("Command is " + command);
        List<String> commands = new ArrayList<String>();
        commands.add(command);
        amContainer.setCommands(commands);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);     

        client.submitApplication(appContext);

        System.out.println(appId);

        client.stop();
        client.close();
    }
}
