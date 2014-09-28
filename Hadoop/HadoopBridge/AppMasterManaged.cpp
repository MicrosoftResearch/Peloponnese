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

#pragma managed

#include "JVMBridgeManaged.h"
#include "AppMasterNative.h"
#include "AppMasterManaged.h"

#include <string>
#include <iostream>

using namespace System;
using namespace System::Text;
using namespace System::IO;

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Yarn
{
    AMInstance::AMInstance(
        String^ jobGuid, String^ serverAddress, String^ groupName,
        String^ stdOut, String^ stdErr,
        int maxNodes, int maxMem, String^ processCmdLine,
        int maxFailuresPerNode, int maxTotalFailures)
    {
        if (!Hadoop::Initialize())
        {
            throw gcnew ApplicationException("Can't initialize Hadoop bridge");
        }

        AMNativeInstance *instance = new AMNativeInstance();

        IntPtr guidPtr = Marshal::StringToHGlobalAnsi(jobGuid);
        const char* guidString = static_cast<const char*>(guidPtr.ToPointer());
        IntPtr addrPtr = Marshal::StringToHGlobalAnsi(serverAddress);
        const char* addrString = static_cast<const char*>(addrPtr.ToPointer());
        IntPtr groupPtr = Marshal::StringToHGlobalAnsi(groupName);
        const char* groupString = static_cast<const char*>(groupPtr.ToPointer());
        IntPtr stdOutPtr = Marshal::StringToHGlobalAnsi(stdOut);
        const char* stdOutString = static_cast<const char*>(stdOutPtr.ToPointer());
        IntPtr stdErrPtr = Marshal::StringToHGlobalAnsi(stdErr);
        const char* stdErrString = static_cast<const char*>(stdErrPtr.ToPointer());
        IntPtr cmdPtr = Marshal::StringToHGlobalAnsi(processCmdLine);
        const char* cmdString = static_cast<const char*>(cmdPtr.ToPointer());

        if (instance->OpenInstance(
            guidString, addrString, groupString,
            stdOutString, stdErrString,
            maxNodes, maxMem, cmdString, maxFailuresPerNode, maxTotalFailures))
        {
            m_instance = IntPtr(instance);
        } 
        else
        {
            m_instance = IntPtr::Zero;
            Marshal::FreeHGlobal(guidPtr);
            Marshal::FreeHGlobal(addrPtr);
            Marshal::FreeHGlobal(groupPtr);
            Marshal::FreeHGlobal(stdOutPtr);
            Marshal::FreeHGlobal(stdErrPtr);
            Marshal::FreeHGlobal(cmdPtr);
            throw gcnew ApplicationException("Unable to initialize Yarn Native App Master Instance");
        }

        Marshal::FreeHGlobal(addrPtr);
        Marshal::FreeHGlobal(groupPtr);
        Marshal::FreeHGlobal(cmdPtr);
    }

    AMInstance::~AMInstance()
    {
        this->!AMInstance();
    }

    AMInstance::!AMInstance()
    {
        Close();
    }

    void AMInstance::Close()
    {
        if (m_instance != IntPtr::Zero)
        {
           AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
           delete instance;
           m_instance = IntPtr::Zero;
        }
    }

    void AMInstance::Shutdown()
    {
        if (m_instance != IntPtr::Zero)
        {
            AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
            instance->Shutdown();
        }
    }

    void AMInstance::Exit(bool success, String^ status)
    {
        if (m_instance != IntPtr::Zero)
        {
            IntPtr statusPtr = Marshal::StringToHGlobalAnsi(status);
            const char* statusString = static_cast<const char*>(statusPtr.ToPointer());

            AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
            instance->Exit(success, statusString);

            Marshal::FreeHGlobal(statusPtr);
        }
    }

    void AMInstance::Start()
    {
        AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
        instance->Start();
    }

    void AMInstance::AddResource(String^ localName, String^ uri, long long timeStamp, long long size, bool isPublic)
    {
        IntPtr namePtr = Marshal::StringToHGlobalAnsi(localName);
        const char* nameString = static_cast<const char*>(namePtr.ToPointer());
        IntPtr uriPtr = Marshal::StringToHGlobalAnsi(uri);
        const char* uriString = static_cast<const char*>(uriPtr.ToPointer());
        
        AMNativeInstance *instance =  (AMNativeInstance *) m_instance.ToPointer();
        instance->AddResource(nameString, uriString, timeStamp, size, isPublic);
        
        Marshal::FreeHGlobal(namePtr);
        Marshal::FreeHGlobal(uriPtr);
    }

    void AMInstance::UpdateTarget(int target)
    {      
        m_targetCallback(target);
    }

    void AMInstance::RegisterProcess(String^ processId, String^ nodeName)
    {      
        m_registerCallback(processId, nodeName);
    }

    void AMInstance::UpdateProcess(String^ processId, int state, int exitCode)
    {      
        m_updateCallback(processId, state, exitCode);
    }

    void AMInstance::ReportFailure()
    {
        m_failureCallback();
    }

    void AMInstance::RegisterCallbacks(
        UpdateNodeTarget^ target, RegisterProcessId^ registerProcess, UpdateProcessState^ update,
        ReportFailureCallback^ reportFailure)
    {
        m_targetCallback = target;
        m_registerCallback = registerProcess;
        m_updateCallback = update;
        m_failureCallback = reportFailure;
    }

}}}}
