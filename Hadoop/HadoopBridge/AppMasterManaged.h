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

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Yarn
{
    public delegate void UpdateNodeTarget(int target);
    public delegate void RegisterProcessId(String^ processId, String^ nodeName);
    public delegate void UpdateProcessState(String^ processId, int state, int exitCode);
    public delegate void ReportFailureCallback();

    public ref class AMInstance : public IDisposable
    {
    public:
        AMInstance(
            String^ jobGuid, String^ serverAddress, String^ groupName,
            String^ stdOut, String^ stdErr,
			int maxNodes, int maxMem, String^ processCmdLine,
            int maxFailuresPerNode, int maxTotalFailures);
        ~AMInstance();
        !AMInstance();

        void Start();
        void Shutdown();
        void Exit(bool success, String^ status);
        void Close();

        void AddResource(String^ localName, String^ uri, long long timeStamp, long long size, bool isPublic);

		static void UpdateTarget(int target);
		static void RegisterProcess(String^ processId, String^ nodeName);
		static void UpdateProcess(String^ processId, int state, int exitCode);
        static void ReportFailure();
        static void RegisterCallbacks(
            UpdateNodeTarget^ updateTarget, RegisterProcessId^ registerProcess, UpdateProcessState^ update,
            ReportFailureCallback^ reportFailure);

    private:
        IntPtr                         m_instance;
        static UpdateNodeTarget^       m_targetCallback;
        static RegisterProcessId^      m_registerCallback;
        static UpdateProcessState^     m_updateCallback;
        static ReportFailureCallback^  m_failureCallback;
    };
}}}}
