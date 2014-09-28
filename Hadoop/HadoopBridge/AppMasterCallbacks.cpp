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

#pragma unmanaged

#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
#include <windows.h>
#include <jni.h>

#include "AppMasterManaged.h"

// push unmanaged state on to stack and set managed state
#pragma managed(push, on)

static void __stdcall SendUpdatedTarget(int target)
{
    Microsoft::Research::Peloponnese::Yarn::AMInstance::UpdateTarget(target);
}

static void __stdcall SendProcessRegistration(const char* processId, const char *nodeName)
{
    System::String^ pidString = gcnew System::String(processId);
	System::String^ nodeNameString = gcnew System::String(nodeName);
    Microsoft::Research::Peloponnese::Yarn::AMInstance::RegisterProcess(pidString, nodeNameString);
}

static void __stdcall SendProcessState(const char* processId, int state, int exitCode)
{
    System::String^ pidString = gcnew System::String(processId);
    Microsoft::Research::Peloponnese::Yarn::AMInstance::UpdateProcess(pidString, state, exitCode);
}

static void __stdcall SendFailureReport()
{
    Microsoft::Research::Peloponnese::Yarn::AMInstance::ReportFailure();
}

#pragma managed(pop)

extern "C"
{
    JNIEXPORT void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_UpdateTargetNumberOfNodes(JNIEnv *env, jobject obj, jint target);
    JNIEXPORT void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_RegisterProcess(JNIEnv *env, jobject obj, jstring processId, jstring nodeName);
    JNIEXPORT void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_SendProcessState(JNIEnv *env, jobject obj, jstring processId, jint state, jint exitCode);
    JNIEXPORT void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_ReportFailure(JNIEnv *env, jobject obj);
}

void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_UpdateTargetNumberOfNodes(JNIEnv *env, jobject obj, jint target)
{
    SendUpdatedTarget(target);
}

void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_RegisterProcess(JNIEnv *env, jobject obj, jstring processId, jstring nodeName)
{
    const char* pidCopy = (const char*)(env->GetStringUTFChars(processId, NULL));
    const char* nodeNameCopy = (const char*)(env->GetStringUTFChars(nodeName, NULL));
    SendProcessRegistration(pidCopy, nodeNameCopy);
    env->ReleaseStringUTFChars(processId, pidCopy);
    env->ReleaseStringUTFChars(nodeName, nodeNameCopy);
}

void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_SendProcessState(JNIEnv *env, jobject obj, jstring processId, jint state, jint exitCode)
{
    const char* pidCopy = (const char*)(env->GetStringUTFChars(processId, NULL));
    SendProcessState(pidCopy, (int) state, (int) exitCode);
    env->ReleaseStringUTFChars(processId, pidCopy);
}

void JNICALL Java_com_microsoft_research_peloponnese_AppMaster_ReportFailure(JNIEnv *env, jobject obj)
{
    SendFailureReport();
}
