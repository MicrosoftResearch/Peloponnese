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

#define DllExport __declspec(dllexport)

#include "JVMBridgeNative.h"
#include "AppMasterNative.h"

#include <jni.h>
#include <windows.h>
#include <assert.h>
#include <Share.h>

using namespace Microsoft::Research::Peloponnese;

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Yarn
{
#define CHAR_BUFFER_SIZES 70000
    class InstanceInternal
    {
    public:
        jmethodID  m_midStart;
        jmethodID  m_midAddResource;
		jmethodID  m_midShutdown;
		jmethodID  m_midExit;
        jclass     m_clsInstance;
        jobject    m_obj;
    };
	
    AMNativeInstance::AMNativeInstance()
    {
        /*
        while (!::IsDebuggerPresent())
        {
            printf("Waiting for debugger\n");fflush(stdout);
            Sleep(1000);
        }
        ::DebugBreak();
        */
        m_inst = NULL;
    }

    AMNativeInstance::~AMNativeInstance()
    {
        JNIEnv* env = JVMBridge::AttachToJvm();

        if (m_inst != NULL && m_inst->m_obj != NULL)
        {
            env->DeleteGlobalRef(m_inst->m_obj);
        }

        delete m_inst;
        m_inst = NULL;
    }

    bool AMNativeInstance::OpenInstance(
        const char* jobGuid, const char* serverAddress, const char* groupName,
        const char* stdOut, const char* stdErr,
        int maxNodes, int maxMem, const char* processCmdLine,
        int maxFailuresPerNode, int maxTotalFailures)
    {
        //TODO Determine if we should detach the current thread from the jvm when exiting this call
        JNIEnv* env = JVMBridge::AttachToJvm();

		jclass clsAppMaster = env->FindClass("com/microsoft/research/peloponnese/AppMaster");
        if (clsAppMaster == NULL)
        {
			jthrowable exc;
			exc = env->ExceptionOccurred();
			if (exc) {
				 env->ExceptionDescribe();
				 env->ExceptionClear();
			}

            fprintf(stderr, "Failed to find AppMaster class\n");fflush(stderr);
            return false;
        }

		jmethodID midAMCons = env->GetMethodID(clsAppMaster, "<init>",
			"(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IILjava/lang/String;Ljava/lang/String;Ljava/lang/String;II)V");
        assert(midAMCons != NULL);

        jstring jjobGuid = env->NewStringUTF(jobGuid);
        jstring jserverAddr = env->NewStringUTF(serverAddress);
        jstring jgroupName = env->NewStringUTF(groupName);
        jstring jstdOut = env->NewStringUTF(stdOut);
        jstring jstdErr = env->NewStringUTF(stdErr);
        jint jmaxNodes = maxNodes;
		jint jmaxMem = maxMem;
        jstring jCmdLine = env->NewStringUTF(processCmdLine);
        jint jmaxFPN = maxFailuresPerNode;
        jint jmaxF = maxTotalFailures;
        jobject localInstance = env->NewObject(clsAppMaster, midAMCons,
            jjobGuid, jserverAddr, jgroupName, jmaxNodes, jmaxMem,
            jCmdLine, jstdOut, jstdErr, jmaxFPN, jmaxF);

        env->DeleteLocalRef(jjobGuid);
        env->DeleteLocalRef(jserverAddr);
        env->DeleteLocalRef(jgroupName);
        env->DeleteLocalRef(jstdOut);
        env->DeleteLocalRef(jstdErr);
        env->DeleteLocalRef(jCmdLine);

        if (localInstance == NULL)
        {
			jthrowable exc;
			exc = env->ExceptionOccurred();
			if (exc) {
				 env->ExceptionDescribe();
				 env->ExceptionClear();
			}

            fprintf(stderr, "Failed to initialize AppMaster\n");fflush(stderr);
            return false;
        }

		jmethodID midStart = env->GetMethodID(clsAppMaster, "start", "()V");
        if (midStart == NULL)
        {
            jthrowable exc;
            exc = env->ExceptionOccurred();
            if (exc) {
                env->ExceptionDescribe();
                env->ExceptionClear();
            }

            fprintf(stderr, "Failed to find AppMaster.start method\n");fflush(stderr);
            return false;
        }

		jmethodID midAddResource = env->GetMethodID(clsAppMaster, "addResource", "(Ljava/lang/String;Ljava/lang/String;JJZ)V");
        if (midAddResource == NULL)
        {
            jthrowable exc;
            exc = env->ExceptionOccurred();
            if (exc) {
                env->ExceptionDescribe();
                env->ExceptionClear();
            }

            fprintf(stderr, "Failed to find AppMaster.addResource method\n");fflush(stderr);
            return false;
        }

		jmethodID midShutdown = env->GetMethodID(clsAppMaster, "shutdown", "()V");
        if (midShutdown == NULL)
        {
            jthrowable exc;
            exc = env->ExceptionOccurred();
            if (exc) {
                env->ExceptionDescribe();
                env->ExceptionClear();
            }

            fprintf(stderr, "Failed to find AppMaster.shutdown method\n");fflush(stderr);
            return false;
        }

		jmethodID midExit = env->GetMethodID(clsAppMaster, "exit", "(ZZLjava/lang/String;)V");
        if (midExit == NULL)
        {
            jthrowable exc;
            exc = env->ExceptionOccurred();
            if (exc) {
                env->ExceptionDescribe();
                env->ExceptionClear();
            }

            fprintf(stderr, "Failed to find AppMaster.exit method\n");fflush(stderr);
            return false;
        }

        m_inst = new InstanceInternal();

        m_inst->m_clsInstance = clsAppMaster;
        m_inst->m_obj = env->NewGlobalRef(localInstance);
        env->DeleteLocalRef(localInstance);
        m_inst->m_midStart = midStart;
        m_inst->m_midAddResource = midAddResource;
		m_inst->m_midShutdown = midShutdown;
		m_inst->m_midExit = midExit;
        fprintf(stderr, "Created YARN Bridge Instance\n");fflush(stderr);
        return true;
    }

    char* AMNativeInstance::GetExceptionMessage()
    {
        JNIEnv* env = JVMBridge::AttachToJvm(); 
        return JVMBridge::GetExceptionMessageLocal(env, m_inst->m_clsInstance, m_inst->m_obj);
    }

    void AMNativeInstance::Start()
    {
        fprintf(stderr, "Starting app master connection to server\n");fflush(stderr);
        JNIEnv* env = JVMBridge::AttachToJvm(); 
        env->CallVoidMethod(m_inst->m_obj, m_inst->m_midStart);
    }

    void AMNativeInstance::AddResource(const char* localName, const char* uri, long long timeStamp, long long size, bool isPublic)
    {
        fprintf(stderr, "Adding resource %s=%s\n", localName, uri);fflush(stderr);
        JNIEnv* env = JVMBridge::AttachToJvm(); 

        jstring jlocal = env->NewStringUTF(localName);
        jstring juri = env->NewStringUTF(uri);
        jlong jtimeStamp = timeStamp;
        jlong jsize = size;
        jboolean jpublic = (isPublic) ? 1 : 0;
        env->CallVoidMethod(m_inst->m_obj, m_inst->m_midAddResource, jlocal, juri, jtimeStamp, jsize, jpublic);

        env->DeleteLocalRef(jlocal);
        env->DeleteLocalRef(juri);

        // detach here?
    }

	void AMNativeInstance::Shutdown()
    {
        fprintf(stderr, "Shutting down AMNativeInstance\n");fflush(stderr);
        JNIEnv* env = JVMBridge::AttachToJvm(); 
		fprintf(stderr, "Calling Shutdown\n");fflush(stderr);
		
        env->CallVoidMethod(m_inst->m_obj, m_inst->m_midShutdown);

        // detach here?
		fprintf(stderr, "Finished Shutdown\n");fflush(stderr);
    }

	void AMNativeInstance::Exit(bool success, const char* status)
    {
        fprintf(stderr, "Exiting AMNativeInstance\n");fflush(stderr);
        JNIEnv* env = JVMBridge::AttachToJvm(); 
		fprintf(stderr, "Calling Exit\n");fflush(stderr);
		
		jboolean jImmedShutdown = 0;
		jboolean jSuccess = (success) ? 1 : 0;
        jstring jStatus = env->NewStringUTF(status);

        env->CallVoidMethod(m_inst->m_obj, m_inst->m_midExit, jImmedShutdown, jSuccess, jStatus);

        env->DeleteLocalRef(jStatus);

        // detach here?
		fprintf(stderr, "Finished Exit\n");fflush(stderr);
    }

} } } }
