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

#include <windows.h>
#include <assert.h>

static JavaVM* s_jvm = NULL;

BOOL APIENTRY DllMain( HMODULE /*hModule*/,
                       DWORD  ul_reason_for_call,
                       LPVOID /*lpReserved*/
                     )
{
    switch (ul_reason_for_call)
    {
        case DLL_PROCESS_ATTACH:
        case DLL_THREAD_ATTACH:
        case DLL_THREAD_DETACH:
        case DLL_PROCESS_DETACH:
            break;
    }
    return TRUE;
}

namespace Microsoft { namespace Research { namespace Peloponnese
{
	bool HadoopNative::Initialize(const char* classPath)
	{
        if (s_jvm != NULL)
		{
            fprintf(stderr, "Already done HadoopNative::Initialize\n");fflush(stderr);
			return true;
		}

		jsize bufLen = 1;
		jsize nVMs = -1;
		int ret = JNI_GetCreatedJavaVMs(&s_jvm, bufLen, &nVMs);
		if (ret < 0)
		{
			fprintf(stderr, "\nGetCreatedJavaVMs returned %d\n", ret);fflush(stderr);
			return false;
		}

		if (nVMs == 1)
		{
			fprintf(stderr, "\nProcess already contains %d Java VMs\n", nVMs);fflush(stderr);
			return true;
		}
        else if (nVMs > 1)
        {
            fprintf(stderr, "\nProcess already contains %d Java VMs\n", nVMs);fflush(stderr);
            return false;
        }

        JavaVMInitArgs vm_args;
		JNI_GetDefaultJavaVMInitArgs(&vm_args);        
        vm_args.version = JNI_VERSION_1_6;
        
#if 1
		JavaVMOption options[1]; // increment when turning on verbose JNI
		vm_args.nOptions = 1;
		vm_args.options = options;
		options[0].optionString = new char[_MAX_ENV];
        sprintf_s(options[0].optionString, _MAX_ENV, "-Djava.class.path=%s", classPath); 
#else
		JavaVMOption options[2]; // increment when turning on verbose JNI
		vm_args.nOptions = 2;
		vm_args.options = options;
		options[0].optionString = new char[_MAX_ENV];
        sprintf_s(options[0].optionString, _MAX_ENV, "-Djava.class.path=%s", classPath); 
		options[1].optionString = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044";
#endif
        //fprintf(stderr, "JNI_CLASSPATH:[%s]\n", options[0].optionString);
		//options[1].optionString = "-verbose:jni";
		/*
		vm_args.nOptions = 1;
		JavaVMOption options;
		options.optionString = "-verbose:jni";		
		vm_args.options = &options;
		*/
        vm_args.ignoreUnrecognized = 0;

		JNIEnv* env;
		ret = JNI_CreateJavaVM(&s_jvm, (void**) &env, &vm_args);

		delete [] options[0].optionString;

		if (ret < 0)
		{
			s_jvm = NULL;
			fprintf(stderr, "\nCreateJavaVM returned %d\n", ret);fflush(stderr);
			return false;
		}

		return true;
	}

    bool HadoopNative::Initialized()
    {
        return (s_jvm != NULL);
    }

	void HadoopNative::DisposeString(char* str)
	{
		free(str);
	}

    char* JVMBridge::GetExceptionMessageLocal(JNIEnv* env, jclass cls, jobject obj)
    {
	    jfieldID fidMessage = env->GetFieldID(
		    cls, "exceptionMessage", "Ljava/lang/String;");

	    assert(fidMessage != NULL);

	    jstring message = (jstring) env->GetObjectField(obj, fidMessage);

	    char* msg = NULL;

	    if (message == NULL)
	    {
		    msg = _strdup("<no message>");
	    }
	    else
	    {
		    const char* msgCopy = (const char*)(env->GetStringUTFChars(message, NULL));
		    msg = _strdup(msgCopy);
		    env->ReleaseStringUTFChars(message, msgCopy);
	    }

	    env->DeleteLocalRef(message);

	    return msg;
    }

    JNIEnv* JVMBridge::AttachToJvm() 
    {
	    JNIEnv* env;
	    int ret = s_jvm->AttachCurrentThread((void**) &env, NULL);

	    assert(ret == JNI_OK);

	    return env;
    }

} } };