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

#pragma once

#pragma unmanaged

#if !(defined DllExport)
#  define DllExport
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <jni.h>
#include "Hadoop.h"

//---------------------------------------------------------------------------------------------------

namespace Microsoft { namespace Research { namespace Peloponnese
{
    class JVMBridge
    {
    public:
        static char* GetExceptionMessageLocal(JNIEnv* env, jclass cls, jobject obj);
        static JNIEnv* AttachToJvm();
    };
} } }