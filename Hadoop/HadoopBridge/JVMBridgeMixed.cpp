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

#define DllExport __declspec(dllexport)

#include "JVMBridgeManaged.h"
#include "JVMBridgeNative.h"

// push unmanaged state on to stack and set managed state
#pragma managed(push, on)

static bool __stdcall InitializeHadoop()
{
    return Microsoft::Research::Peloponnese::Hadoop::Initialize();
}

#pragma managed(pop)

bool Microsoft::Research::Peloponnese::HadoopNative::Initialize()
{
    // make the managed code do the ugly work of initializing the class path
    return InitializeHadoop();
}