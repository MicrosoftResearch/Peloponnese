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

// HdfsBridgeManaged.cpp : main project file.

//#include "stdafx.h"

#pragma managed

#pragma warning( disable: 4793 )

#include "JVMBridgeNative.h"
#include "JVMBridgeManaged.h"

using namespace System::Diagnostics;
using namespace System::IO;
using namespace System::Runtime::InteropServices;
using namespace System::Text;

namespace Microsoft { namespace Research { namespace Peloponnese
{
    static bool IsDllInPath(String^ dll)
    {
        String^ pathString = Environment::GetEnvironmentVariable("PATH");
        array<String^>^ pathDirs = pathString->Split(';');

        for (int i=0; i<pathDirs->Length; ++i)
        {
            String^ targetFile = Path::Combine(pathDirs[i], dll);
            if (File::Exists(targetFile))
            {
                return true;
            }
        }

        return false;
    }

    static void FindDll(String^ dll)
    {
        if (IsDllInPath(dll))
        {
            return;
        }

        String^ javaHome = Environment::GetEnvironmentVariable("JAVA_HOME");
        if (String::IsNullOrEmpty(javaHome))
        {
            throw gcnew ApplicationException("Peloponnese requires the JAVA_HOME environment variable to be set or jvm.dll to be on the path.");
        }

        String^ pathString = Environment::GetEnvironmentVariable("PATH");
        pathString += ";" + Path::Combine(javaHome, "jre", "bin", "server");
        Environment::SetEnvironmentVariable("PATH", pathString);

        if (!IsDllInPath(dll))
        {
            throw gcnew ApplicationException("Can't find " + dll + " in PATH " + pathString);
        }
    }

    static String^ MakeJavaClassPath()
    {
        String^ yarnEnvString = Environment::GetEnvironmentVariable("HADOOP_COMMON_HOME");
        if (yarnEnvString == nullptr)
        {
            throw gcnew Exception("Can't get HDFS class path since HADOOP_COMMON_HOME is not present in the environment");
        }

        String^ baseEnvString = Environment::GetEnvironmentVariable("HADOOP_HOME");
        if (baseEnvString == nullptr)
        {
            Environment::SetEnvironmentVariable("HADOOP_HOME", yarnEnvString);
        }

        StringBuilder^ sb = gcnew StringBuilder(16384);

        array<String^>^ jarFiles = Directory::GetFiles(yarnEnvString, "*.jar", SearchOption::AllDirectories);
        for (int i=0; i<jarFiles->Length; ++i)
        {
            sb->Append(jarFiles[i]);
            sb->Append(";");
        }

        String^ jars = sb->ToString();

        String^ additionalJar = Environment::GetEnvironmentVariable("PELOPONNESE_ADDITIONAL_CLASSPATH");
        if (additionalJar == nullptr)
        {
            String^ exePath = Process::GetCurrentProcess()->MainModule->FileName;
            String^ exeDir = Path::GetDirectoryName(exePath);
            additionalJar = Path::Combine(exeDir, "Microsoft.Research.Peloponnese.HadoopBridge.jar");
        }

        String^ configPath = Path::Combine(yarnEnvString, "etc", "hadoop");

        return additionalJar + ";" + configPath + ";" + jars;
    }

    bool Hadoop::Initialize()
	{
        if (HadoopNative::Initialized())
        {
            return true;
        }

        // Ensure that there is a jvm.dll on the path.
        FindDll("jvm.dll");

        // walk the HADOOP_HOME directory to get all the jars
        String^ classPath = MakeJavaClassPath();

		// Marshal the managed string to unmanaged memory.
        char* classPathCStr = (char*) Marshal::StringToHGlobalAnsi(classPath).ToPointer();

        bool success;

        success = HadoopNative::Initialize(classPathCStr);

        Marshal::FreeHGlobal(IntPtr(classPathCStr));

        return success;
	}
} } };