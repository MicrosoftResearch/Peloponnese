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

using namespace System::Reflection;
using namespace System::Runtime::InteropServices;

using namespace Microsoft::Research::Peloponnese::Shared;

namespace Microsoft { namespace Research { namespace Peloponnese
{
public ref class HadoopDependency : public AssemblyDependencyAttribute
{
public:
    HadoopDependency() : AssemblyDependencyAttribute("Microsoft.Research.Peloponnese.HadoopBridge.jar", false)
    {
    }
};

[assembly: HadoopDependency()];

private ref class YarnDependency : public AssemblyDependencyAttribute
{
public:
    YarnDependency() : AssemblyDependencyAttribute("Microsoft.Research.Peloponnese.YarnLauncher.jar", false)
    {
    }
};

[assembly: YarnDependency()];
}; }; };

[assembly: AssemblyTitle("Microsoft.Research.Peloponnese.HadoopBridge")];
[assembly: AssemblyDescription("")];
[assembly: AssemblyConfiguration("")];
[assembly: AssemblyCompany("Microsoft Corporation")];
[assembly: AssemblyProduct("Microsoft.Research.Peloponnese.HadoopBridge")];
[assembly: AssemblyCopyright("Copyright © Microsoft Corporation.  All rights reserved.")];
[assembly: AssemblyTrademark("")];
[assembly: AssemblyCulture("")];

[assembly: ComVisible(false)];

[assembly: AssemblyVersion("0.8.0.0")];
[assembly: AssemblyFileVersion("0.8.0.0")];
