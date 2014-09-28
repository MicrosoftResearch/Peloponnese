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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Peloponnese.Yarn
{
    public class Utils
    {
        private static string YarnAppIdFromContainerId(string containerId)
        {
            string[] components = containerId.Split('_');
            if (components.Length != 5)
            {
                throw new ApplicationException("Malformed container ID " + containerId);
            }
            return String.Format("application_{0}_{1}", components[1], components[2]);
        }

        public static string GetYarnApplicationId()
        {
            string env = Environment.GetEnvironmentVariable("CONTAINER_ID");
            if (env == null)
            {
                return null;
            }
            else
            {
                return YarnAppIdFromContainerId(env);
            }
        }

        public static string ApplicationIdFromEnvironment()
        {
            string yarnAppId = GetYarnApplicationId();
            string peloponneseJobId = Environment.GetEnvironmentVariable("PELOPONNESE_JOB_GUID");

            if (yarnAppId != null)
            {
                return yarnAppId;
            }
            else if (peloponneseJobId != null)
            {
                return peloponneseJobId;
            }
            else
            {
                return "UnknownApp";
            }
        }
    }
}
