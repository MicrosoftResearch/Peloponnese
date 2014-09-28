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
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese;

namespace Microsoft.Research.Peloponnese.ClusterUtils
{
    class PeloponneseClient
    {
        private readonly Guid jobGuid;
        private readonly string serverAddress;
        private readonly string groupName;
        private readonly string processIdentifier;
        private readonly NotHttpClient.NotHttpClient httpClient;

        public Guid JobGuid { get { return jobGuid; } }
        public string ServerAddress { get { return serverAddress; } }
        public string GroupName { get { return groupName; } }
        public string ProcessIdentifier { get { return processIdentifier; } }

        public PeloponneseClient(XElement processDetails, ILogger logger)
        {
            httpClient = new NotHttpClient.NotHttpClient(false, 1, 10000, logger);

            string guidString = Environment.GetEnvironmentVariable(Constants.EnvJobGuid);
            if (guidString == null)
            {
                throw new ApplicationException("Can't find environment variable " + Constants.EnvJobGuid);
            }
            this.jobGuid = Guid.Parse(guidString);

            serverAddress = Environment.GetEnvironmentVariable(Constants.EnvManagerServerUri);
            if (serverAddress == null)
            {
                throw new ApplicationException("Can't find environment variable " + Constants.EnvManagerServerUri);
            }

            groupName = Environment.GetEnvironmentVariable(Constants.EnvProcessGroup);
            if (groupName == null)
            {
                throw new ApplicationException("Can't find environment variable " + Constants.EnvProcessGroup);
            }

            processIdentifier = Environment.GetEnvironmentVariable(Constants.EnvProcessIdentifier);
            if (processIdentifier == null)
            {
                throw new ApplicationException("Can't find environment variable " + Constants.EnvProcessIdentifier);
            }

            XElement details = new XElement("ProcessDetails");
            details.Add(processDetails);

            string status = details.ToString();

            string registration = String.Format("{0}register?guid={1}&group={2}&identifier={3}", serverAddress, jobGuid.ToString(), groupName, processIdentifier);
            NotHttpClient.IHttpRequest request = httpClient.CreateRequest(registration);
            // throw an exception and exit if we don't get the registration response within 30 seconds
            request.Timeout = 30 * 1000;
            request.Method = "POST";

            using (Stream upload = request.GetRequestStream())
            {
                using (StreamWriter sw = new StreamWriter(upload))
                {
                    sw.Write(status);
                }
            }

            using (NotHttpClient.IHttpResponse response = request.GetResponse())
            {
                // discard the response
            }
        }

        public void NotifyCleanShutdown()
        {
            StringBuilder sb = new StringBuilder(serverAddress);
            sb.Append("startshutdown");

            NotHttpClient.IHttpRequest request = httpClient.CreateRequest(sb.ToString());
            // throw an exception if we don't get the response within 30 seconds
            request.Timeout = 30 * 1000;
            request.Method = "POST";

            using (Stream rStream = request.GetRequestStream())
            {
                // no data
            }

            using (NotHttpClient.IHttpResponse status = request.GetResponse())
            {
                using (Stream response = status.GetResponseStream())
                {
                    // ignore
                }
            }
        }
    }
}
