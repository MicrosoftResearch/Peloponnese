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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Azure;
using Microsoft.Research.Peloponnese.ClusterUtils;

namespace AzureLogDumper
{
    class Program
    {
        static void Tail(Stream log)
        {
            Stream stdOut = Console.OpenStandardOutput();

            byte[] buffer = new byte[1024 * 1024];
            while (true)
            {
                int nRead = -1;
                while (nRead != 0)
                {
                    nRead = log.Read(buffer, 0, buffer.Length);
                    if (nRead > 0)
                    {
                        stdOut.Write(buffer, 0, nRead);
                        stdOut.Flush();
                    }
                }

                Thread.Sleep(1000);
            }
        }

        static void Main(string[] args)
        {
            Uri uri = new Uri(args[0]);
            string account, key, container, blob;
            Utils.FromAzureUri(uri, out account, out key, out container, out blob);
            if (key == null)
            {
                AzureSubscriptions subs = new AzureSubscriptions();
                key = subs.GetAccountKeyAsync(account).Result;
                uri = Utils.ToAzureUri(account, container, blob, null, key);
            }
            Stream log = new AzureLogReaderStream(uri);

            if (args.Length > 1 && args[1] == "-tail")
            {
                Tail(log);
            }
            else
            {
                log.CopyTo(Console.OpenStandardOutput());
            }
        }
    }
}
