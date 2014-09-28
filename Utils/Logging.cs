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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese
{
    /// <summary>
    /// class for logging. This is vestigial for now but could be arbitrarily improved
    /// </summary>
    public class SimpleLogger : ILogger
    {
        private Stream logStream;
        private StreamWriter log;

        private string TryOpen(string path)
        {
            try
            {
                logStream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read);
                return null;
            }
            catch (Exception e)
            {
                return e.ToString();
            }
        }

        private string Open(string logFileName)
        {
            string error1;
            string error2;
            string location = Environment.GetEnvironmentVariable("LOG_DIRS");
            if (location == null)
            {
                error1 = "No LOG_DIRS environment variable";
            }
            else
            {
                // deal with multiple log locations
                location = location.Split(',').First().Trim();
                // this is where we're supposed to be writing
                string path = Path.Combine(location, logFileName);
                error1 = TryOpen(Path.Combine(location, logFileName));
                if (error1 == null)
                {
                    // got a log: we're done
                    return null;
                }
            }

            // let's try the log filename in the working directory
            error2 = TryOpen(logFileName);
            if (error2 == null)
            {
                return "Failed to open log at LOG_DIRS location " + location + " because of " + error1 + " so using working directory";
            }

            // ok, we'll just do stdout then
            logStream = Console.OpenStandardOutput();
            return "Failed to open log at LOG_DIRS location " + location + " because of " + error1 +
                "; Failed to open log in working directory because of " + error2 + " so using standard out";
        }

        private void Flusher()
        {
            while (true)
            {
                System.Threading.Thread.Sleep(1000);
                lock (this)
                {
                    if (log == null)
                    {
                        // discarded
                        return;
                    }

                    log.Flush();
                }
            }
        }

        /// <summary>
        /// initialize the logging system
        /// </summary>
        public SimpleLogger(string logFileName)
        {
            string errors = Open(logFileName);
            log = new StreamWriter(logStream);

            Task.Run(() => Flusher());

            Log("-------- Starting logging --------");
            Log("Start time: " + DateTime.Now.ToLongTimeString());
            if (errors != null)
            {
                Log(errors);
            }
        }

        /// <summary>
        /// write a log entry
        /// </summary>
        /// <param name="entry">line to write to the log</param>
        public void Log(string entry,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            lock (this)
            {
                try
                {
                    if (log == null)
                    {
                        // discarded
                        return;
                    }

                    log.WriteLine("{0},TID={1,6:D6},{2},{3}:{4},{5}",
                        DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"),
                        Thread.CurrentThread.ManagedThreadId, function, file, line, entry);
                }
                catch (Exception)
                {
                    // discard if we can't write
                    try
                    {
                        log.Dispose();
                        logStream.Dispose();
                    }
                    catch
                    {
                    }

                    log = null;
                    logStream = null;
                }
            }
        }

        public void Stop()
        {
            Log("Stop time: " + DateTime.Now.ToLongTimeString());
            Log("-------- Stopping logging --------");

            lock (this)
            {
                try
                {
                    log.Dispose();
                    logStream.Dispose();
                }
                catch
                {
                }

                log = null;
                logStream = null;
            }
        }
    }

    public class ConsoleLogger : ILogger
    {
        /// <summary>
        /// write a log entry
        /// </summary>
        /// <param name="entry">line to write to the log</param>
        public void Log(string entry,
            [CallerFilePath] string file = "(nofile)",
            [CallerMemberName] string function = "(nofunction)",
            [CallerLineNumber] int line = -1)
        {
            Console.WriteLine("{0},TID={1,6:D6},{2},{3}:{4},{5}",
                              DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff"),
                              Thread.CurrentThread.ManagedThreadId, function, file, line, entry);
        }

        public void Stop()
        {
        }

        public static void EnsureLogger(ref ILogger logger)
        {
            if (logger == null)
            {
                logger = new ConsoleLogger();
            }
        }
    }
}
