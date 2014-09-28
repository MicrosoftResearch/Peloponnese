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
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Shared;

namespace Microsoft.Research.Peloponnese.ClusterUtils
{
    public class ResourceFile
    {
        /// <summary>
        /// compute the MD5 hash of a buffer
        /// </summary>
        /// <param name="buffer">the bytes to be hashed</param>
        /// <returns>a string encoding the hash of the file contents</returns>
        static private string MakeHash(byte[] buffer)
        {
            byte[] hash = new MD5CryptoServiceProvider().ComputeHash(buffer);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.Length; ++i)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }

        /// <summary>
        /// compute the MD5 hash of a local file
        /// </summary>
        /// <param name="file">the path to the local file</param>
        /// <returns>a string encoding the hash of the file contents</returns>
        static private string MakeHash(string file)
        {
            byte[] exeBytes = File.ReadAllBytes(file);
            return MakeHash(exeBytes);
        }

        static public Uri UploadResourceIfNeeded(DfsClient dfs, string localPath, Uri dfsDirectory,
                                                 out long modificationTime, out long size, ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            string suffix = MakeHash(localPath);
            string localFileName = Path.GetFileName(localPath);
            string dfsFileName = localFileName + "." + suffix;
            Uri dfsPath = dfs.Combine(dfsDirectory, dfsFileName);

            if (dfs.GetFileStatus(dfsPath, out modificationTime, out size))
            {
                return dfsPath;
            }

            logger.Log("Uploading " + dfsPath.AbsoluteUri);
            dfs.PutDfsFile(dfsPath, localPath);

            if (dfs.GetFileStatus(dfsPath, out modificationTime, out size))
            {
                return dfsPath;
            }

            throw new ApplicationException("Failed to upload resource " + localPath + " to " + dfsPath);
        }

        static public Uri UploadResourceIfNeeded(DfsClient dfs, string localFileName, byte[] bytes, Uri dfsDirectory,
                                                 out long modificationTime, out long size, ILogger logger = null)
        {
            ConsoleLogger.EnsureLogger(ref logger);

            string suffix = MakeHash(bytes);
            string dfsFileName = localFileName + "." + suffix;
            Uri dfsPath = dfs.Combine(dfsDirectory, dfsFileName);

            if (dfs.GetFileStatus(dfsPath, out modificationTime, out size))
            {
                return dfsPath;
            }

            logger.Log("Uploading " + dfsPath.AbsoluteUri);
            dfs.PutDfsFile(dfsPath, bytes);

            if (dfs.GetFileStatus(dfsPath, out modificationTime, out size))
            {
                return dfsPath;
            }

            throw new ApplicationException("Failed to upload resource " + localFileName + " to " + dfsPath);
        }
    }
}
