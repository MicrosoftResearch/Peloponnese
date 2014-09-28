/*
 * Naiad ver. 0.4
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.Shared.DependencyLister
{
    public class Lister : MarshalByRefObject
    {
        [Serializable]
        private class DependencyState
        {
            public readonly List<string> additionalAssemblies;
            public readonly HashSet<string> visited;

            public DependencyState(List<string> additionalAssemblies, HashSet<string> visited)
            {
                this.additionalAssemblies = additionalAssemblies;
                this.visited = visited;
            }
        }

        private static string[] FrameworkAssemblyNames = { "System", "System.Core", "mscorlib", "System.Xml" };

        /// <summary>
        /// Returns the non-framework assemblies on which a given assembly depends.
        /// </summary>
        /// <param name="source">The initial assembly</param>
        /// <returns>A set of non-framework assemblies on which the given assembly depends</returns>
        private static List<string> Dependencies(Assembly source, string binPath, HashSet<string> visited)
        {
            Queue<Assembly> assemblyQueue = new Queue<Assembly>();
            assemblyQueue.Enqueue(source);
            visited.Add(source.Location);

            HashSet<string> additionalAssemblies = new HashSet<string>();

            while (assemblyQueue.Count > 0)
            {
                Assembly currentAssembly = assemblyQueue.Dequeue();

                var attributeDependencies = currentAssembly
                    .GetCustomAttributes(typeof(AssemblyDependencyAttribute), false)
                    .Cast<AssemblyDependencyAttribute>()
                    .Select(dependency =>
                        new { isAssembly = dependency.IsAssembly,
                              path = Path.Combine(Path.GetDirectoryName(currentAssembly.Location), dependency.FileDependency) });

                foreach (var assembly in attributeDependencies)
                {
                    if (!visited.Contains(assembly.path))
                    {
                        if (assembly.isAssembly)
                        {
                            additionalAssemblies.Add(assembly.path);
                        }
                        else
                        {
                            visited.Add(assembly.path);
                        }
                    }
                }

                foreach (AssemblyName name in currentAssembly.GetReferencedAssemblies())
                {
                    Assembly referencedAssembly = Assembly.Load(name);
                    if (!visited.Contains(referencedAssembly.Location) && Path.GetFullPath(referencedAssembly.Location).ToLower().StartsWith(binPath))
                    {
                        visited.Add(referencedAssembly.Location);

                        assemblyQueue.Enqueue(referencedAssembly);
                    }
                }
            }

            return additionalAssemblies.Except(visited).ToList();
        }

        /// <summary>
        /// Returns the locations of non-framework assemblies on which the assembly with the given filename depends.
        /// </summary>
        /// <param name="assemblyFilename">The filename of the assembly</param>
        /// <returns>An array of filenames for non-framework assemblies on which the given assembly depends</returns>
        private DependencyState ListLoadedDependencies(string assemblyFilename, string binPath, HashSet<string> visited)
        {
            Assembly assembly = Assembly.LoadFrom(assemblyFilename);
            List<string> additionalAssemblies = Lister.Dependencies(assembly, binPath, visited);
            return new DependencyState(additionalAssemblies, visited);
        }

        /// <summary>
        /// Returns the locations of non-framework assemblies on which the assembly with the given filename depends.
        /// </summary>
        /// <param name="source">The filename of the assembly</param>
        /// <returns>An array of filenames for non-framework assemblies on which the given assembly depends</returns>
        private static DependencyState ListAssemblyDependencies(string assemblyFilename, string binPath, HashSet<string> visited)
        {
            Assembly source = Assembly.LoadFrom(assemblyFilename);

            AppDomainSetup setup = new AppDomainSetup();
            setup.ApplicationBase = Path.GetDirectoryName(source.Location);
            setup.ApplicationName = Path.GetFileName(source.Location);
            setup.PrivateBinPath = Path.GetDirectoryName(source.Location);
            setup.LoaderOptimization = LoaderOptimization.SingleDomain;

            string configFileName = source.Location + ".config";
            if (File.Exists(configFileName))
            {
                setup.ConfigurationFile = configFileName;
            }

            AppDomain dependencyDomain = AppDomain.CreateDomain("DependencyLister", null, setup);

            string listerAssembly = typeof(Microsoft.Research.Peloponnese.Shared.DependencyLister.Lister).Assembly.Location;
            DependencyLister.Lister lister = (DependencyLister.Lister)dependencyDomain.CreateInstanceFromAndUnwrap(listerAssembly, "Microsoft.Research.Peloponnese.Shared.DependencyLister.Lister");

            DependencyState ret = lister.ListLoadedDependencies(source.Location, binPath, visited);

            AppDomain.Unload(dependencyDomain);

            return ret;
        }

        public static IEnumerable<string> ListDependencies(string assemblyFilename)
        {
            if (Path.GetDirectoryName(assemblyFilename) == "")
            {
                assemblyFilename = @".\" + assemblyFilename;
            }

            string binPath = Path.GetFullPath(Path.GetDirectoryName(assemblyFilename)).ToLower();

            HashSet<string> visited = new HashSet<string>();
            Queue<string> assemblyQueue = new Queue<string>();
            assemblyQueue.Enqueue(assemblyFilename);

            while (assemblyQueue.Count > 0)
            {
                string currentAssemblyFilename = assemblyQueue.Dequeue();

                if (!visited.Contains(currentAssemblyFilename))
                {
                    DependencyState newState = ListAssemblyDependencies(currentAssemblyFilename, binPath, visited);

                    visited = newState.visited;

                    foreach (string newAssembly in newState.additionalAssemblies)
                    {
                        assemblyQueue.Enqueue(newAssembly);
                    }
                }
            }

            return visited;
        }
    }
}
