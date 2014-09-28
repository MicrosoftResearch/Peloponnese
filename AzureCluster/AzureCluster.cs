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
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
using System.Xml.Linq;

using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Management.HDInsight;
using Microsoft.WindowsAzure.Management.Storage;
using Microsoft.WindowsAzure.Management.Storage.Models;

namespace Microsoft.Research.Peloponnese.ClusterUtils
{
    public class AzureCluster
    {
        private ClusterDetails _details;
        private string _name;
        private string _account;
        private string _key;
        private string _subscriptionId;
        private X509Certificate2 _certificate;

        internal AzureCluster(ClusterDetails details, AzureSubscription subscription)
        {
            _details = details;
            _name = _details.Name;
            _account = _details.DefaultStorageAccount.Name.Split('.').First();
            _key = _details.DefaultStorageAccount.Key;
            _subscriptionId = subscription.SubscriptionId;
            _certificate = subscription.Certificate;
        }

        internal AzureCluster(string name, string account, string key, string subscription, string thumbprint)
        {
            _details = null;
            _name = name;
            _subscriptionId = subscription;

            X509Store store = new X509Store();
            store.Open(OpenFlags.ReadOnly);
            _certificate = store.Certificates.Cast<X509Certificate2>().First(item => item.Thumbprint == thumbprint);
            if (_certificate == null)
            {
                throw new ApplicationException("No certificate found matching thumbprint " + thumbprint);
            }

            SetStorageAccount(account, key).Wait();
        }

        internal AzureCluster(string name, string account, string key, string subscription, X509Certificate2 certificate)
        {
            _details = null;
            _name = name;
            _subscriptionId = subscription;
            _certificate = certificate;

            SetStorageAccount(account, key).Wait();
        }

        public async Task SetStorageAccount(string account, string key)
        {
            _account = account;
            if (key == null)
            {
                if (_details == null)
                {
                    AzureSubscription subDetails = new AzureSubscription(_subscriptionId, _certificate);
                    Dictionary<string, AzureCluster> subClusters = await subDetails.GetClustersAsync();
                    if (subClusters.ContainsKey(Name))
                    {
                        _details = subClusters[Name].ClusterDetails;
                    }
                    else
                    {
                        throw new ApplicationException("No cluster " + Name + " configured for subscription " + _subscriptionId);
                    }
                }

                if (_details.DefaultStorageAccount.Name == account)
                {
                    _key = _details.DefaultStorageAccount.Key;
                }
                else
                {
                    IEnumerable<WabStorageAccountConfiguration> matching = _details.AdditionalStorageAccounts.Where(a => a.Name == account);
                    if (matching.Count() == 0)
                    {
                        throw new ApplicationException("No storage account " + account + " configured for cluster " + Name);
                    }
                    _key = matching.First().Key;
                }
            }
            else
            {
                _key = key;
            }
        }

        public ClusterDetails ClusterDetails { get { return _details; } }
        public string Name { get { return _name; } }
        public string StorageAccount { get { return _account; } }
        public string StorageKey { get { return _key; } }
        public string SubscriptionId { get { return _subscriptionId; } }
        public X509Certificate2 Certificate { get { return _certificate; } }
    }

    public class AzureSubscription
    {
        private readonly string _id;
        private readonly Guid _guid;
        private readonly X509Certificate2 _certificate;
        private readonly Task<Dictionary<string, AzureCluster>> _clusters;
        private readonly Task<Dictionary<string, string>> _storageAccounts;

        internal AzureSubscription(string id, string thumbprint)
        {
            _id = id;
            _guid = new Guid(id);

            X509Store store = new X509Store();
            store.Open(OpenFlags.ReadOnly);
            _certificate = store.Certificates.Cast<X509Certificate2>().First(item => item.Thumbprint == thumbprint);

            _clusters = Task.Run(() => ListClusters());
            _storageAccounts = ListStorageAccounts();
        }

        internal AzureSubscription(string id, X509Certificate2 certificate)
        {
            _id = id;
            _guid = new Guid(id);
            _certificate = certificate;

            _clusters = Task.Run(() => ListClusters());
            _storageAccounts = ListStorageAccounts();
        }

        private async Task<Dictionary<string, AzureCluster>> ListClusters()
        {
            Dictionary<string, AzureCluster> clusterList = new Dictionary<string, AzureCluster>();

            HDInsightCertificateCredential sCred = new HDInsightCertificateCredential(_guid, _certificate);
            IHDInsightClient sClient = HDInsightClient.Connect(sCred);

            ICollection<ClusterDetails> clusters = await sClient.ListClustersAsync();
            foreach (ClusterDetails cluster in clusters)
            {
                clusterList.Add(cluster.Name, new AzureCluster(cluster, this));
            }

            return clusterList;
        }

        private async Task<Dictionary<string, string>> ListStorageAccounts()
        {
            Dictionary<string, string> accountDictionary = new Dictionary<string, string>();

            CertificateCloudCredentials creds = new CertificateCloudCredentials(_id, _certificate);
            StorageManagementClient client = new StorageManagementClient(creds);
            StorageAccountListResponse accounts = await client.StorageAccounts.ListAsync();

            foreach (var account in accounts)
            {
                StorageAccountGetKeysResponse keys = await client.StorageAccounts.GetKeysAsync(account.Name);
                accountDictionary.Add(account.Name, keys.PrimaryKey);
            }

            return accountDictionary;
        }

        public Task<Dictionary<string, AzureCluster>> GetClustersAsync()
        {
            return _clusters;
        }

        public Task<Dictionary<string, string>> GetStorageAccountsAsync()
        {
            return _storageAccounts;
        }

        public string SubscriptionId { get { return _id; } }
        public X509Certificate2 Certificate { get { return _certificate; } }
    }

    /// <summary>
    /// manager that holds all subscriptions fetched from the Powershell defaults file
    /// </summary>
    public class AzureSubscriptions
    {
        private List<AzureSubscription> _subscriptions;
        private Dictionary<string, AzureCluster> _extraClusters;
        private Dictionary<string, string> _extraAccounts;

        public AzureSubscriptions()
        {
            _subscriptions = new List<AzureSubscription>();
            _extraClusters = new Dictionary<string, AzureCluster>();
            _extraAccounts = new Dictionary<string, string>();

            string configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "Windows Azure Powershell");
            string defaultFile = Path.Combine(configDir, "WindowsAzureProfile.xml");
            if (File.Exists(defaultFile))
            {
                using (FileStream s = new FileStream(defaultFile, FileMode.Open, FileAccess.Read))
                {
                    XDocument doc = XDocument.Load(s);
                    XNamespace ns = doc.Root.GetDefaultNamespace();
                    IEnumerable<XElement> subs = doc.Descendants(ns + "AzureSubscriptionData");
                    foreach (XElement sub in subs)
                    {
                        try
                        {
                            string thumbprint = sub.Descendants(ns + "ManagementCertificate").Single().Value;
                            string subscriptionId = sub.Descendants(ns + "SubscriptionId").Single().Value;
                            _subscriptions.Add(new AzureSubscription(subscriptionId, thumbprint));
                        }
                        catch { /* swallow any exceptions so we just return null values */ }
                    }
                }
            }
        }

        private IEnumerable<AzureCluster> ConcatenateClusters()
        {
            foreach (AzureCluster cluster in _extraClusters.Values)
            {
                yield return cluster;
            }

            foreach (AzureSubscription subscription in _subscriptions)
            {
                Dictionary<string, AzureCluster> clusters = subscription.GetClustersAsync().Result;
                foreach (AzureCluster cluster in clusters.Values)
                {
                    yield return cluster;
                }
            }
        }

        private IEnumerable<KeyValuePair<string, string>> ConcatenateAccounts()
        {
            foreach (KeyValuePair<string, string> account in _extraAccounts)
            {
                yield return account;
            }

            foreach (AzureSubscription subscription in _subscriptions)
            {
                Dictionary<string, string> accounts = subscription.GetStorageAccountsAsync().Result;
                foreach (KeyValuePair<string, string> account in accounts)
                {
                    yield return account;
                }
            }
        }

        public async Task<IEnumerable<AzureCluster>> GetClustersAsync()
        {
            foreach (AzureSubscription subscription in _subscriptions)
            {
                // just wait for it to get fetched
                await subscription.GetClustersAsync();
            }

            return ConcatenateClusters();
        }

        public IEnumerable<AzureCluster> GetClusters()
        {
            return GetClustersAsync().Result;
        }

        public async Task<IEnumerable<KeyValuePair<string, string>>> GetStorageAccountsAsync()
        {
            foreach (AzureSubscription subscription in _subscriptions)
            {
                // just wait for it to get fetched
                await subscription.GetStorageAccountsAsync();
            }

            return ConcatenateAccounts();
        }

        public IEnumerable<KeyValuePair<string, string>> GetStorageAccounts()
        {
            return GetStorageAccountsAsync().Result;
        }

        public async Task<AzureCluster> GetClusterAsync(string clusterName)
        {
            if (_extraClusters.ContainsKey(clusterName))
            {
                return _extraClusters[clusterName];
            }

            foreach (AzureSubscription subscription in _subscriptions)
            {
                Dictionary<string, AzureCluster> clusters = await subscription.GetClustersAsync();
                if (clusters.ContainsKey(clusterName))
                {
                    return clusters[clusterName];
                }
            }

            return null;
        }

        public async Task<string> GetAccountKeyAsync(string accountName)
        {
            if (_extraAccounts.ContainsKey(accountName))
            {
                return _extraAccounts[accountName];
            }

            foreach (AzureSubscription subscription in _subscriptions)
            {
                Dictionary<string, string> accounts = await subscription.GetStorageAccountsAsync();
                if (accounts.ContainsKey(accountName))
                {
                    return accounts[accountName];
                }
            }

            return null;
        }

        public void AddSubscription(string subscription, string thumbprint)
        {
            _subscriptions.Add(new AzureSubscription(subscription, thumbprint));
        }

        public void AddSubscription(string subscription, X509Certificate2 certificate)
        {
            _subscriptions.Add(new AzureSubscription(subscription, certificate));
        }

        public async Task SetClusterAccountAsync(string name, string account, string key)
        {
            AzureCluster existing = await GetClusterAsync(name);
            await existing.SetStorageAccount(account, key);
        }

        public void AddCluster(string name, string account, string key, string subscription, string thumbprint)
        {
            _extraClusters.Add(name, new AzureCluster(name, account, key, subscription, thumbprint));
        }

        public void AddCluster(string name, string account, string key, string subscription, X509Certificate2 certificate)
        {
            _extraClusters.Add(name, new AzureCluster(name, account, key, subscription, certificate));
        }

        public void AddAccount(string account, string key)
        {
            _extraAccounts.Add(account, key);
        }
    }

}
