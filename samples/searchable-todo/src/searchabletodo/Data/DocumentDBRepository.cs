using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using searchabletodo.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Web;

namespace searchabletodo.Data
{
    public static class DocumentDBRepository<T>
    {
        public static async Task<T> Get(Expression<Func<T, bool>> predicate)
        {
            Container c = await GetOrCreateCollection();
            return c.GetItemLinqQueryable<T>(allowSynchronousQueryExecution: true)
                        .Where(predicate)
                        .AsEnumerable()
                        .FirstOrDefault();
        }

        public static async Task<T> GetById(string id, string partitionKeyValue)
        {
            Container c = await GetOrCreateCollection();
            T doc = c.ReadItemAsync<T>(id, new PartitionKey(partitionKeyValue)).Result;
            return doc;
        }

        public static async Task<IEnumerable<T>> Find(Expression<Func<T, bool>> predicate)
        {
            Container c = await GetOrCreateCollection();
            var ret = c.GetItemLinqQueryable<T>(allowSynchronousQueryExecution: true)
                .Where(predicate)
                .ToList();

            return ret;
        }

        public static async Task<T> CreateAsync(T entity)
        {
            Container c = await GetOrCreateCollection();
            T doc = await c.CreateItemAsync(entity);
            return doc;
        }

        public static async Task<T> UpdateAsync(string id, T entity)
        {
            Container c = await GetOrCreateCollection();
            T doc = await c.ReplaceItemAsync(entity, id);
            return doc;
        }

        public static async Task DeleteAsync(string id, string partitionKeyValue)
        {
            Container c = await GetOrCreateCollection();
            await c.DeleteItemAsync<T>(id, new PartitionKey(partitionKeyValue));
        }

        private static Lazy<string> databaseId =
            new Lazy<string>(() => ConfigurationManager.AppSettings["docdb-database"]);
        public static string DatabaseId => databaseId.Value;
       
        private static Lazy<string> collectionId = 
            new Lazy<string>(() => ConfigurationManager.AppSettings["docdb-collection"]);
        public static string CollectionId => collectionId.Value;
        
        private static Lazy<string> partitionKeyPath = 
            new Lazy<string>(() => ConfigurationManager.AppSettings["docdb-partitionkeypath"]);
        public static string PartitionKeyPath => partitionKeyPath.Value;
        


        private static Database database;

        private static Container collection;

        private static CosmosClient client;
        private static CosmosClient Client
        {
            get
            {


                if (client == null)
                {
                    string endpoint = ConfigurationManager.AppSettings["docdb-endpoint"];
                    string authKey = ConfigurationManager.AppSettings["docdb-authKey"];

                    client = new CosmosClientBuilder(endpoint, authKey)
                                .WithApplicationRegion(Regions.WestEurope)
                                .Build();
                }

                return client;
            }
        }

        public static async Task<Container> GetOrCreateCollection(string databaseId, string collectionId)
        {
            Database db = Client.GetDatabase(databaseId);
            return await db.CreateContainerIfNotExistsAsync(collectionId, PartitionKeyPath);
        }

        public static async Task<Database> GetOrCreateDatabase(string databaseId)
        {
            return await Client.CreateDatabaseIfNotExistsAsync(databaseId);
        }

        public static async Task<Container> GetOrCreateCollection()
        {
            if (collection == null)
                collection = await GetOrCreateCollection(DatabaseId, CollectionId);
            return collection;
        }

        public static async Task<Database> GetOrCreateDatabase()
        {
            if (database == null)
                database = await GetOrCreateDatabase(DatabaseId);
            return database;
        }
    }
}