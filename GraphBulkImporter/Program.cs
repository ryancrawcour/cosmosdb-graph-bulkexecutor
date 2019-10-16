using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph;
using GraphBulkImporter.Models;

namespace GraphBulkImporter
{ 
    class Program
    {
        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        private static DocumentClient client;

        public async static Task Main(string[] args)
        {
            Trace.WriteLine("Summary:");
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine($"Endpoint: {EndpointUrl}");
            Trace.WriteLine($"Collection : {DatabaseName}.{CollectionName}");
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine("");

            try
            {
                using (client = new DocumentClient(
                    new Uri(EndpointUrl),
                    AuthorizationKey,
                    ConnectionPolicy))
                {
                    await RunBulkImportAsync();
                }
            }
            catch (AggregateException e)
            {
                Trace.TraceError($"Caught AggregateException in Main, Inner Exception:\n{e.ToString()}");
                Console.ReadKey();
            }

        }

        private async static Task RunBulkImportAsync()
        {
            // Cleanup on start if set in config.
            DocumentCollection dataCollection = null;
            try
            {
                if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]))
                {
                    Database database = Utils.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null)
                    {
                        await client.DeleteDatabaseAsync(database.SelfLink);
                    }

                    Trace.TraceInformation($"Creating database {DatabaseName}");
                    database = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                    Trace.TraceInformation($"Creating collection {CollectionName} with {CollectionThroughput} RU/s");
                    dataCollection = await Utils.CreatePartitionedCollectionAsync(client, DatabaseName, CollectionName, CollectionThroughput);
                }
                else
                {
                    dataCollection = Utils.GetCollectionIfExists(client, DatabaseName, CollectionName);
                    if (dataCollection == null)
                    {
                        throw new Exception("The data collection does not exist");
                    }
                }
            }
            catch (Exception de)
            {
                Trace.TraceError($"Unable to initialize, exception message:\n{de.ToString()}");
                throw;
            }

            // Prepare for bulk import.

            // Creating documents with simple partition key here.
            string partitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");
            long numberOfDocumentsToGenerate = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToImport"]);

            // Set retry options high for initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            IBulkExecutor graphbulkExecutor = new GraphBulkExecutor(client, dataCollection);
            await graphbulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            BulkImportResponse vResponse = null;
            BulkImportResponse eResponse = null;

            try
            {
                vResponse = await graphbulkExecutor.BulkImportAsync(
                        Utils.GenerateVertices<Store>(numberOfDocumentsToGenerate),
                        enableUpsert: true,
                        disableAutomaticIdGeneration: true,
                        maxConcurrencyPerPartitionKeyRange: null,
                        maxInMemorySortingBatchSize: null,
                        cancellationToken: token);

                eResponse = await graphbulkExecutor.BulkImportAsync(
                        Utils.GenerateEdges(numberOfDocumentsToGenerate),
                        enableUpsert: true,
                        disableAutomaticIdGeneration: true,
                        maxConcurrencyPerPartitionKeyRange: null,
                        maxInMemorySortingBatchSize: null,
                        cancellationToken: token);
            }
            catch (DocumentClientException de)
            {
                Trace.TraceError($"Document Client Exception:\n{de.ToString()}");
            }
            catch (Exception e)
            {
                Trace.TraceError($"Exception:\n{e.ToString()}");
            }

            var vertexCount = vResponse.NumberOfDocumentsImported;
            var vertexTime = vResponse.TotalTimeTaken.TotalSeconds;
            var vertexRU = vResponse.TotalRequestUnitsConsumed;

            var edgeCount = eResponse.NumberOfDocumentsImported;
            var edgeTime = eResponse.TotalTimeTaken.TotalSeconds;
            var edgeRU = eResponse.TotalRequestUnitsConsumed;
            
            var graphElementCount = vertexCount + edgeCount;
            var totalTime = vertexTime + edgeCount;
            var totalRU = vertexRU + edgeRU;

            var writesPerSec = Math.Round(vertexCount / totalTime);
            var ruPerSec = Math.Round(totalRU / totalTime);

            Console.WriteLine("\nSummary for batch");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine($"Inserted {graphElementCount} graph elements ({vertexCount} vertices, {edgeCount} edges) " +
                              $"@ {writesPerSec} writes/s, {ruPerSec} RU/s in {totalTime} sec");
            Console.WriteLine($"Average RU consumption per insert: {totalRU / graphElementCount}");
            Console.WriteLine("---------------------------------------------------------------------\n");

            if (vResponse.BadInputDocuments.Count > 0 || eResponse.BadInputDocuments.Count > 0)
            {
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"BadVertices.txt", true))
                {
                    foreach (object doc in vResponse.BadInputDocuments)
                    {
                        file.WriteLine(doc);
                    }
                }

                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"BadEdges.txt", true))
                {
                    foreach (object doc in eResponse.BadInputDocuments)
                    {
                        file.WriteLine(doc);
                    }
                }
            }

            // Cleanup on finish if set in config.
            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]))
            {
                Trace.TraceInformation($"Deleting Database {DatabaseName}");
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            }

            Trace.WriteLine("\nPress any key to exit.");
            Console.ReadKey();
        }
    }
}
