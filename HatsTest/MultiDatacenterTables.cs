using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Microsoft.WindowsAzure.Storage.TableTest
{
    class MultiDatacenterTables
    {
        private const string ConnectionString1 =
            "REPLACE THIS WITH CONNECTION STRING";
        private const string ConnectionString2 =
            "REPLACE THIS WITH CONNECTION STRING";
        private const string ConnectionString3 =
            "REPLACE THIS WITH CONNECTION STRING";

        public CloudTable Table1 { get; private set; }
        public CloudTable Table2 { get; private set; }
        public CloudTable Table3 { get; private set; }


        public MultiDatacenterTables(string tableName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConnectionString1);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            this.Table1 = tableClient.GetTableReference(tableName);
            this.Table1.CreateIfNotExists();

            storageAccount = CloudStorageAccount.Parse(ConnectionString2);
            tableClient = storageAccount.CreateCloudTableClient();
            this.Table2 = tableClient.GetTableReference(tableName);
            this.Table2.CreateIfNotExists();

            storageAccount = CloudStorageAccount.Parse(ConnectionString3);
            tableClient = storageAccount.CreateCloudTableClient();
            this.Table3 = tableClient.GetTableReference(tableName);
            this.Table3.CreateIfNotExists();
        }
    }
}
