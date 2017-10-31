using System.Collections.Generic;

namespace Microsoft.WindowsAzure.Storage.Table
{
    // Same as BatchOperation
    public sealed class HatsBatchOperation
    {
        internal List<HatsOperation> Operations { get; private set; }

        public HatsBatchOperation()
        {
            this.Operations = new List<HatsOperation>();
        }

        public void Delete(ITableEntity entity)
        {
            this.Operations.Add(HatsOperation.Delete(entity));
        }

        public void Insert(ITableEntity entity)
        {
            this.Operations.Add(HatsOperation.Insert(entity));
        }

        public void InsertOrMerge(ITableEntity entity)
        {
            this.Operations.Add(HatsOperation.InsertOrMerge(entity));
        }

        public void InsertOrReplace(ITableEntity entity)
        {
            this.Operations.Add(HatsOperation.InsertOrReplace(entity));
        }

        public void Merge(ITableEntity entity)
        {
            this.Operations.Add(HatsOperation.Merge(entity));
        }

        public void Replace(ITableEntity entity)
        {
            this.Operations.Add(HatsOperation.Replace(entity));
        }

        public void Retrieve(string partitionKey, string rowkey)
        {
            this.Operations.Add(HatsOperation.Retrieve(partitionKey, rowkey));
        }
    }
}
