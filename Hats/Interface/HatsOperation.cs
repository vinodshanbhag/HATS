

namespace Microsoft.WindowsAzure.Storage.Table
{
      // Same as of TableOperation
      public sealed class HatsOperation
      {
        public ITableEntity Entity { get; private set; }
        public TableOperationType OperationType { get; private set; }

        public string RowKey { get; private set; }
        public string PartitionKey { get; private set; }
       
        internal HatsOperation(ITableEntity entity, TableOperationType operationType)
        {
            this.Entity = entity;
            this.OperationType = operationType;
        }

        internal HatsOperation(string partitionKey, string rowKey)
        {
            this.OperationType = TableOperationType.Retrieve;
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public static HatsOperation Delete(ITableEntity entity)
        {
            return new HatsOperation(entity, TableOperationType.Delete);
        }

        public static HatsOperation Insert(ITableEntity entity)
        {
            return new HatsOperation(entity, TableOperationType.Insert);
        }

        public static HatsOperation InsertOrMerge(ITableEntity entity)
        {
            return new HatsOperation(entity, TableOperationType.InsertOrMerge);
        }

        public static HatsOperation InsertOrReplace(ITableEntity entity)
        {
            return new HatsOperation(entity, TableOperationType.InsertOrReplace); 
        }

        public static HatsOperation Merge(ITableEntity entity)
        {
            return new HatsOperation(entity, TableOperationType.Merge); 
        }

        public static HatsOperation Replace(ITableEntity entity)
        {
            return new HatsOperation(entity, TableOperationType.Replace);
        }

        public static HatsOperation Retrieve(string partitionKey, string rowkey)
        {
            return new HatsOperation(partitionKey, rowkey);
        }
  }
}
