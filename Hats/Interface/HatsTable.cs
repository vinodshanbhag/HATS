
using System.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Table
{
    
    public sealed class HatsTable
    {
        Hats hats;

        public HatsTable(IEnumerable<CloudTable> tables)
        {
            if (tables.Count() < 3)
            {
                throw new ArgumentOutOfRangeException("tables", "At least 3 or more tables are required");
            }

            List<ITable> azureTables = new List<ITable>();
            foreach (CloudTable table in tables)
            {
                azureTables.Add(new AzureTable(table));
            }

            hats = new Hats(azureTables);
        }

        public async Task<DynamicTableEntity> ExecuteAsync(HatsOperation operation)
        {
            if (operation.OperationType != TableOperationType.Retrieve && operation.Entity == null)
            {
                throw new ArgumentNullException("operation", "Entity cant be null");
            }

            if (operation.Entity != null)
            {
                foreach (KeyValuePair<string, EntityProperty> prop in operation.Entity.WriteEntity(null))
                {
                    if (prop.Key.StartsWith(Hats.HatsPropertyPrefix))
                    {
                        throw new ArgumentOutOfRangeException("operation", string.Format("Property name prefix {0} is reserved", Hats.HatsPropertyPrefix));
                    }
                }
            }

            long eTag = 0;
            if (operation.Entity != null &&
                operation.Entity.ETag != null 
                && operation.Entity.ETag != "*" 
                && !long.TryParse(operation.Entity.ETag, out eTag))
            {
                throw new ArgumentOutOfRangeException("operation", "ETag is in wrong format. It should be a number");
            }

            
           

            if (operation.OperationType == TableOperationType.Insert)
            {
                return await hats.Insert(operation.Entity);
            }
            if (operation.OperationType == TableOperationType.InsertOrReplace)
            {
                return await hats.InsertOrReplace(operation.Entity);
            }
            if (operation.OperationType == TableOperationType.InsertOrMerge)
            {
                return await hats.InsertOrMerge(operation.Entity);
            }
            if (operation.OperationType == TableOperationType.Replace)
            {
                return await hats.Replace(operation.Entity);
            }
            if (operation.OperationType == TableOperationType.Merge)
            {
                return await hats.Merge(operation.Entity);
            }
            if (operation.OperationType == TableOperationType.Delete)
            {
                return await hats.Delete(operation.Entity);
            }
            else
            {
                return await hats.Retrieve(operation.PartitionKey, operation.RowKey);
            }
        }

        public Task<IList<TableResult>> ExecuteBatchAsync(HatsBatchOperation batchOperation)
        {
            throw new NotImplementedException();
        }
    }
}
