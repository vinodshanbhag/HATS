using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.TableTest
{

    public sealed class RegularAzureTable : ITable
    {
        CloudTable cloudTable;

        public RegularAzureTable(CloudTable table)
        {
            this.cloudTable = table;
        }

        public async Task<TableResult> ExecuteAsync(HatsOperation operation)
        {
            if (operation.OperationType == TableOperationType.Insert)
            {
                return await cloudTable.ExecuteAsync(TableOperation.Insert(operation.Entity));
            }
            else if (operation.OperationType == TableOperationType.Delete)
            {
                return await cloudTable.ExecuteAsync(TableOperation.Delete(operation.Entity));
            }
            else if (operation.OperationType == TableOperationType.InsertOrReplace)
            {
                return await cloudTable.ExecuteAsync(TableOperation.InsertOrReplace(operation.Entity));
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public Task<IList<TableResult>> ExecuteBatchAsync(HatsBatchOperation batchOperation)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<DynamicTableEntity>> GetTopTwoEntities(TableQuery<DynamicTableEntity> query)
        {
            TableQuerySegment<DynamicTableEntity> results = await cloudTable.ExecuteQuerySegmentedAsync(query, (pk, rk, ts, props, etag) => new DynamicTableEntity(pk, rk, etag, props) {Timestamp = ts}, null);
            Debug.Assert(!(results.Count() < 2 && results.ContinuationToken != null));
            return results;
        }
    }
}
