
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.TableTest
{

    public sealed class ReadSucceedWriteFailTable : ITable
    {
        CloudTable cloudTable;

        public Exception exception;

        public ReadSucceedWriteFailTable(CloudTable table, Exception exceptionToThrow)
        {
            this.cloudTable = table;
            this.exception = exceptionToThrow;
        }

        public async Task<TableResult> ExecuteAsync(HatsOperation operation)
        {
            throw this.exception;
        }

        public Task<IList<TableResult>> ExecuteBatchAsync(HatsBatchOperation batchOperation)
        {
            throw this.exception;
        }

        public async Task<IEnumerable<DynamicTableEntity>> GetTopTwoEntities(TableQuery<DynamicTableEntity> query)
        {
            TableQuerySegment<DynamicTableEntity> results = await cloudTable.ExecuteQuerySegmentedAsync(query, (pk, rk, ts, props, etag) => new DynamicTableEntity(pk, rk, etag, props) { Timestamp = ts }, null);
            Debug.Assert(!(results.Count() < 2 && results.ContinuationToken != null));
            return results;
        }
    }
}
