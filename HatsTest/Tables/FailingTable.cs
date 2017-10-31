
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.TableTest
{

    public sealed class FailingTable : ITable
    {
        CloudTable cloudTable;

        public Exception exception;

        public FailingTable(CloudTable table, Exception exceptionToThrow)
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
            throw this.exception;
        }
    }
}
