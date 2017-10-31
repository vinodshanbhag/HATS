using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Table
{
    // This is the interface used for testability - should not be used for production
    public interface ITable
    {
        Task<TableResult> ExecuteAsync(HatsOperation operation);

        Task<IList<TableResult>> ExecuteBatchAsync(HatsBatchOperation batchOperation);

        Task<IEnumerable<DynamicTableEntity>> GetTopTwoEntities(TableQuery<DynamicTableEntity> query);
    }
}
