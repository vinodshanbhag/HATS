using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;


namespace Microsoft.WindowsAzure.Storage.TableTest
{
    [TestClass]
    public class DeleteAndAfterTests
    {
        HatsTable hatsTable = null;

        public DeleteAndAfterTests()
        {
            MultiDatacenterTables dcTables = new MultiDatacenterTables("DeleteAndAfterTests");
            this.hatsTable = new HatsTable(new List<CloudTable>() { dcTables.Table1, dcTables.Table2, dcTables.Table3 });
        }

        [TestMethod]
        public async Task InsertAfterDelete()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned));
            Debug.Assert(entity.IsSameAs(entityReturned));
            Debug.Assert(entityReturned.ETag == null);
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entityReturned == null);

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        [ExpectedException(typeof(NotFoundException))]
        
        public async Task ReplaceAfterDeleteFails()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned));
            Debug.Assert(entity.IsSameAs(entityReturned));
            Debug.Assert(entityReturned.ETag == null);
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entityReturned == null);

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Replace(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        [ExpectedException(typeof (NotFoundException))]

        public async Task DoubleDeleteFails()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned));
        }

        [TestMethod]
        public async Task MergeAfterDeleteDoesNotMergeWithDeleted()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned));
            Debug.Assert(entity.IsSameAs(entityReturned));
            Debug.Assert(entityReturned.ETag == null);
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entityReturned == null);

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrMerge(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }
    }
}
