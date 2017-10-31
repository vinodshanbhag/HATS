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
    public class BasicTests
    {
        HatsTable hatsTable = null;

        public BasicTests()
        {
            MultiDatacenterTables dcTables = new MultiDatacenterTables("BasicTests");
            this.hatsTable = new HatsTable(new List<CloudTable>() { dcTables.Table1, dcTables.Table2, dcTables.Table3 });
        }

        [TestMethod]
        public async Task BasicRetrieve()
        {
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve("Unknown" + Guid.NewGuid().ToString(), Guid.NewGuid().ToString()));
            Debug.Assert(entityReturned == null);
        }

        [TestMethod]
        public async Task BasicInsert()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task BasicInsertReplace()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task BasicInsertOrMerge()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrMerge(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task BasicReplace()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entity.ETag = entityReturned.ETag;

            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Replace(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task BasicMerge()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            const string keyToRetain = "keyToRetain";
            const string valueToRetain = "valueToRetain";
            entity.Properties.Add(keyToRetain, new EntityProperty(valueToRetain));
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));

            // Add a property, cahnge a property, remove a property - make sure all these properties exist eventually
            entity.Properties.Remove(keyToRetain);
            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entity.Properties.Add("NewProperty", new EntityProperty(Guid.NewGuid().ToString()));
            entity.ETag = entityReturned.ETag;
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Merge(entity));

            entity.Properties.Add(keyToRetain, new EntityProperty(valueToRetain));
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task BasicDelete()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned));
            Debug.Assert(entity.IsSameAs(entityReturned));
            Debug.Assert(entityReturned.ETag == null);
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entityReturned == null);
        }

        [TestMethod]
        public async Task BasicGetAndSet()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));

            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entity.ETag = entityReturned.ETag;
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));

            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Retrieve(entity.PartitionKey, entity.RowKey));
            Debug.Assert(entity.IsSameAs(entityReturned));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entity.ETag = entityReturned.ETag;
            entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            Debug.Assert(entity.IsSameAs(entityReturned));
        }
    }
}
