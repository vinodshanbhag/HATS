using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;


namespace Microsoft.WindowsAzure.Storage.TableTest
{
    [TestClass]
    public class FixupTests
    {
        MultiDatacenterTables dcTables;

        public FixupTests()
        {
            this.dcTables = new MultiDatacenterTables("FixupTests");
        }

        [TestMethod]
        public async Task FixedUpAfterRead()
        {
            //1-1-1
            //2-2-2
            //3-3-3
            //4-4
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hats.InsertOrReplace(entity);

            DynamicTableEntity version2Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            version2Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version2Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version2Entity));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(version2Entity));

            DynamicTableEntity version3Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 3, false);
            version3Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version3Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version3Entity));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(version3Entity));

            DynamicTableEntity version4Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 4, false);
            version4Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version4Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version4Entity));

            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version4Entity).IsSameAs(entityReturned));

            Thread.Sleep(2000);
            // Fix up
            Debug.Assert(this.dcTables.Table1.ExecuteAsync(TableOperation.Retrieve(version4Entity.PartitionKey, version4Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table2.ExecuteAsync(TableOperation.Retrieve(version4Entity.PartitionKey, version4Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table3.ExecuteAsync(TableOperation.Retrieve(version4Entity.PartitionKey, version4Entity.RowKey)).Result.Result != null);
        }

        [TestMethod]
        public async Task FixedUpLatestButOneRowAfterRead()
        {
            //1-1-1
            //2-2-2
            //3-3
            //4T1-4T2
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hats.InsertOrReplace(entity);

            DynamicTableEntity version2Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            version2Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version2Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version2Entity));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(version2Entity));

            DynamicTableEntity version3Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 3, false);
            version3Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version3Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version3Entity));

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 4, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 4, false)));
            
            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version3Entity).IsSameAs(entityReturned));

            Thread.Sleep(2000);

            // Fix up
            Debug.Assert(this.dcTables.Table1.ExecuteAsync(TableOperation.Retrieve(version3Entity.PartitionKey, version3Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table2.ExecuteAsync(TableOperation.Retrieve(version3Entity.PartitionKey, version3Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table3.ExecuteAsync(TableOperation.Retrieve(version3Entity.PartitionKey, version3Entity.RowKey)).Result.Result != null);

            entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version3Entity).IsSameAs(entityReturned));
        }


        [TestMethod]
        public async Task FixedUpDeletedEntityAfterRead()
        {
            //1-1
            //D-D
            Hats hatsFaulty = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new WebException()) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsFaulty.InsertOrReplace(entity);
            await hatsFaulty.Delete(entityReturned);

            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3),  
            });
            
            entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entityReturned == null);

            DynamicTableEntity version2Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            
            
            Thread.Sleep(2000);
            // Fix up
            Debug.Assert(this.dcTables.Table1.ExecuteAsync(TableOperation.Retrieve(version2Entity.PartitionKey, version2Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table2.ExecuteAsync(TableOperation.Retrieve(version2Entity.PartitionKey, version2Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table3.ExecuteAsync(TableOperation.Retrieve(version2Entity.PartitionKey, version2Entity.RowKey)).Result.Result != null);
        }

        [TestMethod]
        public async Task NoUnintendedFixupAfterRead()
        {
            //1-1-1
            //2
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hats.InsertOrReplace(entity);

            DynamicTableEntity version2Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            version2Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version2Entity));
           
            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entity.IsSameAs(entityReturned));

            Thread.Sleep(2000);
            
            // Nothing changed after read
            Debug.Assert(this.dcTables.Table1.ExecuteAsync(TableOperation.Retrieve(version2Entity.PartitionKey, version2Entity.RowKey)).Result.Result != null);
            Debug.Assert(this.dcTables.Table2.ExecuteAsync(TableOperation.Retrieve(version2Entity.PartitionKey, version2Entity.RowKey)).Result.Result == null);
            Debug.Assert(this.dcTables.Table3.ExecuteAsync(TableOperation.Retrieve(version2Entity.PartitionKey, version2Entity.RowKey)).Result.Result == null);

            entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task All3UncommittedRowGetsFixedupAfterRead()
        {
            //1-1-1
            //2T1-2T2-2T3
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hats.InsertOrReplace(entity);

            DynamicTableEntity version2EntityTable1 = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            version2EntityTable1.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version2EntityTable1));

            Thread.Sleep(2000);
            DynamicTableEntity version2EntityTable2 = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            version2EntityTable2.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version2EntityTable2));

            Thread.Sleep(2000);
            DynamicTableEntity version2EntityTable3 = FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false);
            version2EntityTable3.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(version2EntityTable3));

            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Thread.Sleep(2000);

            // Nothing changed after read
            DynamicTableEntity entityReturnedDirect = (DynamicTableEntity)this.dcTables.Table1.ExecuteAsync(TableOperation.Retrieve(version2EntityTable1.PartitionKey, version2EntityTable1.RowKey)).Result.Result;
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(entityReturnedDirect).IsSameAs(entityReturned));

            entityReturnedDirect = (DynamicTableEntity)this.dcTables.Table2.ExecuteAsync(TableOperation.Retrieve(version2EntityTable1.PartitionKey, version2EntityTable1.RowKey)).Result.Result;
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(entityReturnedDirect).IsSameAs(entityReturned));

            entityReturnedDirect = (DynamicTableEntity)this.dcTables.Table3.ExecuteAsync(TableOperation.Retrieve(version2EntityTable1.PartitionKey, version2EntityTable1.RowKey)).Result.Result;
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(entityReturnedDirect).IsSameAs(entityReturned));
        }
        
        [TestMethod]
        public async Task ConsensusDuringWriteFixesUpTheFailedDueToConflict()
        {
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3), 
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity version1Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false);
            version1Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version1Entity));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            DynamicTableEntity entityReturned = await hats.Insert(entity);
            Debug.Assert(entity.IsSameAs(entityReturned));
            
            Thread.Sleep(2000);

            Hats hatsFaulty = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new FailingTable(this.dcTables.Table2, new StorageException()), 
                new RegularAzureTable(this.dcTables.Table3), 
            });

            // Going against faulty checks whether we can get consensus with 1 and 3
            entityReturned = await hatsFaulty.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }
    }
}
