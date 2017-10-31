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
    public class QuorumConsensusTests
    {
        MultiDatacenterTables dcTables;

        public QuorumConsensusTests()
        {
            this.dcTables = new MultiDatacenterTables("QuorumConsensusTests");
        }

        [TestMethod]
        public async Task QuorumOfEmptyResolvesCorrectly()
        {
            //0-0-1
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity version1Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false);
            version1Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version1Entity));
            
            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entityReturned == null);
        }

        [TestMethod]
        public async Task QuorumOfNonEmptyResolvesCorrectly()
        {
            //0-1-1
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();

            DynamicTableEntity version1Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false);
            version1Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version1Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version1Entity));
            
            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version1Entity).IsSameAs(entityReturned));
        }


        [TestMethod]
        public async Task MultipleVersionsResolvesCorrectly()
        {
            //1-1-1
            //2-2-2
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

            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version2Entity).IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task LatestVersionIsQuorumResolvesCorrectly()
        {
            //1-1-1
            //2-2
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
            
            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version2Entity).IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task LatestIsNotQuorumResolvesCorrectly()
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
        }

        [TestMethod]
        public async Task All3DiffeerntLatestButStillResolvesCorrectly()
        {
            //1-1-1
            //  2-2
            //3-3
            //4
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
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version2Entity));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(version2Entity));

            DynamicTableEntity version3Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 3, false);
            version3Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version3Entity));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(version3Entity));

            DynamicTableEntity version4Entity = FakeHats.BuildfakeHatsEntityToWrite(entity, 4, false);
            version4Entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(version4Entity));
            
            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(FakeHats.ConvertFakeToOriginalEntity(version3Entity).IsSameAs(entityReturned));
        }
    }
}
