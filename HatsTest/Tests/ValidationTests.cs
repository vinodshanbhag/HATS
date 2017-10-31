using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Microsoft.WindowsAzure.Storage.TableTest
{
    [TestClass]
    public class ValidationTests
    {
        HatsTable hatsTable = null;

        public ValidationTests()
        {
            MultiDatacenterTables dcTables = new MultiDatacenterTables("ValidationTests");
            this.hatsTable = new HatsTable(new List<CloudTable>() { dcTables.Table1, dcTables.Table2, dcTables.Table3 });
       }


        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public async Task MinimumTablesRequired()
        {
            MultiDatacenterTables dcTables = new MultiDatacenterTables("ValidationTests");
            new HatsTable(new List<CloudTable>() { dcTables.Table1, dcTables.Table2 });
        }


        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public async Task ETagMustBeANumber()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            entity.ETag = "test";
            await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
  
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public async Task PropertyCantStartWithReservedPrefix()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            entity.Properties.Add(Hats.HatsPropertySchemaVersion, new EntityProperty("test"));
            await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public async Task EntityCantBeNull()
        {
            await hatsTable.ExecuteAsync(HatsOperation.Insert(null));
        }

        
    }
}
