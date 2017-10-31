using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.TableTest
{
    [TestClass]
    public class ETagTests
    {
        HatsTable hatsTable = null;

        public ETagTests()
        {
            MultiDatacenterTables dcTables = new MultiDatacenterTables("ETagTests");
            this.hatsTable = new HatsTable(new List<CloudTable>() { dcTables.Table1, dcTables.Table2, dcTables.Table3 });
        }

        [TestMethod]
        public async Task ETagIsDifferentAfterEveryWrite()
        {
            HashSet<string> eTags = new HashSet<string>();
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            eTags.Add(entityReturned.ETag);

            int loops = 3;
            for (int i = 0; i < loops; i++)
            {
                entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrMerge(entity));
                eTags.Add(entityReturned.ETag);

                entityReturned = await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
                eTags.Add(entityReturned.ETag);

                entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Replace(entityReturned));
                eTags.Add(entityReturned.ETag);

                entityReturned = await hatsTable.ExecuteAsync(HatsOperation.Merge(entityReturned));
                eTags.Add(entityReturned.ETag);
            }

            Debug.Assert(eTags.Count == 4 * loops + 1);
        }

        [TestMethod]
        [ExpectedException(typeof(ETagMismatchException))]
        public async Task ReplaceFailsWithWrongETag()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned1 = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Replace(entityReturned1));
        }

        [TestMethod]
        [ExpectedException(typeof(ETagMismatchException))]

        public async Task MergeFailsWithWrongETag()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned1 = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Merge(entityReturned1));
        }

        [TestMethod]
        [ExpectedException(typeof(ETagMismatchException))]
        public async Task DeleteFailsWithWrongETag()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned1 = await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Delete(entityReturned1));
        }

        [TestMethod]
        public async Task StarETagWorksForAllWrites()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            entity.ETag = "*";
            await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            await hatsTable.ExecuteAsync(HatsOperation.InsertOrMerge(entity));
            await hatsTable.ExecuteAsync(HatsOperation.InsertOrReplace(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Replace(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Merge(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Delete(entity));
        }

        [TestMethod]
        [ExpectedException(typeof(AlredyExistsException))]

        public async Task InsertingTwiceFails()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
            await hatsTable.ExecuteAsync(HatsOperation.Insert(entity));
        }

        [TestMethod]
        [ExpectedException(typeof(NotFoundException))]
        public async Task ReplaceWithNoEntityFails()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hatsTable.ExecuteAsync(HatsOperation.Replace(entity));
        }

        [TestMethod]
        [ExpectedException(typeof(NotFoundException))]
        public async Task MergeWithNoEntityFails()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hatsTable.ExecuteAsync(HatsOperation.Merge(entity));
        }

        [TestMethod]
        [ExpectedException(typeof(NotFoundException))]
        public async Task DeleteWithNoEntityFails()
        {
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hatsTable.ExecuteAsync(HatsOperation.Delete(entity));
        }
        
    }
}
