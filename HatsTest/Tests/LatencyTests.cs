using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.TableTest
{
    [TestClass]
    public class LatencyTests
    {
        MultiDatacenterTables dcTables;

        public LatencyTests()
        {
            this.dcTables = new MultiDatacenterTables("LatencyTests");
        }

        [TestMethod]
        public async Task OneTableDelayDoesNotCauseOverallDelayDuringRetrieve()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new LatentAzureTable(this.dcTables.Table1, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3)
            });
            Stopwatch watch = Stopwatch.StartNew();
            DynamicTableEntity entityReturned = await hatsTable.Retrieve("Unknown" + Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
            Debug.Assert(watch.ElapsedMilliseconds < milliSecondsToWait);
            Debug.Assert(entityReturned == null);
        }

        [TestMethod]
        public async Task OneTableDelayDoesNotCauseOverallDelayDuringWrite()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>() 
            { 
                new LatentAzureTable(this.dcTables.Table1, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            Stopwatch watch = Stopwatch.StartNew();
            DynamicTableEntity entityReturned = await hatsTable.InsertOrReplace(entity);
            Debug.Assert(watch.ElapsedMilliseconds < milliSecondsToWait);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task TwoTableDelayDoesCauseOverallDelayDuringRetrieve()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new LatentAzureTable(this.dcTables.Table1, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
                new LatentAzureTable(this.dcTables.Table2, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
                new RegularAzureTable(this.dcTables.Table3)
            });
            Stopwatch watch = Stopwatch.StartNew();
            DynamicTableEntity entityReturned = await hatsTable.Retrieve("Unknown" + Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
            Debug.Assert(watch.ElapsedMilliseconds >= milliSecondsToWait);
            Debug.Assert(entityReturned == null);
        }

        [TestMethod]
        public async Task TwoTableDelayDoesCauseOverallDelayDuringWrite()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new LatentAzureTable(this.dcTables.Table1, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
                new LatentAzureTable(this.dcTables.Table2, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
                new RegularAzureTable(this.dcTables.Table3)
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            Stopwatch watch = Stopwatch.StartNew();
            DynamicTableEntity entityReturned = await hatsTable.InsertOrMerge(entity);
            Debug.Assert(watch.ElapsedMilliseconds >= milliSecondsToWait);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task TwoTableFailuresDoesCauseImmediateReturnDuringWrite()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new LatentAzureTable(this.dcTables.Table3, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            Stopwatch watch = Stopwatch.StartNew();
            bool failed = false;
            try
            {
                await hatsTable.InsertOrMerge(entity);
            }
            catch (QuorumFailureException)
            {
                failed = true;
            }

            Debug.Assert(failed);
            Debug.Assert(watch.ElapsedMilliseconds < milliSecondsToWait);
        }

        [TestMethod]
        public async Task TwoTableFailuresDoesCauseImmediateReturnDuringWriteAdvanced()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new ReadSucceedWriteFailTable(this.dcTables.Table1, new StorageException()), 
                new ReadSucceedWriteFailTable(this.dcTables.Table1, new StorageException()), 
                new LatentAzureTable(this.dcTables.Table3, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            Stopwatch watch = Stopwatch.StartNew();
            bool failed = false;
            try
            {
                await hatsTable.InsertOrMerge(entity);
            }
            catch (QuorumFailureException)
            {
                failed = true;
            }

            Debug.Assert(failed);
            Debug.Assert(watch.ElapsedMilliseconds < milliSecondsToWait);
        }

        [TestMethod]
        public async Task TwoTableFailuresDoesCauseImmediateReturnDuringRetrieve()
        {
            const int milliSecondsToWait = 2000;
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new LatentAzureTable(this.dcTables.Table3, TimeSpan.FromMilliseconds(milliSecondsToWait)), 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            Stopwatch watch = Stopwatch.StartNew();
            bool failed = false;
            try
            {
                await hatsTable.Retrieve("Unknown" + Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
            }
            catch (Exception)
            {
                failed = true;
            }

            Debug.Assert(failed);
            Debug.Assert(watch.ElapsedMilliseconds < milliSecondsToWait);
        }
    }
}
