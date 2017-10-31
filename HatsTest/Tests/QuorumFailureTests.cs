using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.TableTest
{
    [TestClass]
    public class QuorumFailureTests
    {
        MultiDatacenterTables dcTables;

        public QuorumFailureTests()
        {
            this.dcTables = new MultiDatacenterTables("QuorumFailureTests");
        }

        [TestMethod]
        public async Task OneTableFailureDoesNotCauseOverallFailureDuringReatrieve()
        {
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3)
            });
            
           await hatsTable.Retrieve("Unknown" + Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
       }

        [TestMethod]
        public async Task OneTableFailureDoesNotCauseOverallFailureDuringWrite()
        {
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3)
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hatsTable.Insert(entity);
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hatsTable.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        [ExpectedException(typeof (QuorumFailureException))]
        public async Task TwoTableFailureDoesCauseOverallFailureDuringReatrieve()
        {
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new FailingTable(this.dcTables.Table1, new StorageException()),
                new FailingTable(this.dcTables.Table1, new StorageException()),
                new RegularAzureTable(this.dcTables.Table3)
            });
            await hatsTable.Retrieve("Unknown" + Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
        }

        [TestMethod]
        [ExpectedException(typeof(QuorumFailureException))]
        public async Task TwoTableFailureDoesCauseOverallFailureDuringWrite()
        {
            Hats hatsTable = new Hats(new List<ITable>() 
            { 
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new RegularAzureTable(this.dcTables.Table3) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hatsTable.InsertOrMerge(entity);
        }

        [TestMethod]
        public async Task QuorumFailureHasCorrectInternalExceptions()
        {
            Hats hatsTable = new Hats(new List<ITable>()
            {
                new FailingTable(this.dcTables.Table1, new WebException()), 
                new FailingTable(this.dcTables.Table1, new SocketException()), 
                new RegularAzureTable(this.dcTables.Table3)
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            bool failedAppropriately = false;
            try
            {
                await hatsTable.InsertOrMerge(entity);
            }
            catch (QuorumFailureException ex)
            {
                AggregateException aex = ex.InnerException as AggregateException;
                if (aex != null)
                {
                    failedAppropriately =
                        aex.Flatten().InnerExceptions.ToList().TrueForAll(iex => (iex is WebException || iex is SocketException));
                }
            }

            Debug.Assert(failedAppropriately);
       }
    }
}
