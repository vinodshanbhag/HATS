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
    public class RacyOperationTests
    {
        MultiDatacenterTables dcTables;

        public RacyOperationTests()
        {
            this.dcTables = new MultiDatacenterTables("RacyOperationTests");
        }

        [TestMethod]
        public async Task CommitTheUncommittedWhileReading()
        {
            //1T1-1T2-1T3
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3), 
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity fakeEntity1 = FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false);
            fakeEntity1.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());

            DynamicTableEntity fakeEntity2 = FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false);
            fakeEntity2.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            
            DynamicTableEntity fakeEntity3 = FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false);
            fakeEntity3.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(fakeEntity1));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(fakeEntity2));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(fakeEntity3));

            // Now chose the one with least timestamp as the winner
            List<DynamicTableEntity> allEntities = new List<DynamicTableEntity>() {fakeEntity1, fakeEntity2, fakeEntity3};
            DynamicTableEntity consensusEntity = allEntities.First();
            foreach (DynamicTableEntity candidate in allEntities)
            {
                if (candidate.Timestamp.CompareTo(consensusEntity.Timestamp) < 0)
                {
                    consensusEntity = candidate;
                }
            }

            DynamicTableEntity entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entityReturned.IsSameAs(FakeHats.ConvertFakeToOriginalEntity(consensusEntity)));
        }
        
        [TestMethod]
        public async Task CommitTheUncommittedFirstRowAndSucceedWriting()
        {
            //1T1-1T2-1T3
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3), 
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));

            DynamicTableEntity entityReturned = await hats.InsertOrMerge(entity);
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        public async Task CommitTheUncommittedSecondRowAndSucceedWriting()
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
            DynamicTableEntity entityReturned = await hats.InsertOrReplace(entity);

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            
            entityReturned = await hats.InsertOrMerge(entity);
            Debug.Assert(entity.IsSameAs(entityReturned));
            entityReturned = await hats.Retrieve(entity.PartitionKey, entity.RowKey);
            Debug.Assert(entity.IsSameAs(entityReturned));
        }

        [TestMethod]
        [Ignore]
        public async Task FirstRowPartialRaceHenceRetryAndSucceed()
        {
            //1T1-1T2-1T3
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3), 
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            
            await hats.InsertOrMerge(entity);
        }

        [TestMethod]
        [Ignore]
        public async Task SecondRowPartialRaceHenceRetryAndSucceed()
        {
            int count = 100;
            while (count-- > 0)
            {
                //1-1-1
                //2T1-2T2
                Hats hats = new Hats(new List<ITable>() 
                { 
                    new RegularAzureTable(this.dcTables.Table1), 
                    new RegularAzureTable(this.dcTables.Table2), 
                    new RegularAzureTable(this.dcTables.Table3) 
                });
                DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
                DynamicTableEntity entityReturned = await hats.InsertOrReplace(entity);

                await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
                await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));

                entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
                entity.ETag = entityReturned.ETag;

                await hats.Merge(entity);
            }
         }


        [TestMethod]
        [ExpectedException(typeof(NoQuroumConsensusException))]
        public async Task FirstRowRaceButCantWalkUpBecauseOneReplicaIsUnavailableDuringWrite()
        {
            //1T1-1T2-1T3
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new StorageException()) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            
            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            await hats.InsertOrMerge(entity);
        }

        [TestMethod]
        [ExpectedException(typeof(NoQuroumConsensusException))]
        public async Task SecondRowRaceButCantWalkUpBecauseOneReplicaIsUnavailableDuringWrite()
        {
            //1-1-1
            //2T1-2T2-2T3 (But one unavailable)
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new StorageException()) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hats.InsertOrReplace(entity);

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));

            entity.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
            entity.ETag = entityReturned.ETag;
            await hats.Replace(entity);
        }

        [TestMethod]
        [ExpectedException(typeof(NoQuroumConsensusException))]
        public async Task FirstRowRaceButCantWalkUpBecauseOneReplicaIsUnavailableDuringRead()
        {
            //1T1-1T2-1T3
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new StorageException()) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 1, false)));

            await hats.Retrieve(entity.PartitionKey, entity.RowKey);
        }

        [TestMethod]
        [ExpectedException(typeof(NoQuroumConsensusException))]
        public async Task SecondRowRaceButCantWalkUpBecauseOneReplicaIsUnavailableDuringRead()
        {
            //1-1-1
            //2T1-2T2-2T3 (But one unavailable)
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new StorageException()) 
            });
            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            DynamicTableEntity entityReturned = await hats.InsertOrReplace(entity);

            await this.dcTables.Table1.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
            await this.dcTables.Table2.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));
            await this.dcTables.Table3.ExecuteAsync(TableOperation.Insert(FakeHats.BuildfakeHatsEntityToWrite(entity, 2, false)));

            await hats.Retrieve(entity.PartitionKey, entity.RowKey);
        }

        [TestMethod]
        public async Task TooManyWritersOnSingleKeyMakeProgress()
        {
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3)
            });

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
            await hats.InsertOrReplace(entity);

            int workers = 20;
            int loops = 3;
            int successfulInserts = 0;
            int etagMisMatchException = 0;
            int quorumFailureException = 0;


            Parallel.For(0, workers, i =>
            {
                int counter = loops;
                while (--counter > 0)
                {
                    try
                    {
                        //Hats hatsToUse = hats;
                        DynamicTableEntity entityReturned = hats.Retrieve(entity.PartitionKey, entity.RowKey).Result;
                        Debug.Assert(entityReturned != null);
                        entityReturned.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());
                        DynamicTableEntity entityReturned2 = hats.Replace(entityReturned).Result;
                        Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        Interlocked.Increment(ref successfulInserts);
                    }
                    catch (AggregateException aex)
                    {
                        // ignore
                        if (aex.InnerException is ETagMismatchException)
                        {
                            Interlocked.Increment(ref etagMisMatchException);
                        }
                        else if (aex.InnerException is QuorumFailureException)
                        {
                            Interlocked.Increment(ref quorumFailureException);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
                
            });
            
            Debug.Assert(successfulInserts >= 2);
            Debug.Assert(quorumFailureException + etagMisMatchException>= 2);
       }

        [TestMethod]
        public async Task MultipleRacyWritesOnThreeDCs()
        {
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new RegularAzureTable(this.dcTables.Table3)
            });

            int workers = 4;
            int totalKeys = 10;
            int successfulInserts = 0;
            int quorumFailureException = 0;
            int noQuorumException = 0;
            int alreadyExistsException = 0;

            for (int keyCounter = 0; keyCounter < totalKeys; keyCounter++)
            {
                DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();

                Parallel.For(0, workers, i =>
                {
                    try
                    {
                        DynamicTableEntity entityReturned2 = hats.Insert(entity).Result;
                        Debug.Assert(entity.IsSameAs(entityReturned2));
                        Interlocked.Increment(ref successfulInserts);
                    }
                    catch (AggregateException aex)
                    {
                        if (aex.InnerException is QuorumFailureException)
                        {
                            Interlocked.Increment(ref quorumFailureException);
                        }
                        else if (aex.InnerException is NoQuroumConsensusException)
                        {
                            Interlocked.Increment(ref noQuorumException);
                        }
                        else if (aex.InnerException is AlredyExistsException)
                        {
                            Interlocked.Increment(ref alreadyExistsException);
                        }
                        else if (aex.InnerException is NoQuroumConsensusException)
                        {
                            Interlocked.Increment(ref noQuorumException);
                        }
                        else
                        {
                            throw;
                        }
                    }
                });
            }

            Debug.Assert(successfulInserts == totalKeys);
            Debug.Assert(quorumFailureException + noQuorumException + alreadyExistsException >= 2);
            Debug.Assert(noQuorumException == 0);
        }

        [TestMethod]
        public async Task MultipleRacyWritesWithOneDCDown()
        {
            Hats hats = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new WebException())
            });

            int workers = 4;
            int totalKeys = 10;
            int successfulInserts = 0;
            int quorumFailureException = 0;
            int noQuorumException = 0;
            int rowAlareadyExistsException = 0;
            
            for (int keyCounter = 0; keyCounter < totalKeys; keyCounter++)
            {
                DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();
               
                Parallel.For(0, workers, i =>
                {
                    try
                    {
                        DynamicTableEntity entityReturned2 = hats.Insert(entity).Result;
                        Debug.Assert(entity.IsSameAs(entityReturned2));
                        Interlocked.Increment(ref successfulInserts);
                    }
                    catch (AggregateException aex)
                    {
                        if (aex.InnerException is QuorumFailureException)
                        {
                            Interlocked.Increment(ref quorumFailureException);
                        }
                        else if (aex.InnerException is NoQuroumConsensusException)
                        {
                            Interlocked.Increment(ref noQuorumException);
                        }
                        else if (aex.InnerException is AlredyExistsException)
                        {
                            Interlocked.Increment(ref rowAlareadyExistsException);
                        }
                        else
                        {
                            throw;
                        }
                    }
                });
            }

            Debug.Assert(successfulInserts > 1);
            Debug.Assert(quorumFailureException + noQuorumException + rowAlareadyExistsException >= 2);
        }

        [TestMethod]
        public async Task ComplexCombinationsMakesProgress()
        {
            // Networks splits
            // All read/write operations happening simultaneously 
            // All ETag combinations (*, null, someValue)
            Hats hatsAllOk = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new RegularAzureTable(this.dcTables.Table2), 
                new LatentAzureTable(this.dcTables.Table3, TimeSpan.FromMilliseconds(100)), 
            });

            Hats hatsOneFaulty = new Hats(new List<ITable>() 
            { 
                new RegularAzureTable(this.dcTables.Table1), 
                new LatentAzureTable(this.dcTables.Table2, TimeSpan.FromMilliseconds(100)), 
                new FailingTable(this.dcTables.Table3, new StorageException())
            });

            Hats hatsTwoFaulty = new Hats(new List<ITable>() 
            { 
                new ReadSucceedWriteFailTable(this.dcTables.Table1, new StorageException()), 
                new RegularAzureTable(this.dcTables.Table2), 
                new FailingTable(this.dcTables.Table3, new StorageException())
            });

            Hats hatsThreeFaulty = new Hats(new List<ITable>() 
            { 
                new FailingTable(this.dcTables.Table1, new StorageException()), 
                new FailingTable(this.dcTables.Table2, new StorageException()), 
                new FailingTable(this.dcTables.Table3, new StorageException())
            });

            int workers = 6;
            int loops = 10;
            int successfulInserts = 0;
            int etagMisMatchException = 0;
            int noQuorumConsensusException = 0;
            int quorumFailureException = 0;
            int alreadyExistsException = 0;
            int notFoundException = 0;

            DynamicTableEntity entity = DynamicTableEntityExtensions.GetRandomEntity();

            Parallel.For(0, workers, i =>
            {
                int counter = loops;
                while (--counter > 0)
                {
                    try
                    {
                        Hats hatsToUse = null;
                        int h = counter%4;
                        if (h == 0)
                        {
                            hatsToUse = hatsAllOk;
                        }
                        else if (h == 1)
                        {
                            hatsToUse = hatsOneFaulty;
                        }
                        else if (h == 2)
                        {
                            hatsToUse = hatsTwoFaulty;
                        }
                        else 
                        {
                            hatsToUse = hatsThreeFaulty;
                        }

                        DynamicTableEntity entityReturned = hatsToUse.Retrieve(entity.PartitionKey, entity.RowKey).Result;
                        if (entityReturned == null)
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.Insert(entity).Result;
                            Debug.Assert(entity.IsSameAs(entityReturned2));
                            Interlocked.Increment(ref successfulInserts);
                            continue;
                        }

                        entityReturned.Properties[entity.Properties.First().Key] = new EntityProperty(Guid.NewGuid().ToString());

                        int e = counter % 4;
                        if (e == 0)
                        {
                            entityReturned.ETag = "*";
                        }
                        else if (e == 1)
                        {
                            entityReturned.ETag = "1234";
                        }
                        else if (e == 2)
                        {
                            entityReturned.ETag = null;
                        }
                        else
                        {
                            // leave as is
                        }

                        
                        int op = counter%6;
                        if (op == 0)
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.Insert(entityReturned).Result;
                            Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        }
                        if (op == 1)
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.Replace(entityReturned).Result;
                            Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        }
                        if (op == 2)
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.Merge(entityReturned).Result;
                            Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        }
                        if (op == 4)
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.InsertOrMerge(entityReturned).Result;
                            Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        }
                        if (op == 5)
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.InsertOrReplace(entityReturned).Result;
                            Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        }
                        else
                        {
                            DynamicTableEntity entityReturned2 = hatsToUse.Delete(entityReturned).Result;
                            Debug.Assert(entityReturned.IsSameAs(entityReturned2));
                        }
                        
                        Interlocked.Increment(ref successfulInserts);
                    }
                    catch (AggregateException aex)
                    {
                        // ignore
                        if (aex.InnerException is ETagMismatchException)
                        {
                            Interlocked.Increment(ref etagMisMatchException);
                        }
                        else if (aex.InnerException is NoQuroumConsensusException)
                        {
                            Interlocked.Increment(ref noQuorumConsensusException);
                        }
                        else if (aex.InnerException is QuorumFailureException)
                        {
                            Interlocked.Increment(ref quorumFailureException);
                        }
                        else if (aex.InnerException is AlredyExistsException)
                        {
                            Interlocked.Increment(ref alreadyExistsException);
                        }
                        else if (aex.InnerException is NotFoundException)
                        {
                            Interlocked.Increment(ref notFoundException);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }

            });

            Debug.Assert(successfulInserts >= 2);
            Debug.Assert(quorumFailureException >= 2);
            Debug.Assert(noQuorumConsensusException >= 2);
            Debug.Assert(etagMisMatchException + alreadyExistsException + notFoundException >= 2);
        }
    }
}
