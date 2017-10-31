using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Table
{
    public class Hats
    {
        // If you happen to add any more properties after release make sure you change HatsSchemaVersion
        public const string HatsPropertyPrefix = "HatsProperty";
        public const string HatsPropertyIsdeleted = "HatsPropertyIsdeleted";
        public const string HatsPropertyTransactionId = "HatsPropertyTransactionId";
        public const string HatsPropertySchemaVersion = "HatsPropertySchemaVersion";
        public const string HatsPropertyEntityVersion = "HatsPropertyEntityVersion";

        // We use the starting version as the BiggestVersion - beause table store returns in ascending order during range query 
        // and we want the latest version to come first
        public const long SmallestVersion = 1111111111111111111;
        public const long BiggestVersion =  8888888888888888888;
        public const long StartingVersion = BiggestVersion;
        public const string RowKeyWithPostfix = "{0}_{1}";


        List<ITable> hatsTables;
        int quorum;

        public Hats(List<ITable> tables)
        {
            this.hatsTables = tables;
            this.quorum = tables.Count/2 + 1;
        }

        public async Task<DynamicTableEntity> Insert(ITableEntity entity)
        {
            return await Write(entity, TableOperationType.Insert);
        }

        public async Task<DynamicTableEntity> Merge(ITableEntity entity)
        {
            return await Write(entity, TableOperationType.Merge);
        }

        public async Task<DynamicTableEntity> InsertOrMerge(ITableEntity entity)
        {
            return await Write(entity, TableOperationType.InsertOrMerge);
        }

        public async Task<DynamicTableEntity> InsertOrReplace(ITableEntity entity)
        {
            return await Write(entity, TableOperationType.InsertOrReplace);
        }

        public async Task<DynamicTableEntity> Replace(ITableEntity entity)
        {
            return await Write(entity, TableOperationType.Replace);
        }

        public async Task<DynamicTableEntity> Delete(ITableEntity entity)
        {
            return await Write(entity, TableOperationType.Delete);
        }

        public async Task<DynamicTableEntity> Retrieve(string partitionKey, string rowKey)
        {
            DynamicTableEntity rawConsensusEntity = await this.RetrieveRaw(partitionKey, rowKey);
            if (rawConsensusEntity == null)
            {
                return null;
            }

            return Hats.ModifyEntityBeforeResponse(rawConsensusEntity, TableOperationType.Retrieve);
        }

        public async Task<DynamicTableEntity> RetrieveRaw(string partitionKey, string rowKey)
        {
            Dictionary<Task<IEnumerable<DynamicTableEntity>>, ITable> taskToTableMap = new Dictionary<Task<IEnumerable<DynamicTableEntity>>, ITable>();
            Dictionary<DynamicTableEntity, ITable> entityToTableMap = new Dictionary<DynamicTableEntity, ITable>();

            List<Task<IEnumerable<DynamicTableEntity>>> failedTasks = new List<Task<IEnumerable<DynamicTableEntity>>>();
            List<Task<IEnumerable<DynamicTableEntity>>> succeededTasks = new List<Task<IEnumerable<DynamicTableEntity>>>();
            List<DynamicTableEntity> latestVersionEntities = new List<DynamicTableEntity>();
            List<DynamicTableEntity> allEntities = new List<DynamicTableEntity>();
            DynamicTableEntity consensusEntity = null;
            bool keyNotFoundOnQuorum = false;

            foreach (ITable table in hatsTables)
            {
                Task<IEnumerable<DynamicTableEntity>> task = GetTopTwoRowsForKey(partitionKey, rowKey, table);
                task.ObserveAsyncException();
                taskToTableMap.Add(task, table);
            }
            
            Tuple<DynamicTableEntity, bool> earlyConsensus = await this.TryReachingEarlyConsensus(
                taskToTableMap, 
                entityToTableMap, 
                failedTasks, 
                succeededTasks, 
                latestVersionEntities, 
                allEntities);

            consensusEntity = earlyConsensus.Item1;
            keyNotFoundOnQuorum = earlyConsensus.Item2;

            if (consensusEntity == null && keyNotFoundOnQuorum == false)
            {
                Tuple<DynamicTableEntity, bool> finalConsensus =  this.TryReachingFinalConsensus(
                                                                            succeededTasks,
                                                                            latestVersionEntities,
                                                                            allEntities);
                consensusEntity = finalConsensus.Item1;
                keyNotFoundOnQuorum = finalConsensus.Item2;
            }

            Debug.Assert(keyNotFoundOnQuorum || consensusEntity != null);
            
            if (consensusEntity != null)
            {
                Task.Run(async () => await this.ForceWriteConsenus(taskToTableMap, allEntities, entityToTableMap, consensusEntity)).ObserveAsyncException();

                // The entity could be deleted - if yes return null
                if (consensusEntity.Properties[HatsPropertyIsdeleted].BooleanValue.Value)
                {
                    return null;
                }

                return consensusEntity;
            }
            else
            {
                return null;
            }
        }


        Tuple<DynamicTableEntity, bool> TryReachingFinalConsensus(
            List<Task<IEnumerable<DynamicTableEntity>>> succeededTasks,
            List<DynamicTableEntity> latestVersionEntities,
            List<DynamicTableEntity> allEntities)
        {
            DynamicTableEntity consensusEntity = null;
            bool keyNotFoundOnQuorum = false;
            bool allDcsHaveUnCommittedRows = false;

            if (succeededTasks.Count() == this.hatsTables.Count)
            {
                if (allEntities.Any())
                {
                    if (this.IsQuorumConsensusReachedUsingAllRows(allEntities, out consensusEntity))
                    {
                        long latestCommittedVersion = consensusEntity.Properties[HatsPropertyEntityVersion].Int64Value.Value;
                        allDcsHaveUnCommittedRows = latestVersionEntities.TrueForAll(latestEntity =>
                            latestEntity.Properties[HatsPropertyEntityVersion].Int64Value.Value < latestCommittedVersion);

                        if (allDcsHaveUnCommittedRows)
                        {
                            // All versions have to match
                            Debug.Assert(latestVersionEntities.TrueForAll(entity => 
                                entity.Properties[HatsPropertyEntityVersion].Int64Value.Value ==
                                latestVersionEntities.First().Properties[HatsPropertyEntityVersion].Int64Value.Value));
                        }
                    }
                    else
                    {
                        // This must be the special case where there was a race in writing the first row and no one really succeded
                        foreach (DynamicTableEntity entity in allEntities)
                        {
                            Debug.Assert(entity.Properties[HatsPropertyEntityVersion].Int64Value.Value == StartingVersion);
                        }

                        // All tables are available - there are rows but there are no quorum
                        if (latestVersionEntities.Count == this.hatsTables.Count)
                        {
                            allDcsHaveUnCommittedRows = true;
                        }
                        else
                        {
                            keyNotFoundOnQuorum = true;
                        }
                    }
                }
                else
                {
                    // There are no rows in any existing tables - so consesus is null
                    keyNotFoundOnQuorum = true;
                }
            }
            else
            {
                throw new NoQuroumConsensusException();
            }

            if (allDcsHaveUnCommittedRows)
            {
                // Now chose the one with least timestamp as the winner
                consensusEntity = latestVersionEntities.First();
                foreach (DynamicTableEntity candidate in latestVersionEntities)
                {
                    if (candidate.Timestamp.CompareTo(consensusEntity.Timestamp) < 0)
                    {
                        consensusEntity = candidate;
                    }
                }
            }

            return new Tuple<DynamicTableEntity, bool>(consensusEntity, keyNotFoundOnQuorum);
        }

        async Task<Tuple<DynamicTableEntity, bool>> TryReachingEarlyConsensus(
            Dictionary<Task<IEnumerable<DynamicTableEntity>>, ITable> taskToTableMap,
            Dictionary<DynamicTableEntity, ITable> entityToTableMap,
            List<Task<IEnumerable<DynamicTableEntity>>> failedTasks,
            List<Task<IEnumerable<DynamicTableEntity>>> succeededTasks,
            List<DynamicTableEntity> latestVersionEntities,
            List<DynamicTableEntity> allEntities)
        {
            DynamicTableEntity consensusEntity = null;
            bool keyNotFoundOnQuorum = false;

            while (taskToTableMap.Any())
            {
                Task<IEnumerable<DynamicTableEntity>> finishedTask = await Task.WhenAny(taskToTableMap.Keys.ToArray());
                ITable table = taskToTableMap[finishedTask];
                taskToTableMap.Remove(finishedTask);

                if (finishedTask.IsFaulted)
                {
                    failedTasks.Add(finishedTask);
                    if (failedTasks.Count() >= this.quorum)
                    {
                        throw new QuorumFailureException(new AggregateException(failedTasks.Select(failedTask => failedTask.Exception)));
                    }
                }
                else
                {
                    succeededTasks.Add(finishedTask);
                    if (finishedTask.Result.Any())
                    {
                        latestVersionEntities.Add(finishedTask.Result.First());
                    }

                    int countOfRows = 0;
                    foreach (DynamicTableEntity entity in finishedTask.Result)
                    {
                        allEntities.Add(entity);
                        entityToTableMap.Add(entity, table);
                        countOfRows++;
                        if (countOfRows == 2)
                        {
                            // Stop at 2 rows - if not table store will go get 2 more rows again 
                            break;
                        }
                    }

                    if (succeededTasks.Count() >= this.quorum)
                    {
                        if (latestVersionEntities.Count >= this.quorum)
                        {
                            if (this.IsQuorumConsensusReachedUsingLatestRows(latestVersionEntities, out consensusEntity))
                            {
                                break;
                            }
                        }
                        else
                        {
                            if (succeededTasks.Count - latestVersionEntities.Count >= this.quorum)
                            {
                                keyNotFoundOnQuorum = true;
                                break;
                            }
                        }
                    }
                }
            }

            return new Tuple<DynamicTableEntity, bool>(consensusEntity, keyNotFoundOnQuorum);
        }

        async Task<DynamicTableEntity> Write(ITableEntity entity, TableOperationType operationType)
        {
            string transactionIdToUse = Guid.NewGuid().ToString();
            QuorumFailureException qfeToRethrow = null;
            DynamicTableEntity newEntityToWrite = await this.GetNewEntityToWrite(entity, operationType, transactionIdToUse);

            try
            {
                DynamicTableEntity entityToReturn = await this.InsertIntoAllAzureTables(newEntityToWrite);
                return Hats.ModifyEntityBeforeResponse(entityToReturn, operationType);
            }
            catch (QuorumFailureException qfe)
            {
                if (((AggregateException)qfe.InnerException).Flatten().InnerExceptions.Any(Hats.IsConflictException))
                {
                    qfeToRethrow = qfe;
                    // At least one exception is Conflict exception - this gives rise to many possibilities 
                    // 1. 3 writers raced and this particular one could have been winner and hence check and return success if that is the case
                    // 2. This actually timeout on first attempt but succeeded on retry from within azure library - hence got a conflict. However that is a success case
                    // Rerearding again to check whether this succeeded will give us the right behavior - it will also do the necessary fix up
                }
                else
                {
                    throw;
                }
            }

            DynamicTableEntity rawConsensusEntity = await this.RetrieveRaw(entity.PartitionKey, entity.RowKey);
            if (rawConsensusEntity != null && rawConsensusEntity.Properties[HatsPropertyTransactionId].StringValue == transactionIdToUse)
            {
                return ModifyEntityBeforeResponse(rawConsensusEntity, operationType);
            }
            else
            {
                throw qfeToRethrow;
            }
        }

        async Task<DynamicTableEntity> GetNewEntityToWrite(ITableEntity incomingEntity, TableOperationType operationType, string transactionId)
        {
            List<Task<IEnumerable<DynamicTableEntity>>> failedTasks = new List<Task<IEnumerable<DynamicTableEntity>>>();
            List<Task<IEnumerable<DynamicTableEntity>>> succeededTasks = new List<Task<IEnumerable<DynamicTableEntity>>>();

            Dictionary<Task<IEnumerable<DynamicTableEntity>>, ITable> taskToTableMap = new Dictionary<Task<IEnumerable<DynamicTableEntity>>, ITable>();
            Dictionary<DynamicTableEntity, ITable> entityToTableMap = new Dictionary<DynamicTableEntity, ITable>();


            List<DynamicTableEntity> latestVersionEntities = new List<DynamicTableEntity>();
            List<DynamicTableEntity> allEntities = new List<DynamicTableEntity>();
            DynamicTableEntity consensusEntity = null;
            long newVersionToWrite = -1;
            bool keyNotFoundOnQuorum = false;

            foreach (ITable table in hatsTables)
            {
                Task<IEnumerable<DynamicTableEntity>> task = GetTopTwoRowsForKey(incomingEntity.PartitionKey, incomingEntity.RowKey, table);
                task.ObserveAsyncException();
                taskToTableMap.Add(task, table);
            }

            Tuple<DynamicTableEntity, bool> earlyConsensus = await this.TryReachingEarlyConsensus(
                taskToTableMap,
                entityToTableMap,
                failedTasks,
                succeededTasks,
                latestVersionEntities,
                allEntities);

            consensusEntity = earlyConsensus.Item1;
            keyNotFoundOnQuorum = earlyConsensus.Item2;

            if (consensusEntity == null && keyNotFoundOnQuorum == false)
            {
                Tuple<DynamicTableEntity, bool> finalConsensus = this.TryReachingFinalConsensus(
                                                            succeededTasks,
                                                            latestVersionEntities,
                                                            allEntities);
                consensusEntity = finalConsensus.Item1;
                keyNotFoundOnQuorum = finalConsensus.Item2;
            }
 
            bool keyIsDeletedOnQuorum = consensusEntity != null && consensusEntity.Properties[HatsPropertyIsdeleted].BooleanValue.Value;
            this.ValidateOperationTypeAndETag((!keyNotFoundOnQuorum && !keyIsDeletedOnQuorum), operationType, incomingEntity, consensusEntity);

            // Our new version is always one less than the previous version
            newVersionToWrite = keyNotFoundOnQuorum ? StartingVersion : consensusEntity.Properties[HatsPropertyEntityVersion].Int64Value.Value - 1;

            // Build up a new entity with hats properties
            DynamicTableEntity newEntityToWrite = Hats.BuildHatsEntityToWrite(incomingEntity, newVersionToWrite, operationType == TableOperationType.Delete, transactionId);
            
            // Merge if required
            if (!keyNotFoundOnQuorum && 
                !keyIsDeletedOnQuorum &&
                (operationType == TableOperationType.Merge || operationType == TableOperationType.InsertOrMerge))
            {
                Hats.MergeValuesWithConsensus(newEntityToWrite, consensusEntity);
            }

            return newEntityToWrite;
        }

        void ValidateOperationTypeAndETag(bool keyUpdate, TableOperationType operationType, ITableEntity incomingEntity, DynamicTableEntity consensusEntity)
        {
            if (keyUpdate)
            {
                if (operationType == TableOperationType.Insert)
                {
                    throw new AlredyExistsException();
                }

                if (operationType == TableOperationType.Replace
                    || operationType == TableOperationType.Merge
                    || operationType == TableOperationType.Delete)
                {
                    if (incomingEntity.ETag == null || 
                        (incomingEntity.ETag != "*" &&
                        long.Parse(incomingEntity.ETag) != consensusEntity.Properties[HatsPropertyEntityVersion].Int64Value.Value))
                    {
                        throw new ETagMismatchException();
                    }
                }
            }
            else
            {
                if (operationType == TableOperationType.Replace
                     || operationType == TableOperationType.Merge
                     || operationType == TableOperationType.Delete)
                {
                    throw new NotFoundException();
                }
            }
        }

        async Task<DynamicTableEntity> InsertIntoAllAzureTables(ITableEntity entity)
        {
            Dictionary<Task<TableResult>, ITable> taskToTableMap = new Dictionary<Task<TableResult>, ITable>();
            Dictionary<Task<TableResult>, ITable> succeededTaskToTableMap = new Dictionary<Task<TableResult>, ITable>();
            Dictionary<Task<TableResult>, ITable> failedTaskToTableMap = new Dictionary<Task<TableResult>, ITable>();
            DynamicTableEntity consensusEntity = null;

            foreach (ITable table in hatsTables)
            {
                Task<TableResult> task = table.ExecuteAsync(new HatsOperation(entity, TableOperationType.Insert));
                task.ObserveAsyncException();
                taskToTableMap.Add(task, table);
            }

            while (taskToTableMap.Any())
            {
                Task<TableResult> finishedTask = await Task.WhenAny(taskToTableMap.Keys.ToArray());
                ITable table = taskToTableMap[finishedTask];
                taskToTableMap.Remove(finishedTask);

                if (finishedTask.IsFaulted)
                {
                   failedTaskToTableMap.Add(finishedTask, table);
                    if (failedTaskToTableMap.Count() >= this.quorum)
                    {
                        break;
                    }
                }
                else
                {
                    succeededTaskToTableMap.Add(finishedTask, table);
                    if (succeededTaskToTableMap.Count() >= this.quorum)
                    {
                        consensusEntity = (DynamicTableEntity)succeededTaskToTableMap.Keys.First().Result.Result;
                        break;
                    }
                }
            }

            if (consensusEntity != null)
            {
                Task.Run(async () =>
                {
                    while (taskToTableMap.Any())
                    {
                        Task<TableResult> finishedTask = await Task.WhenAny(taskToTableMap.Keys.ToArray());
                        ITable table = taskToTableMap[finishedTask];
                        taskToTableMap.Remove(finishedTask);

                        if (finishedTask.IsFaulted)
                        {
                            failedTaskToTableMap.Add(finishedTask, table);
                        }
                        else
                        {
                            succeededTaskToTableMap.Add(finishedTask, table);
                        }
                    }

                    // for each faulted one - force write the new one 
                    foreach (Task<TableResult> failedTask in failedTaskToTableMap.Keys)
                    {
                        // Check whether the fault is because of conflict - because some other writer raced and lost
                        // This winner can now go and overwrite whatever dirty row exists there
                        if (IsConflictException(failedTask.Exception.InnerException))
                        {
                            await Hats.ForceWriteConsenusToAzureTable(consensusEntity, failedTaskToTableMap[failedTask]);
                        }
                        else
                        {
                            // TODO - Mark as committed on the succeeded one
                        }
                    }
                }).ObserveAsyncException();
            }
            else
            {
                Task.Run(async () =>
                {
                    if (failedTaskToTableMap.Keys.ToList().Any(failedTask => !Hats.IsConflictException(failedTask.Exception.InnerException)))
                    {
                        foreach (Task<TableResult> succeededTask in succeededTaskToTableMap.Keys)
                        {
                            succeededTaskToTableMap.Remove(succeededTask);
                        }
                    }

                    while (taskToTableMap.Any())
                    {
                        Task<TableResult> finishedTask = await Task.WhenAny(taskToTableMap.Keys.ToArray());
                        ITable table = taskToTableMap[finishedTask];
                        taskToTableMap.Remove(finishedTask);

                        if (!finishedTask.IsFaulted)
                        {
                            succeededTaskToTableMap.Add(finishedTask, table);
                        }
                    }

                    // Do this again - because there could be a case where first 2 failed and we returned - but the last one succeeded
                    if (failedTaskToTableMap.Keys.ToList().Any(failedTask => !Hats.IsConflictException(failedTask.Exception.InnerException)))
                    {
                        foreach (Task<TableResult> succeededTask in succeededTaskToTableMap.Keys)
                        {
                            succeededTaskToTableMap.Remove(succeededTask);
                        }
                    }
               }).ObserveAsyncException();

                throw new QuorumFailureException(new AggregateException(failedTaskToTableMap.Keys.Select(failedTask => failedTask.Exception)));
            }

            return consensusEntity;
        }

        static bool IsConflictException(Exception ex)
        {
            return  ((ex is StorageException) &&
                     ((StorageException)ex).RequestInformation != null &&
                     ((StorageException)ex).RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict);
        }

        async Task ForceWriteConsenus(
           Dictionary<Task<IEnumerable<DynamicTableEntity>>, ITable> taskToTableMap,
           List<DynamicTableEntity> allEntities,
           Dictionary<DynamicTableEntity, ITable> entityToTableMap,
           DynamicTableEntity consensusEntity)
        {
            // This asynchrnous task writes consensus into the table that does not have it
            // Note that consensus can also be a deleted entry - even that needs to be forcibly written
            while (taskToTableMap.Any())
            {
                Task<IEnumerable<DynamicTableEntity>> finishedTask = await Task.WhenAny(taskToTableMap.Keys.ToArray());
                ITable table = taskToTableMap[finishedTask];
                taskToTableMap.Remove(finishedTask);

                if (!finishedTask.IsFaulted)
                {
                    int countOfRows = 0;
                    foreach (DynamicTableEntity entity in finishedTask.Result)
                    {
                        allEntities.Add(entity);
                        entityToTableMap.Add(entity, table);
                        countOfRows++;
                        if (countOfRows == 2)
                        {
                            // Stop at 2 rows - if not table store will go get 2 more rows again 
                            break;
                        }
                    }
                }
            }

            List<ITable> tablesToFix = new List<ITable>(this.hatsTables);
            foreach (DynamicTableEntity entity in allEntities)
            {
                if (entity.Properties[HatsPropertyTransactionId].StringValue ==
                    consensusEntity.Properties[HatsPropertyTransactionId].StringValue)
                {
                    tablesToFix.Remove(entityToTableMap[entity]);
                }

                if (!tablesToFix.Any())
                {
                    break;
                }
            }

            foreach (ITable hatsTable in tablesToFix)
            {
                await Hats.ForceWriteConsenusToAzureTable(consensusEntity, hatsTable);
            }
        }

        static async Task ForceWriteConsenusToAzureTable(DynamicTableEntity entity, ITable table)
        {
            try
            {
                 await table.ExecuteAsync(new HatsOperation(new DynamicTableEntity(entity.PartitionKey, entity.RowKey, "*", entity.Properties), TableOperationType.InsertOrReplace));
            }
            catch (StorageException storageException)
            {
                // Conflict can happen because some other reader also is doing it in parallel
                if (!IsConflictException(storageException))
                {
                    throw;
                }
            }
        }

        bool IsQuorumConsensusReachedUsingLatestRows(List<DynamicTableEntity> entities,  out DynamicTableEntity consensusEntity)
        {
            consensusEntity = null;
            IEnumerable<IGrouping<string, DynamicTableEntity>> groups = entities.GroupBy(entity => entity.Properties[HatsPropertyTransactionId].StringValue);
            foreach (IGrouping<string, DynamicTableEntity> group in groups)
            {
                if (group.Count() >= this.quorum)
                {
                    consensusEntity = group.First();
                    return true;
                }
            }

            return false;
        }

        bool IsQuorumConsensusReachedUsingAllRows(List<DynamicTableEntity> entities, out DynamicTableEntity consensusEntity)
        {
            consensusEntity = null;
            long consensusVersion = long.MaxValue;
            IEnumerable<IGrouping<string, DynamicTableEntity>> groups = entities.GroupBy(entity => entity.Properties[HatsPropertyTransactionId].StringValue);
            foreach (IGrouping<string, DynamicTableEntity> group in groups)
            {
                if (group.Count() >= this.quorum)
                {
                    long version = group.First().Properties[HatsPropertyEntityVersion].Int64Value.Value;
                    if (version < consensusVersion)
                    {
                        consensusEntity = group.First();
                        consensusVersion = version;
                    }
                }
            }

            if (consensusVersion != long.MaxValue)
            {
                return true;
            }

            return false;
        }
        
        static DynamicTableEntity BuildHatsEntityToWrite(ITableEntity entity, long version, bool isDelete, string transactionId)
        {
            string newRowKey = string.Format(RowKeyWithPostfix, entity.RowKey, version);

            // We are not using the constructor with 4 paremeters because it copies properties by reference
            DynamicTableEntity hatsEntity = new DynamicTableEntity(entity.PartitionKey, newRowKey);
            hatsEntity.ETag = entity.ETag;
            foreach (KeyValuePair<string, EntityProperty> kvp in entity.WriteEntity(null))
            {
                hatsEntity.Properties.Add(kvp);
            }

            hatsEntity.Properties.Add(HatsPropertySchemaVersion, new EntityProperty(HatsSchemaVersion.Nov2014.ToString()));
            hatsEntity.Properties.Add(HatsPropertyIsdeleted, new EntityProperty(isDelete));
            hatsEntity.Properties.Add(HatsPropertyTransactionId, new EntityProperty(transactionId));
            hatsEntity.Properties.Add(HatsPropertyEntityVersion, new EntityProperty(version));
            return hatsEntity;
        }

        static void MergeValuesWithConsensus(DynamicTableEntity entity, DynamicTableEntity consensus)
        {
            foreach (KeyValuePair<string, EntityProperty> kvp in consensus.Properties)
            {
                if (!entity.Properties.ContainsKey(kvp.Key))
                {
                    entity.Properties.Add(kvp);
                }
            }
        }

        static DynamicTableEntity ModifyEntityBeforeResponse(DynamicTableEntity entity, TableOperationType operationType)
        {
            DynamicTableEntity entityToReturn = new DynamicTableEntity(entity.PartitionKey, entity.RowKey.Substring(0, entity.RowKey.Length - 20));
            entityToReturn.ETag = operationType == TableOperationType.Delete ? null : entity.Properties[HatsPropertyEntityVersion].Int64Value.ToString();
            foreach (KeyValuePair<string, EntityProperty> kvp in entity.WriteEntity(null))
            {
                if (!kvp.Key.StartsWith(HatsPropertyPrefix))
                {
                    entityToReturn.Properties.Add(kvp);
                }
            }

            return entityToReturn;
        }

        Task<IEnumerable<DynamicTableEntity>> GetTopTwoRowsForKey(string partitionKey, string rowKey, ITable table)
        {
            string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            string rkFrom = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, string.Format(RowKeyWithPostfix, rowKey, BiggestVersion));
            string rkTo = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, string.Format(RowKeyWithPostfix, rowKey, SmallestVersion));
            string combinedFilter = TableQuery.CombineFilters(rkFrom, TableOperators.And, rkTo);
            combinedFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, combinedFilter);

            TableQuery<DynamicTableEntity> query = new TableQuery<DynamicTableEntity>().Where(combinedFilter).Take(2);
            return table.GetTopTwoEntities(query);
        }
    }
}

/*
 * 
			        ETagRequired 	IfETagIs*, got conflict, should retry? 
Insert			    No	 	        No retry
Replace			    Yes		        Yes
Merge 			    Yes		        Yes but with new consensus
InsertOrReplace		No		        Yes
InsertOrMerge 		No		        Yes but with new consensus
Delete			    Yes		        Yes


HATS testcase - 100 concurrent InsertOrMerge on same entity - all 100 new properties should exist

Auto merge resolution during 3 can also depeendent upon 
Write - indoubt/committed/operationType rows
Write - operation type

*/