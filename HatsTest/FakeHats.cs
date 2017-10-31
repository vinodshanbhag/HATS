using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace Microsoft.WindowsAzure.Storage.TableTest
{
    static class FakeHats
    {
        public static DynamicTableEntity ConvertFakeToOriginalEntity(DynamicTableEntity entity)
        {
            DynamicTableEntity entityToReturn = new DynamicTableEntity(entity.PartitionKey, entity.RowKey.Substring(0, entity.RowKey.Length - 20));
            entityToReturn.ETag = entity.Properties[Hats.HatsPropertyEntityVersion].Int64Value.ToString();

            foreach (KeyValuePair<string, EntityProperty> kvp in entity.WriteEntity(null))
            {
                if (!kvp.Key.StartsWith(Hats.HatsPropertyPrefix))
                {
                    entityToReturn.Properties.Add(kvp);
                }
            }

            return entityToReturn;
        }

        public static DynamicTableEntity BuildfakeHatsEntityToWrite(ITableEntity entity, long version, bool isDelete)
        {
            version = GetHatsVersion(version);
            string transactionId = Guid.NewGuid().ToString();
            string newRowKey = string.Format(Hats.RowKeyWithPostfix, entity.RowKey, version);

            // We are not using the constructor with 4 paremeters because it copies properties by reference
            DynamicTableEntity hatsEntity = new DynamicTableEntity(entity.PartitionKey, newRowKey);
            hatsEntity.ETag = entity.ETag;
            foreach (KeyValuePair<string, EntityProperty> kvp in entity.WriteEntity(null))
            {
                hatsEntity.Properties.Add(kvp);
            }

            hatsEntity.Properties.Add(Hats.HatsPropertySchemaVersion, new EntityProperty(HatsSchemaVersion.Nov2014.ToString()));
            hatsEntity.Properties.Add(Hats.HatsPropertyIsdeleted, new EntityProperty(isDelete));
            hatsEntity.Properties.Add(Hats.HatsPropertyTransactionId, new EntityProperty(transactionId));
            hatsEntity.Properties.Add(Hats.HatsPropertyEntityVersion, new EntityProperty(version));
            return hatsEntity;
        }

        // Assumes 1 based index
        static long GetHatsVersion(long version)
        {
            return Hats.StartingVersion + 1 - version;
        }
    }
}
