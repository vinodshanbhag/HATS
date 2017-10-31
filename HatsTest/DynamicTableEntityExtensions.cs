using Microsoft.WindowsAzure.Storage.Table;
using System;


namespace Microsoft.WindowsAzure.Storage.TableTest
{
    public static class DynamicTableEntityExtensions
    {
        public static bool IsSameAs(this DynamicTableEntity entity, DynamicTableEntity entityToCompare)
        {
            if (entity.PartitionKey != entityToCompare.PartitionKey ||
                entity.RowKey != entityToCompare.RowKey ||
                entity.Properties.Count != entityToCompare.Properties.Count)
                return false;

            foreach (string key in entity.Properties.Keys)
            {
                if (entity[key].StringValue != entityToCompare[key].StringValue)
                    return false;
            }

            return true;
        }

        public static DynamicTableEntity GetRandomEntity()
        {
            DynamicTableEntity entity = new DynamicTableEntity(Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
            entity.Properties.Add("Prop" + Guid.NewGuid().ToString("N"), new EntityProperty(Guid.NewGuid().ToString()));
            return entity;
        }

    }
}
