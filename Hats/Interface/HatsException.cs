using System;

namespace Microsoft.WindowsAzure.Storage.Table
{
    // All exceptions coming form HATS should be derived from HatsException
    public class HatsException : Exception
    {
        public HatsException(string message)
            : base(message)
        {
        }

        public HatsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    public class QuorumFailureException : HatsException
    {
        public QuorumFailureException(Exception innerException)
            : base("Multiple failures - Quorum can't be reached", innerException)
        {
        }
    }

    public class NoQuroumConsensusException : HatsException
    {
        public NoQuroumConsensusException()
            : base("Quorum is not in consensus")
        {
        }
    }

    public class NotFoundException : HatsException
    {
        public NotFoundException()
            : base("No row found for this partitionkey/rowKey combination. Replace and Merge operations requires a row to exist.")
        {
        }
    }

    public class AlredyExistsException : HatsException
    {
        public AlredyExistsException()
            : base("A row already exists for this partitionkey/rowKey combination. Insert requires a row to not exist.")
        {
        }
    }

    public class ETagMismatchException : HatsException
    {
        public ETagMismatchException()
            : base("The current ETag and ETag provided in the API dont match.")
        {
        }
    }
}
