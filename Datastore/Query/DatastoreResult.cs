using System;

namespace Datastore.Query
{
    public class DatastoreResult<T> : DatastoreEntry<T>
    {
        public Exception Error { get; }

        public DatastoreResult(DatastoreKey datastoreKey, T value, Exception error = null)
            : base(datastoreKey, value)
        {
            Error = error;
        }

        public DatastoreResult(DatastoreEntry<T> e)
            : base(e.DatastoreKey, e.Value)
        {
        }
    }
}