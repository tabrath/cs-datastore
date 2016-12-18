namespace Datastore.Query
{
    public class DatastoreEntry<T>
    {
        public DatastoreKey DatastoreKey { get; }
        public T Value { get; }

        public DatastoreEntry(DatastoreKey datastoreKey, T value)
        {
            DatastoreKey = datastoreKey;
            Value = value;
        }
    }
}