namespace Datastore
{
    public interface IDatastoreBatch<T>
    {
        void Put(DatastoreKey datastoreKey, T value);
        void Delete(DatastoreKey datastoreKey);
        void Commit();
    }
}