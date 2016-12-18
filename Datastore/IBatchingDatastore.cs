namespace Datastore
{
    public interface IBatchingDatastore<T> : IDatastore<T>
    {
        IDatastoreBatch<T> Batch();
    }
}