namespace Datastore
{
    public interface IDatastoreShim<T> : IDatastore<T>
    {
        IDatastore<T>[] Children { get; }
    }
}