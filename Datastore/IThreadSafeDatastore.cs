namespace Datastore
{
    public interface IThreadSafeDatastore<T> : IDatastore<T>
    {
        bool IsThreadSafe { get; }
    }
}