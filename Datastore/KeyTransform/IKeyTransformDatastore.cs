namespace Datastore.KeyTransform
{
    public interface IKeyTransformDatastore<T> : IDatastoreShim<T>, IKeyTransform, IBatchingDatastore<T>
    {
    }
}