namespace Datastore.KeyTransform
{
    public interface IKeyTransform
    {
        DatastoreKey ConvertKey(DatastoreKey key);
        DatastoreKey InvertKey(DatastoreKey key);
    }
}