namespace Datastore.KeyTransform
{
    public delegate DatastoreKey KeyMapping(DatastoreKey key);

    public class KeyTransformPair : IKeyTransform
    {
        public KeyMapping Convert { get; }
        public KeyMapping Invert { get; }

        public KeyTransformPair(KeyMapping convert, KeyMapping invert)
        {
            Convert = convert;
            Invert = invert;
        }

        public DatastoreKey ConvertKey(DatastoreKey key) => Convert(key);
        public DatastoreKey InvertKey(DatastoreKey key) => Invert(key);
    }
}
