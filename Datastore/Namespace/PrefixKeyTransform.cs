using System;
using Datastore.KeyTransform;

namespace Datastore.Namespace
{
    public class PrefixKeyTransform : IKeyTransform
    {
        private readonly DatastoreKey _prefix;

        public PrefixKeyTransform(DatastoreKey prefix)
        {
            _prefix = prefix;
        }

        public DatastoreKey ConvertKey(DatastoreKey key) => _prefix.Child(key);

        public DatastoreKey InvertKey(DatastoreKey key)
        {
            if (_prefix == "/")
                return key;

            if (!_prefix.IsAncestorOf(key))
                throw new Exception("expected prefix not found");

            return new DatastoreKey(key.ToString().Substring(_prefix.ToString().Length), true);
        }
    }
}
