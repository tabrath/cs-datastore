using System;
using Datastore.Query;
using Datastore.Sync;
using NChannels;

namespace Datastore.KeyTransform
{
    public class KeyTransformDatastore<T> : IKeyTransformDatastore<T>
    {
        protected readonly IDatastore<T> _child;
        private readonly IKeyTransform _keyTransform;

        public IDatastore<T>[] Children => new[] { _child };

        public KeyTransformDatastore(IDatastore<T> child, IKeyTransform keyTransform)
        {
            _child = child;
            _keyTransform = keyTransform;
        }

        public virtual IDatastoreBatch<T> Batch()
        {
            var bds = _child as IBatchingDatastore<T>;
            if (bds == null)
                throw new NotSupportedException("batching is not supported");

            return new TransformBatch<T>(bds.Batch(), ConvertKey);
        }

        public void Dispose() => _child.Dispose();
        public virtual void Put(DatastoreKey datastoreKey, T value) => _child.Put(ConvertKey(datastoreKey), value);
        public virtual T Get(DatastoreKey datastoreKey) => _child.Get(ConvertKey(datastoreKey));
        public virtual bool Has(DatastoreKey datastoreKey) => _child.Has(ConvertKey(datastoreKey));
        public virtual void Delete(DatastoreKey datastoreKey) => _child.Delete(ConvertKey(datastoreKey));

        public virtual DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            var qr = _child.Query(q);

            var ch = new Chan<DatastoreResult<T>>();

            qr.Next()
                .Where(r => r.Error == null)
                .Select(r => new DatastoreResult<T>(InvertKey(r.DatastoreKey), r.Value))
                .Forward(ch)
                .ContinueWith(_ => ch.Close());

            return qr.DerivedResults(ch);
        }

        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);

        public DatastoreKey ConvertKey(DatastoreKey key) => _keyTransform.ConvertKey(key);
        public DatastoreKey InvertKey(DatastoreKey key) => _keyTransform.InvertKey(key);

        private class TransformBatch<T> : IDatastoreBatch<T>
        {
            private readonly IDatastoreBatch<T> _child;
            private readonly KeyMapping _convert;

            public TransformBatch(IDatastoreBatch<T> child, KeyMapping convert)
            {
                _child = child;
                _convert = convert;
            }

            public void Put(DatastoreKey datastoreKey, T value) => _child.Put(_convert(datastoreKey), value);
            public void Delete(DatastoreKey datastoreKey) => _child.Delete(_convert(datastoreKey));
            public void Commit() => _child.Commit();
        }
    }
}