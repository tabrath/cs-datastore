using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Query;
using Datastore.Sync;

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

            var ch = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
            {
                foreach (var result in qr.Next())
                {
                    if (result.Error == null)
                    {
                        if (!ch.TryAdd(new DatastoreResult<T>(InvertKey(result.DatastoreKey), result.Value), Timeout.Infinite, CancellationToken.None))
                            break;
                    }
                }
            }).ContinueWith(_ => ch.CompleteAdding());

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