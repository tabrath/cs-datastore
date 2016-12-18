using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Extensions;
using Datastore.Query;

namespace Datastore.Sync
{
    public class SynchronizedDatastore<T> : IBatchingDatastore<T>, IDatastoreShim<T>, IThreadSafeDatastore<T>
    {
        private readonly IDatastore<T> _child;
        private readonly ReaderWriterLockSlim _lock;

        public IDatastore<T>[] Children => new[] {_child};
        public bool IsThreadSafe => true;

        public SynchronizedDatastore(IDatastore<T> child)
        {
            _child = child;
            _lock = new ReaderWriterLockSlim();
        }

        public IDatastoreBatch<T> Batch()
        {
            return _lock.Read(() =>
            {
                var bds = _child as IBatchingDatastore<T>;
                if (bds == null)
                    throw new NotSupportedException();

                var batch = bds.Batch();

                return new SynchronizedBatch(batch, _lock);
            });
        }

        public void Dispose()
        {
            _lock.Write(_child.Dispose);
            _lock.Dispose();
        }

        public void Put(DatastoreKey datastoreKey, T value) => _lock.Write(() => _child.Put(datastoreKey, value));
        public T Get(DatastoreKey datastoreKey) => _lock.Read(() => _child.Get(datastoreKey));
        public bool Has(DatastoreKey datastoreKey) => _lock.Read(() => _child.Has(datastoreKey));
        public void Delete(DatastoreKey datastoreKey) => _lock.Write(() => _child.Delete(datastoreKey));
        public DatastoreResults<T> Query(DatastoreQuery<T> q) => _lock.Read(() => _child.Query(q));
        public IThreadSafeDatastore<T> Synchronized() => this;

        private class SynchronizedBatch : IDatastoreBatch<T>
        {
            private readonly IDatastoreBatch<T> _batch;
            private readonly ReaderWriterLockSlim _lock;

            public SynchronizedBatch(IDatastoreBatch<T> batch, ReaderWriterLockSlim @lock)
            {
                _batch = batch;
                _lock = @lock;
            }

            public void Put(DatastoreKey datastoreKey, T value) => _lock.Write(() => _batch.Put(datastoreKey, value));
            public void Delete(DatastoreKey datastoreKey) => _lock.Write(() => _batch.Delete(datastoreKey));
            public void Commit() => _lock.Write(_batch.Commit);
        }
    }
}
