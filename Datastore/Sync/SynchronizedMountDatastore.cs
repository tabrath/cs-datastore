using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Extensions;
using Datastore.Mount;

namespace Datastore.Sync
{
    public class SynchronizedMountDatastore<T> : MountDatastore<T>, IThreadSafeDatastore<T>
    {
        public bool IsThreadSafe => true;

        private readonly ReaderWriterLockSlim _lock;

        public SynchronizedMountDatastore(params DatastoreMount<T>[] mounts)
            : base(mounts)
        {
            _lock = new ReaderWriterLockSlim();
        }

        protected override bool Lookup(DatastoreKey key, out IDatastore<T> datastore, out DatastoreKey mountPoint,
            out DatastoreKey rest)
        {
            _lock.EnterWriteLock();
            try
            {
                return base.Lookup(key, out datastore, out mountPoint, out rest);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public override void Dispose()
        {
            _lock.Write(base.Dispose);
            _lock.Dispose();
        }

        public override IDatastoreBatch<T> Batch() => new SynchronizedMountBatch(this, _lock);

        protected class SynchronizedMountBatch : MountBatch
        {
            private readonly ReaderWriterLockSlim _lock;

            public SynchronizedMountBatch(MountDatastore<T> ds, ReaderWriterLockSlim @lock)
                : base(ds)
            {
                _lock = @lock;
            }

            protected override bool Lookup(DatastoreKey key, out IDatastoreBatch<T> batch, out DatastoreKey rest)
            {
                _lock.EnterWriteLock();
                try
                {
                    return base.Lookup(key, out batch, out rest);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }
        }
    }
}
