using System;
using System.Collections.Generic;
using Datastore.KeyTransform;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.Mount
{
    public class MountDatastore<T> : IBatchingDatastore<T>
    {
        private readonly DatastoreMount<T>[] _mounts;

        public MountDatastore(params DatastoreMount<T>[] mounts)
        {
            _mounts = mounts;
        }

        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedMountDatastore<T>(_mounts);

        protected virtual bool Lookup(DatastoreKey key, out IDatastore<T> datastore, out DatastoreKey mountPoint, out DatastoreKey rest)
        {
            foreach (var mount in _mounts)
            {
                if (mount.Prefix.Equals(key) || mount.Prefix.IsAncestorOf(key))
                {
                    var s = key.ToString().Substring(mount.Prefix.ToString().Length);
                    rest = new DatastoreKey(s);
                    datastore = mount.Datastore;
                    mountPoint = mount.Prefix;
                    return true;
                }
            }

            datastore = null;
            mountPoint = new DatastoreKey("/");
            rest = key;
            return false;
        }

        public virtual IDatastoreBatch<T> Batch() => new MountBatch(this);

        public virtual void Dispose()
        {
            foreach (var mount in _mounts)
            {
                mount.Datastore?.Dispose();
            }
        }

        public virtual void Put(DatastoreKey datastoreKey, T value)
        {
            IDatastore<T> ds;
            DatastoreKey mp, k;
            if (!Lookup(datastoreKey, out ds, out mp, out k))
                throw new Exception("No mount found");

            ds.Put(k, value);
        }

        public virtual T Get(DatastoreKey datastoreKey)
        {
            IDatastore<T> ds;
            DatastoreKey mp, k;
            if (!Lookup(datastoreKey, out ds, out mp, out k))
                throw new Exception("No mount found");

            return ds.Get(k);
        }

        public virtual bool Has(DatastoreKey datastoreKey)
        {
            IDatastore<T> ds;
            DatastoreKey mp, k;
            if (!Lookup(datastoreKey, out ds, out mp, out k))
                throw new Exception("No mount found");

            return ds.Has(k);
        }

        public virtual void Delete(DatastoreKey datastoreKey)
        {
            IDatastore<T> ds;
            DatastoreKey mp, k;
            if (!Lookup(datastoreKey, out ds, out mp, out k))
                throw new Exception("No mount found");

            ds.Delete(k);
        }

        public virtual DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            if (q.QueryFilters.Length > 0 ||
                q.QueryOrders.Length > 0 ||
                q.Limit > 0 ||
                q.Offset > 0)
            {
                throw new Exception("mount only supports listing all prefixed keys in random order");
            }

            var key = new DatastoreKey(q.Prefix);
            IDatastore<T> ds;
            DatastoreKey mp, k;
            if (!Lookup(key, out ds, out mp, out k))
                throw new Exception("Mount only supports listing a mount point");

            var q2 = new DatastoreQuery<T>(prefix: k);
            var wrapDS = new KeyTransformDatastore<T>(ds, new KeyTransformPair(null, x => mp.Child(x)));
            var r = wrapDS.Query(q2);

            return DatastoreResults<T>.ReplaceQuery(r, q);
        }

        protected class MountBatch : IDatastoreBatch<T>
        {
            private readonly MountDatastore<T> _ds;
            private readonly Dictionary<string, IDatastoreBatch<T>> _mounts;

            public MountBatch(MountDatastore<T> ds)
            {
                _ds = ds;
                _mounts = new Dictionary<string, IDatastoreBatch<T>>();
            }

            protected virtual bool Lookup(DatastoreKey key, out IDatastoreBatch<T> batch, out DatastoreKey rest)
            {
                IDatastore<T> child;
                DatastoreKey loc;
                if (!_ds.Lookup(key, out child, out loc, out rest))
                {
                    batch = null;
                    return false;
                }

                if (_mounts.ContainsKey(loc))
                {
                    batch = _mounts[loc];
                    return true;
                }

                var bds = child as IBatchingDatastore<T>;
                if (bds == null)
                    throw new Exception("batch not supported");

                batch = bds.Batch();
                _mounts.Add(loc, batch);
                return true;
            }

            public virtual void Put(DatastoreKey datastoreKey, T value)
            {
                IDatastoreBatch<T> batch;
                DatastoreKey rest;
                if (!Lookup(datastoreKey, out batch, out rest))
                    throw new NotSupportedException();

                batch.Put(rest, value);
            }

            public virtual void Delete(DatastoreKey datastoreKey)
            {
                IDatastoreBatch<T> batch;
                DatastoreKey rest;
                if (!Lookup(datastoreKey, out batch, out rest))
                    throw new NotSupportedException();

                batch.Delete(rest);
            }

            public virtual void Commit()
            {
                foreach (var m in _mounts)
                {
                    m.Value.Commit();
                }
            }
        }
    }
}