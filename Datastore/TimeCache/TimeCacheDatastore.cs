using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Extensions;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.TimeCache
{
    public class TimeCacheDatastore<T> : IDatastore<T>
    {
        private readonly IDatastore<T> _ds;
        private readonly TimeSpan _ttl;
        private readonly Dictionary<DatastoreKey, DateTime> _ttls;
        private readonly SemaphoreSlim _lock;

        public TimeCacheDatastore(TimeSpan ttl)
            : this(new MapDatastore<T>(), ttl)
        {
        }

        public TimeCacheDatastore(IDatastore<T> ds, TimeSpan ttl)
        {
            _ds = ds;
            _ttl = ttl;
            _ttls = new Dictionary<DatastoreKey, DateTime>();
            _lock = new SemaphoreSlim(1, 1);
        }

        private void GC()
        {
            var now = DateTime.Now;
            var del = new List<DatastoreKey>();

            _lock.Wait();
            try
            {
                foreach (var ttl in _ttls)
                {
                    if (ttl.Value > now)
                    {
                        del.Add(ttl.Key);
                    }
                }
                foreach (var ttl in del)
                {
                    _ttls.Remove(ttl);
                }
            }
            finally
            {
                _lock.Release();
            }

            foreach (var k in del)
            {
                _ds.Delete(k);
            }
        }

        public void Dispose()
        {
            _lock.Dispose();
            _ds.Dispose();
        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            _ds.Put(datastoreKey, value);
            _lock.Lock(() => _ttls.Add(datastoreKey, DateTime.Now.Add(_ttl)));
        }

        public T Get(DatastoreKey datastoreKey)
        {
            GC();
            return _ds.Get(datastoreKey);
        }

        public bool Has(DatastoreKey datastoreKey)
        {
            GC();
            return _ds.Has(datastoreKey);
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            _lock.Lock(() => _ttls.Remove(datastoreKey));
            _ds.Delete(datastoreKey);
        }

        public DatastoreResults<T> Query(DatastoreQuery<T> q) => _ds.Query(q);
        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}
