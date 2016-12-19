using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.Coalesce
{
    public class CoalescingDatastore<T> : IDatastore<T>
    {
        private static readonly DatastoreKey PutKey = new DatastoreKey("put");
        private static readonly DatastoreKey GetKey = new DatastoreKey("get");
        private static readonly DatastoreKey HasKey = new DatastoreKey("has");
        private static readonly DatastoreKey DeleteKey = new DatastoreKey("delete");

        private readonly IDatastore<T> _child;
        private readonly ConcurrentDictionary<KeySync, ValueSyc> _req;

        private struct KeySync
        {
            public string op;
            public DatastoreKey k;
            public object value;
        }

        private struct ValueSyc
        {
            public object value;
            public Exception error;
            public ManualResetEvent done;

            public ValueSyc(object value = null, Exception error = null, ManualResetEvent done = null)
            {
                this.value = value;
                this.error = error;
                this.done = done ?? new ManualResetEvent(false);
            }
        }

        public CoalescingDatastore(IDatastore<T> child)
        {
            _child = child;
            _req = new ConcurrentDictionary<KeySync, ValueSyc>();
        }

        private bool Sync(KeySync ks, out ValueSyc vs)
        {
            if (!_req.TryGetValue(ks, out vs))
            {
                vs = new ValueSyc(done: new ManualResetEvent(false));
                _req.TryAdd(ks, vs);
                return false;
            }
            else
            {
                vs.done.Set();
                return true;
            }
        }

        private void Sync(KeySync ks)
        {
            ValueSyc vs;
            if (!_req.TryRemove(ks, out vs))
                throw new Exception("attemping to sync non-existing request");

            vs.done.Set();
        }

        public void Dispose()
        {

        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            var ks = new KeySync {op = PutKey, k = datastoreKey, value = value};
            ValueSyc vs;
            if (!Sync(ks, out vs))
            {
                _child.Put(datastoreKey, value);
                Sync(ks);
            }
        }

        public T Get(DatastoreKey datastoreKey)
        {
            var ks = new KeySync { op = GetKey, k = datastoreKey };
            ValueSyc vs;
            if (!Sync(ks, out vs))
            {
                vs.value = _child.Get(datastoreKey);
                Sync(ks);
            }
            return (T)vs.value;
        }

        public bool Has(DatastoreKey datastoreKey)
        {
            var ks = new KeySync { op = HasKey, k = datastoreKey };
            ValueSyc vs;
            if (!Sync(ks, out vs))
            {
                vs.value = _child.Has(datastoreKey);
                Sync(ks);
            }
            return (bool)vs.value;
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            var ks = new KeySync { op = DeleteKey, k = datastoreKey };
            ValueSyc vs;
            if (!Sync(ks, out vs))
            {
                _child.Delete(datastoreKey);
                Sync(ks);
            }
        }

        public DatastoreResults<T> Query(DatastoreQuery<T> q) => _child.Query(q);
        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}
