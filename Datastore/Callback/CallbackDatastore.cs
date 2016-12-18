using System;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.Callback
{
    public class CallbackDatastore<T> : IDatastore<T>
    {
        private readonly IDatastore<T> _ds;
        private readonly Action _callback;

        public CallbackDatastore(IDatastore<T> ds, Action callback)
        {
            _ds = ds;
            _callback = callback;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            _callback();
            _ds.Put(datastoreKey, value);
        }

        public T Get(DatastoreKey datastoreKey)
        {
            _callback();
            return _ds.Get(datastoreKey);
        }

        public bool Has(DatastoreKey datastoreKey)
        {
            _callback();
            return _ds.Has(datastoreKey);
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            _callback();
            _ds.Delete(datastoreKey);
        }

        public DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            _callback();
            return _ds.Query(q);
        }

        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}
