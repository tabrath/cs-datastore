using System;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore
{
    public class NullDatastore<T> : IBatchingDatastore<T>
        where T : class
    {
        public NullDatastore() { }

        public IDatastoreBatch<T> Batch() => new BasicDatastoreBatch<T>(this);
        public void Dispose() { }
        public void Put(DatastoreKey datastoreKey, T value) { }
        public T Get(DatastoreKey datastoreKey) => null;
        public bool Has(DatastoreKey datastoreKey) => false;
        public void Delete(DatastoreKey datastoreKey) { }
        public DatastoreResults<T> Query(DatastoreQuery<T> q) => DatastoreResults<T>.WithEntries(q, Array.Empty<DatastoreEntry<T>>());
        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}