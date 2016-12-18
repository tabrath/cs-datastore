using System.Collections.Generic;
using System.Linq;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore
{
    public class MapDatastore<T> : IBatchingDatastore<T>
    {
        private readonly Dictionary<DatastoreKey, T> _values;

        public MapDatastore()
        {
            _values = new Dictionary<DatastoreKey, T>();
        }

        public void Put(DatastoreKey datastoreKey, T value) => _values[datastoreKey] = value;
        public T Get(DatastoreKey datastoreKey) => _values[datastoreKey];
        public bool Has(DatastoreKey datastoreKey) => _values.ContainsKey(datastoreKey);
        public void Delete(DatastoreKey datastoreKey) => _values.Remove(datastoreKey);

        public DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            var re = _values
                .Select(kv => new DatastoreEntry<T>(kv.Key, kv.Value))
                .ToArray();

            return DatastoreResults<T>
                .WithEntries(q, re)
                .NaiveQueryApply();
        }

        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
        public IDatastoreBatch<T> Batch() => new BasicDatastoreBatch<T>(this);
        public void Dispose() => _values.Clear();
    }
}