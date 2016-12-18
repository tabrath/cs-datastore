using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.Tiered
{
    public class TieredDatastore<T> : IDatastore<T>
    {
        private readonly IDatastore<T>[] _datastores;

        public TieredDatastore(params IDatastore<T>[] datastores)
        {
            _datastores = datastores;
        }

        public void Dispose()
        {
        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            Task.WaitAll(_datastores
                .Select(ds => Task.Factory.StartNew(() => ds.Put(datastoreKey, value)))
                .ToArray());
        }

        public T Get(DatastoreKey datastoreKey)
        {
            foreach (var ds in _datastores)
            {
                var value = ds.Get(datastoreKey);
                if (value != null)
                    return value;
            }

            throw new KeyNotFoundException();
        }

        public bool Has(DatastoreKey datastoreKey) => _datastores.Any(ds => ds.Has(datastoreKey));

        public void Delete(DatastoreKey datastoreKey)
        {
            Task.WaitAll(_datastores
                .Select(ds => Task.Factory.StartNew(() => ds.Delete(datastoreKey)))
                .ToArray());
        }

        public DatastoreResults<T> Query(DatastoreQuery<T> q) => _datastores.Last().Query(q);
        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}
