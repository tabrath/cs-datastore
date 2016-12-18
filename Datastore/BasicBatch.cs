using System.Collections.Generic;

namespace Datastore
{
    public class BasicDatastoreBatch<T> : IDatastoreBatch<T>
    {
        private readonly IDatastore<T> _ds;
        private readonly Dictionary<DatastoreKey, T> _puts;
        private readonly List<DatastoreKey> _deletes;

        public BasicDatastoreBatch(IDatastore<T> ds)
        {
            _ds = ds;
            _puts = new Dictionary<DatastoreKey, T>();
            _deletes = new List<DatastoreKey>();
        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            _puts.Add(datastoreKey, value);
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            _deletes.Add(datastoreKey);
        }

        public void Commit()
        {
            foreach (var p in _puts)
            {
                _ds.Put(p.Key, p.Value);
            }

            _deletes.ForEach(_ds.Delete);
        }
    }
}