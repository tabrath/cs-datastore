using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.AutoBatch
{
    public class AutoBatchDatastore<T> : IDatastore<T>
    {
        private readonly IBatchingDatastore<T> _datastore;
        private readonly int _maxBufferEntries;
        private readonly Dictionary<DatastoreKey, T> _buffer;

        public AutoBatchDatastore(IBatchingDatastore<T> datastore, int maxBufferEntries)
        {
            _datastore = datastore;
            _maxBufferEntries = maxBufferEntries;
            _buffer = new Dictionary<DatastoreKey, T>(_maxBufferEntries);
        }

        public void Dispose()
        {
            _buffer.Clear();
        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            _buffer[datastoreKey] = value;
            if (_buffer.Count > _maxBufferEntries)
                Flush();
        }

        private void Flush()
        {
            var batch = _datastore.Batch();

            foreach (var kv in _buffer)
            {
                batch.Put(kv.Key, kv.Value);
            }

            _buffer.Clear();

            batch.Commit();
        }

        public T Get(DatastoreKey datastoreKey)
        {
            T value;
            return _buffer.TryGetValue(datastoreKey, out value) ? value : _datastore.Get(datastoreKey);
        }

        public bool Has(DatastoreKey datastoreKey)
        {
            if (_buffer.ContainsKey(datastoreKey))
                return true;

            return _datastore.Has(datastoreKey);
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            if (_buffer.ContainsKey(datastoreKey))
                _buffer.Remove(datastoreKey);

            _datastore.Delete(datastoreKey);
        }

        public DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            Flush();

            return _datastore.Query(q);
        }

        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}
