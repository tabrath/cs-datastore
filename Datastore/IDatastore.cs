using System;
using Datastore.Query;

namespace Datastore
{
    public interface IDatastore<T> : IDisposable
    {
        void Put(DatastoreKey datastoreKey, T value);
        T Get(DatastoreKey datastoreKey);
        bool Has(DatastoreKey datastoreKey);
        void Delete(DatastoreKey datastoreKey);
        DatastoreResults<T> Query(DatastoreQuery<T> q);
        IThreadSafeDatastore<T> Synchronized();
    }
}
