using System;

namespace Datastore.Query
{
    public class DatastoreQuery<T>
    {
        public const int KeysOnlyBufferSize = 128;

        public string Prefix { get; }
        public QueryFilter<T>[] QueryFilters { get; }
        public QueryOrder<T>[] QueryOrders { get; }
        public int Limit { get; }
        public int Offset { get; }
        public bool KeysOnly { get; }

        public DatastoreQuery(string prefix = null, int limit = 0, int offset = 0, bool keysOnly = false,
            QueryFilter<T>[] queryFilters = null, QueryOrder<T>[] queryOrders = null)
        {
            Prefix = prefix;
            Limit = limit;
            Offset = offset;
            KeysOnly = keysOnly;
            QueryFilters = queryFilters ?? Array.Empty<QueryFilter<T>>();
            QueryOrders = queryOrders ?? Array.Empty<QueryOrder<T>>();
        }
    }
}