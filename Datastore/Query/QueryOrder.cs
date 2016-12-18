using System;
using System.Collections.Generic;

namespace Datastore.Query
{
    public abstract class QueryOrder<T>
    {
        public abstract void Sort(List<DatastoreEntry<T>> entries);

        public static QueryOrder<T> ByValueAscending(Comparison<T> comparison) => new QueryOrderByValueAscending<T>(comparison);
        public static QueryOrder<T> ByValueDescending(Comparison<T> comparison) => new QueryOrderByValueDescending<T>(comparison);
        public static QueryOrder<T> ByKeyAscending() => new QueryOrderByKeyAscending<T>();
        public static QueryOrder<T> ByKeyDescending() => new QueryOrderByKeyDescending<T>();
    }

    public abstract class QueryOrderByValue<T> : QueryOrder<T>
    {
        private readonly Comparison<T> _comparison;
        private readonly bool _descending;

        protected QueryOrderByValue(Comparison<T> comparison, bool descending)
        {
            _comparison = comparison;
            _descending = @descending;
        }

        public override void Sort(List<DatastoreEntry<T>> entries)
        {
            entries.Sort((x, y) => _comparison(x.Value, y.Value));
            if (_descending)
                entries.Reverse();
        }
    }

    public class QueryOrderByValueAscending<T> : QueryOrderByValue<T>
    {
        public QueryOrderByValueAscending(Comparison<T> comparison)
            : base(comparison, false)
        {
        }
    }

    public class QueryOrderByValueDescending<T> : QueryOrderByValue<T>
    {
        public QueryOrderByValueDescending(Comparison<T> comparison)
            : base(comparison, true)
        {
        }
    }

    public abstract class QueryOrderByKey<T> : QueryOrder<T>
    {
        private readonly bool _descending;

        protected QueryOrderByKey(bool descending)
        {
            _descending = @descending;
        }

        public override void Sort(List<DatastoreEntry<T>> entries)
        {
            entries.Sort((x, y) => x.DatastoreKey.CompareTo(y.DatastoreKey));
            if (_descending)
                entries.Reverse();
        }
    }

    public class QueryOrderByKeyAscending<T> : QueryOrderByKey<T>
    {
        public QueryOrderByKeyAscending()
            : base(false)
        {
        }
    }

    public class QueryOrderByKeyDescending<T> : QueryOrderByKey<T>
    {
        public QueryOrderByKeyDescending()
            : base(true)
        {
        }
    }
}
