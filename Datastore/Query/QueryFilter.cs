using System;

namespace Datastore.Query
{
    public abstract class QueryFilter<T>
    {
        public abstract bool Apply(DatastoreEntry<T> e);

        public static QueryFilter<T> ByValue(Operator @operator, T value) => new QueryFilterValueCompare<T>(@operator, value);
        public static QueryFilter<T> ByKey(Operator @operator, DatastoreKey datastoreKey) => new QueryFilterKeyCompare<T>(@operator, datastoreKey);
        public static QueryFilter<T> ByKeyPrefix(string prefix) => new QueryFilterKeyPrefix<T>(prefix);
    }

    public enum Operator
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreatherThanOrEqual,
        LessThan,
        LessThanOrEqual
    }

    public class QueryFilterValueCompare<T> : QueryFilter<T>
    {
        public Operator Operator { get; }
        public T Value { get; }

        public QueryFilterValueCompare(Operator @operator, T value)
        {
            Operator = @operator;
            Value = value;
        }

        public override bool Apply(DatastoreEntry<T> e)
        {
            switch (Operator)
            {
                case Operator.Equal:
                    return Value.Equals(e.Value);
                case Operator.NotEqual:
                    return !Value.Equals(e.Value);
                default:
                    throw new Exception($"cannot apply operator '{Operator}' to '{typeof(T)}'");
            }
        }
    }

    public class QueryFilterKeyCompare<T> : QueryFilter<T>
    {
        public Operator Operator { get; }
        public DatastoreKey DatastoreKey { get; }

        public QueryFilterKeyCompare(Operator @operator, DatastoreKey datastoreKey)
        {
            Operator = @operator;
            DatastoreKey = datastoreKey;
        }

        public override bool Apply(DatastoreEntry<T> e)
        {
            switch (Operator)
            {
                case Operator.Equal:
                    return e.DatastoreKey.Equals(DatastoreKey);
                case Operator.NotEqual:
                    return !e.DatastoreKey.Equals(DatastoreKey);
                case Operator.GreaterThan:
                    return e.DatastoreKey > DatastoreKey;
                case Operator.GreatherThanOrEqual:
                    return e.DatastoreKey >= DatastoreKey;
                case Operator.LessThan:
                    return e.DatastoreKey < DatastoreKey;
                case Operator.LessThanOrEqual:
                    return e.DatastoreKey <= DatastoreKey;
                default:
                    throw new Exception($"unknown operator '{Operator}'");
            }
        }
    }

    public class QueryFilterKeyPrefix<T> : QueryFilter<T>
    {
        public string Prefix { get; }

        public QueryFilterKeyPrefix(string prefix)
        {
            Prefix = prefix;
        }

        public override bool Apply(DatastoreEntry<T> e) => e.DatastoreKey.ToString().StartsWith(Prefix);
    }
}
