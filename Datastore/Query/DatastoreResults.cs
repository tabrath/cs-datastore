using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Datastore.Query
{
    public class DatastoreResults<T>
    {
        public DatastoreQuery<T> DatastoreQuery { get; }
        public CancellationTokenSource Cancellation { get; }

        private readonly BlockingCollection<DatastoreResult<T>> _res;

        internal DatastoreResults(DatastoreQuery<T> datastoreQuery, CancellationTokenSource cancellation, BlockingCollection<DatastoreResult<T>> res)
        {
            DatastoreQuery = datastoreQuery;
            Cancellation = cancellation;
            _res = res;
        }

        public void Close()
        {
            Cancellation.Dispose();
        }

        public BlockingCollection<DatastoreResult<T>> Next() => _res;

        public DatastoreEntry<T>[] Rest()
        {
            var es = new List<DatastoreEntry<T>>();

            while (!_res.IsCompleted && !Cancellation.IsCancellationRequested)
            {
                DatastoreResult<T> e;
                if (!_res.TryTake(out e, Timeout.Infinite, Cancellation.Token))
                    break;

                es.Add(e);
            }

            return es.ToArray();
        }

        public class ResultBuilder
        {
            public DatastoreQuery<T> DatastoreQuery { get; }
            public BlockingCollection<DatastoreResult<T>> Output { get; }
            public CancellationTokenSource Cancellation { get; }

            public ResultBuilder(DatastoreQuery<T> q)
            {
                var bufSize = 1;
                if (q.KeysOnly)
                    bufSize = DatastoreQuery<T>.KeysOnlyBufferSize;

                DatastoreQuery = q;
                Output = new BlockingCollection<DatastoreResult<T>>(bufSize);
                Cancellation = new CancellationTokenSource();
                Cancellation.Token.Register(() => Output.CompleteAdding());
            }

            public DatastoreResults<T> Results() => new DatastoreResults<T>(DatastoreQuery, Cancellation, Output);
        }

        public static DatastoreResults<T> WithCollection(DatastoreQuery<T> q, BlockingCollection<DatastoreResult<T>> res)
        {
            var b = new ResultBuilder(q);

            Task.Factory.StartNew(() =>
            {
                DatastoreResult<T> item;
                while (res.TryTake(out item, Timeout.Infinite, b.Cancellation.Token))
                {
                    b.Output.Add(item, b.Cancellation.Token);
                }
            }).ContinueWith(_ => b.Output.CompleteAdding());

            return b.Results();
        }

        public static DatastoreResults<T> WithEntries(DatastoreQuery<T> q, DatastoreEntry<T>[] res)
        {
            var b = new ResultBuilder(q);

            Task.Factory.StartNew(() =>
            {
                foreach (var r in res.Select(e => new DatastoreResult<T>(e)))
                {
                    b.Output.Add(r, b.Cancellation.Token);
                }
            }).ContinueWith(_ => b.Output.CompleteAdding());

            return b.Results();
        }

        public static DatastoreResults<T> ReplaceQuery(DatastoreResults<T> r, DatastoreQuery<T> q) => new DatastoreResults<T>(q, r.Cancellation, r.Next());
        public DatastoreResults<T> DerivedResults(BlockingCollection<DatastoreResult<T>> ch) => new DatastoreResults<T>(DatastoreQuery, Cancellation, ch);

        public DatastoreResults<T> NaiveFilter(QueryFilter<T> queryFilter)
        {
            var bc = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
            {
                while (!_res.IsCompleted && !Cancellation.IsCancellationRequested)
                {
                    DatastoreResult<T> item;
                    if (_res.TryTake(out item, Timeout.Infinite, Cancellation.Token) && item.Error == null &&
                        queryFilter.Apply(item))
                    {
                        if (!bc.TryAdd(item, Timeout.Infinite, Cancellation.Token))
                            break;
                    }
                }
            }).ContinueWith(_ => bc.CompleteAdding());

            return DerivedResults(bc);
        }

        public DatastoreResults<T> NaiveLimit(int limit)
        {
            var bc = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
            {
                var l = 0;
                while (!_res.IsCompleted && !Cancellation.IsCancellationRequested)
                {
                    DatastoreResult<T> item;
                    if (!_res.TryTake(out item, Timeout.Infinite, Cancellation.Token))
                        break;

                    if (item.Error != null)
                    {
                        if (!bc.TryAdd(item, Timeout.Infinite, Cancellation.Token))
                            break;

                        continue;
                    }

                    if (!bc.TryAdd(item, Timeout.Infinite, Cancellation.Token))
                        break;

                    if (limit > 0 && ++l >= limit)
                        break;
                }
            }).ContinueWith(_ => bc.CompleteAdding());

            return DerivedResults(bc);
        }

        public DatastoreResults<T> NaiveOffset(int offset)
        {
            var bc = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
            {
                var sent = 0;
                while (!_res.IsCompleted && !Cancellation.IsCancellationRequested)
                {
                    DatastoreResult<T> item;
                    if (!_res.TryTake(out item, Timeout.Infinite, Cancellation.Token))
                        break;

                    if (item.Error != null)
                    {
                        if (!bc.TryAdd(item, Timeout.Infinite, Cancellation.Token))
                            break;

                        continue;
                    }

                    if (++sent <= offset)
                        continue;

                    if (!bc.TryAdd(item, Timeout.Infinite, Cancellation.Token))
                        break;
                }
            }).ContinueWith(_ => bc.CompleteAdding());

            return DerivedResults(bc);
        }

        public DatastoreResults<T> NaiveOrder(QueryOrder<T> o)
        {
            var bc = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
            {
                var entries = new List<DatastoreEntry<T>>();
                while (!_res.IsCompleted && !Cancellation.IsCancellationRequested)
                {
                    DatastoreResult<T> item;
                    if (!_res.TryTake(out item, Timeout.Infinite, Cancellation.Token))
                        break;

                    if (item.Error != null)
                    {
                        if (!bc.TryAdd(item, Timeout.Infinite, Cancellation.Token))
                            break;

                        continue;
                    }

                    entries.Add(item);
                }
                return entries;
            })
            .ContinueWith(t =>
                {
                    o.Sort(t.Result);
                    foreach (var entry in t.Result)
                    {
                        if (Cancellation.IsCancellationRequested)
                            break;

                        if (!bc.TryAdd(new DatastoreResult<T>(entry), Timeout.Infinite, Cancellation.Token))
                            break;
                    }
                })
            .ContinueWith(_ => bc.CompleteAdding());

            return DerivedResults(bc);
        }

        public DatastoreResults<T> NaiveQueryApply()
        {
            DatastoreResults<T> qr = this;

            if (!string.IsNullOrEmpty(DatastoreQuery.Prefix))
                qr = qr.NaiveFilter(QueryFilter<T>.ByKeyPrefix(DatastoreQuery.Prefix));

            foreach (var filter in DatastoreQuery.QueryFilters)
            {
                qr = qr.NaiveFilter(filter);
            }

            foreach (var order in DatastoreQuery.QueryOrders)
            {
                qr = qr.NaiveOrder(order);
            }

            if (DatastoreQuery.Offset != 0)
                qr = qr.NaiveOffset(DatastoreQuery.Offset);

            if (DatastoreQuery.Limit != 0)
                qr = qr.NaiveLimit(DatastoreQuery.Limit);

            return qr;
        }

        public static DatastoreEntry<T>[] EntriesFrom(IEnumerable<KeyValuePair<DatastoreKey, T>> pairs) => pairs.Select(kv => new DatastoreEntry<T>(kv.Key, kv.Value)).ToArray();
    }
}
