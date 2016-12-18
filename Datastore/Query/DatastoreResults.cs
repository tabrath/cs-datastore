using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NChannels;

namespace Datastore.Query
{
    public class DatastoreResults<T>
    {
        public DatastoreQuery<T> DatastoreQuery { get; }
        public CancellationTokenSource Cancellation { get; }

        private readonly Chan<DatastoreResult<T>> _res;

        internal DatastoreResults(DatastoreQuery<T> datastoreQuery, CancellationTokenSource cancellation, Chan<DatastoreResult<T>> res)
        {
            DatastoreQuery = datastoreQuery;
            Cancellation = cancellation;
            _res = res;
        }

        public void Close()
        {
            Cancellation.Dispose();
        }

        public Chan<DatastoreResult<T>> Next() => _res;

        public DatastoreEntry<T>[] Rest()
        {
            var es = new List<DatastoreEntry<T>>();
            _res.Where(e => e.Error == null)
                .ForEach(e => es.Add(e))
                .Wait();
            return es.ToArray();
        }

        public class ResultBuilder
        {
            public DatastoreQuery<T> DatastoreQuery { get; }
            public Chan<DatastoreResult<T>> Output { get; }
            public CancellationTokenSource Cancellation { get; }

            public ResultBuilder(DatastoreQuery<T> q)
            {
                var bufSize = 1;
                if (q.KeysOnly)
                    bufSize = DatastoreQuery<T>.KeysOnlyBufferSize;

                DatastoreQuery = q;
                Output = new Chan<DatastoreResult<T>>(bufSize);
                Cancellation = new CancellationTokenSource();
                Cancellation.Token.Register(() => Output.Close());
            }

            public DatastoreResults<T> Results() => new DatastoreResults<T>(DatastoreQuery, Cancellation, Output);
        }

        public static DatastoreResults<T> WithChannel(DatastoreQuery<T> q, Chan<DatastoreResult<T>> res)
        {
            var b = new ResultBuilder(q);

            res.Forward(b.Output)
                .ContinueWith(_ => b.Output.Close());

            return b.Results();
        }

        public static DatastoreResults<T> WithEntries(DatastoreQuery<T> q, DatastoreEntry<T>[] res)
        {
            var b = new ResultBuilder(q);

            b.Output
                .Send(res.Select(e => new DatastoreResult<T>(e)))
                .ContinueWith(_ => b.Output.Close());

            return b.Results();
        }

        public static DatastoreResults<T> ReplaceQuery(DatastoreResults<T> r, DatastoreQuery<T> q) => new DatastoreResults<T>(q, r.Cancellation, r.Next());
        public DatastoreResults<T> DerivedResults(Chan<DatastoreResult<T>> ch) => new DatastoreResults<T>(DatastoreQuery, Cancellation, ch);

        public DatastoreResults<T> NaiveFilter(QueryFilter<T> queryFilter)
        {
            var ch = new Chan<DatastoreResult<T>>();

            Next()
                .Where(e => e.Error != null || queryFilter.Apply(e))
                .Forward(ch)
                .ContinueWith(_ =>
                {
                    ch.Close();
                    Close();
                });

            return DerivedResults(ch);
        }

        public DatastoreResults<T> NaiveLimit(int limit)
        {
            var ch = new Chan<DatastoreResult<T>>();

            Task.Factory.StartNew(async () =>
            {
                var l = 0;
                ChanResult<DatastoreResult<T>> e;
                while ((e = await Next().Receive()).IsSuccess)
                {
                    if (e.Result.Error != null)
                    {
                        await ch.Send(e.Result);
                        continue;
                    }

                    await ch.Send(e.Result);
                    l++;
                    if (limit > 0 && l >= limit)
                        break;
                }
            })
            .ContinueWith(_ => ch.Close());

            return DerivedResults(ch);
        }

        public DatastoreResults<T> NaiveOffset(int offset)
        {
            var ch = new Chan<DatastoreResult<T>>();

            Task.Factory.StartNew(async () =>
            {
                var sent = 0;
                ChanResult<DatastoreResult<T>> e;
                while ((e = await Next().Receive()).IsSuccess)
                {
                    if (e.Result.Error != null)
                    {
                        await ch.Send(e.Result);
                        continue;
                    }

                    if (sent < offset)
                    {
                        sent++;
                        continue;
                    }
                    await ch.Send(e.Result);
                }
            })
            .ContinueWith(_ => ch.Close());

            return DerivedResults(ch);
        }

        public DatastoreResults<T> NaiveOrder(QueryOrder<T> o)
        {
            var ch = new Chan<DatastoreResult<T>>();

            var entries = new List<DatastoreEntry<T>>();
            Next().ForEach(async e =>
                {
                    if (e.Error != null)
                        await ch.Send(e);
                    else
                    {
                        lock (entries)
                        {
                            entries.Add(e);
                        }
                    }
                })
                .ContinueWith(_ =>
                {
                    o.Sort(entries);
                    ch.Send(entries.Select(x => new DatastoreResult<T>(x)))
                        .ContinueWith(__ => ch.Close());
                });

            return DerivedResults(ch);
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
