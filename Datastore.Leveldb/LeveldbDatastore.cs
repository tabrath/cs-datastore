using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Extensions;
using Datastore.Query;
using Datastore.Sync;
using LevelDB;

namespace Datastore.Leveldb
{
    public class LeveldbDatastore : IBatchingDatastore<byte[]>, IThreadSafeDatastore<byte[]>
    {
        private readonly DB _db;

        public LeveldbDatastore(string path, Options options = null)
        {
            _db = DB.Open(path, options ?? new Options { CreateIfMissing = true });
        }

        public IDatastoreBatch<byte[]> Batch() => new LeveldbBatch(_db);

        public void Dispose()
        {
            _db.Dispose();
        }

        public void Put(DatastoreKey datastoreKey, byte[] value)
        {
            _db.Put(WriteOptions.Default, datastoreKey.ToBytes(), value);
        }

        public byte[] Get(DatastoreKey datastoreKey)
        {
            return _db.Get(ReadOptions.Default, datastoreKey.ToBytes()).ToArray();
        }

        public bool Has(DatastoreKey datastoreKey)
        {
            Slice v;
            return _db.TryGet(ReadOptions.Default, datastoreKey.ToBytes(), out v);
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            if (!Has(datastoreKey))
                throw new KeyNotFoundException();

            _db.Delete(WriteOptions.Default, datastoreKey.ToBytes());
        }

        public DatastoreResults<byte[]> Query(DatastoreQuery<byte[]> q)
        {
            var qrb = new DatastoreResults<byte[]>.ResultBuilder(q);

            Task.Run(() => RunQuery(qrb))
                .ContinueWith(_ => qrb.Output.CompleteAdding());

            var qr = qrb.Results();
            foreach (var filter in q.QueryFilters)
            {
                qr = qr.NaiveFilter(filter);
            }

            foreach (var order in q.QueryOrders)
            {
                qr = qr.NaiveOrder(order);
            }

            return qr;
        }

        private void RunQuery(DatastoreResults<byte[]>.ResultBuilder qrb)
        {
            byte[] range = null;
            if (!string.IsNullOrEmpty(qrb.DatastoreQuery.Prefix))
                range = Encoding.UTF8.GetBytes(qrb.DatastoreQuery.Prefix);

            using (var i = _db.NewIterator(ReadOptions.Default))
            {
                i.SeekToFirst();

                if (qrb.DatastoreQuery.Offset > 0)
                {
                    var offset = 0;
                    while (offset < qrb.DatastoreQuery.Offset)
                    {
                        if (range != null)
                        {
                            if (i.Key().ToArray().HasPrefix(range))
                                offset++;
                        }
                        else
                        {
                            offset++;
                        }

                        i.Next();
                    }
                }

                for (var sent = 0; i.Valid(); sent++)
                {
                    if (qrb.DatastoreQuery.Limit > 0 && sent >= qrb.DatastoreQuery.Limit)
                        break;

                    if (qrb.Cancellation.IsCancellationRequested)
                        break;

                    if (range != null && i.Key().ToArray().HasPrefix(range))
                    {
                        var k = new DatastoreKey(i.Key().ToString());
                        var v = qrb.DatastoreQuery.KeysOnly ? null : i.Value().ToArray();
                        var e = new DatastoreEntry<byte[]>(k, v);

                        if (!qrb.Output.TryAdd(new DatastoreResult<byte[]>(e), Timeout.Infinite, qrb.Cancellation.Token))
                            break;
                    }

                    i.Next();
                }
            }
        }

        public IThreadSafeDatastore<byte[]> Synchronized() => this;
        public bool IsThreadSafe => true;

        private class LeveldbBatch : IDatastoreBatch<byte[]>
        {
            private readonly DB _db;
            private readonly WriteBatch _batch;

            public LeveldbBatch(DB db)
            {
                _db = db;
                _batch = new WriteBatch();
            }

            public void Put(DatastoreKey datastoreKey, byte[] value)
            {
                _batch.Put(datastoreKey.ToBytes(), value);
            }

            public void Delete(DatastoreKey datastoreKey)
            {
                _batch.Delete(datastoreKey.ToBytes());
            }

            public void Commit()
            {
                _db.Write(WriteOptions.Default, _batch);
            }
        }
    }
}
