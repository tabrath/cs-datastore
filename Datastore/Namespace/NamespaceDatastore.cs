using System.Collections.Concurrent;
using System.Threading.Tasks;
using Datastore.KeyTransform;
using Datastore.Query;

namespace Datastore.Namespace
{
    public class NamespaceDatastore<T> : KeyTransformDatastore<T>
    {
        private readonly DatastoreKey _prefix;

        public NamespaceDatastore(IDatastore<T> child, DatastoreKey prefix)
            : base(child, new PrefixKeyTransform(prefix))
        {
            _prefix = prefix;
        }

        public override DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            var qr = _child.Query(q);

            var ch = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
                {
                    var l = 0;
                    DatastoreResult<T> e;
                    while (qr.Next().TryTake(out e))
                    {
                        if (e.Error != null)
                        {
                            ch.Add(e);
                            continue;
                        }

                        var k = new DatastoreKey(e.DatastoreKey);
                        if (_prefix.IsAncestorOf(k))
                        {
                            if (!ch.TryAdd(new DatastoreResult<T>(InvertKey(k), e.Value)))
                                break;
                        }
                    }
                })
                .ContinueWith(_ => ch.CompleteAdding());

            return qr.DerivedResults(ch);
        }
    }
}