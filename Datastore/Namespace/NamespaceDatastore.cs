using System.Threading.Tasks;
using Datastore.KeyTransform;
using Datastore.Query;
using NChannels;

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

            var ch = new Chan<DatastoreResult<T>>();

            Task.Factory.StartNew(async () =>
                {
                    var l = 0;
                    ChanResult<DatastoreResult<T>> e;
                    while ((e = await qr.Next().Receive()).IsSuccess)
                    {
                        if (e.Result.Error != null)
                        {
                            await ch.Send(e.Result);
                            continue;
                        }

                        var k = new DatastoreKey(e.Result.DatastoreKey);
                        if (!_prefix.IsAncestorOf(k))

                            await ch.Send(new DatastoreResult<T>(InvertKey(k), e.Result.Value));
                    }
                })
                .ContinueWith(_ => ch.Close());

            return qr.DerivedResults(ch);
        }
    }
}