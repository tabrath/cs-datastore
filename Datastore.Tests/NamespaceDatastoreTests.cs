using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datastore.Namespace;
using Datastore.Query;
using NUnit.Framework;

namespace Datastore.Tests
{
    [TestFixture]
    public class NamespaceDatastoreTests
    {
        [TestCase("abc")]
        [TestCase("")]
        public void TestBasic(string prefix)
        {
            var mpds = new MapDatastore<byte[]>();
            var nsds = new NamespaceDatastore<byte[]>(mpds, new DatastoreKey(prefix));
            var keys = new[]
            {
                "foo",
                "foo/bar",
                "foo/bar/baz",
                "foo/barb",
                "foo/bar/bazb",
                "foo/bar/baz/barb",
            }.Select(s => new DatastoreKey(s)).ToArray();

            foreach (var key in keys)
            {
                nsds.Put(key, Encoding.UTF8.GetBytes(key.ToString()));
            }

            foreach (var key in keys)
            {
                var v1 = nsds.Get(key);
                Assert.That(v1, Is.EqualTo(Encoding.UTF8.GetBytes(key.ToString())));

                var v2 = mpds.Get(new DatastoreKey(prefix).Child(key));
                Assert.That(v2, Is.EqualTo(Encoding.UTF8.GetBytes(key.ToString())));
            }

            Func<IDatastore<byte[]>, DatastoreQuery<byte[]>, List<DatastoreKey>> run = (d, q) =>
            {
                return d.Query(q).Rest().Select(e => e.DatastoreKey).ToList();
            };

            var listA = run(mpds, new DatastoreQuery<byte[]>());
            var listB = run(nsds, new DatastoreQuery<byte[]>());
            Assert.That(listA.Count, Is.EqualTo(listB.Count));

            listA.Sort();
            listB.Sort();

            for (var i = 0; i < listA.Count; i++)
            {
                var kA = listA[i];
                var kB = listB[i];

                Assert.That(nsds.InvertKey(kA), Is.EqualTo(kB));
                Assert.That(kA, Is.EqualTo(nsds.ConvertKey(kB)));
            }
        }
    }
}
