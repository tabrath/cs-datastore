using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Datastore.Filesystem;
using Datastore.Query;
using NUnit.Framework;

namespace Datastore.Tests
{
    [TestFixture]
    public class FilesystemDatastoreTests
    {
        [Test]
        public void TestOpen()
        {
            Assert.DoesNotThrow(() => new FilesystemDatastore<object>(TestContext.CurrentContext.TestDirectory));
        }

        [Test]
        public void TestBasic()
        {
            var keys = new[]
            {
                "foo",
                "foo/bar",
                "foo/bar/baz",
                "foo/barb",
                "foo/bar/bazb",
                "foo/bar/baz/barb"
            }.Select(k => new DatastoreKey(k)).ToArray();

            var ds = new FilesystemDatastore<object>(TestContext.CurrentContext.WorkDirectory);

            foreach (var key in keys)
            {
                ds.Put(key, key.ToString());
            }

            foreach (var key in keys)
            {
                var value = ds.Get(key);
                Assert.That(value, Is.EqualTo(key.ToString()));
            }

            var r = ds.Query(new DatastoreQuery<object>(prefix: "/foo/bar/"));
                
            var expected = new[]
                {
                    "/foo/bar/baz",
                    "/foo/bar/bazb",
                    "/foo/bar/baz/barb"
                }.Select(k => new DatastoreKey(k)).ToArray();

            var all = r.Rest();

            Assert.That(all.Length, Is.EqualTo(expected.Length));

            foreach (var k in expected)
            {
                Assert.That(all.Any(kv => kv.DatastoreKey.Equals(k)), Is.True);
            }
        }
    }
}
