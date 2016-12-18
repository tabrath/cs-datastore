using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Datastore.AutoBatch.Tests
{
    [TestFixture]
    public class AutoBatchDatastoreTests
    {
        [Test]
        public void BasicPuts()
        {
            var d = new AutoBatchDatastore<string>(new MapDatastore<string>(), 16);
            var key = new DatastoreKey("test");
            var value = "hello world";

            d.Put(key, value);
            var result = d.Get(key);

            Assert.That(result, Is.EqualTo(value));
        }

        [Test]
        public void Flushing()
        {
            var child = new MapDatastore<string>();
            var d = new AutoBatchDatastore<string>(child, 16);

            var keys = Enumerable.Range(0, 16).Select(i => new DatastoreKey($"test{i}")).ToArray();
            var value = "hello world";

            foreach (var key in keys)
            {
                d.Put(key, value);
            }

            Assert.Throws<KeyNotFoundException>(() => child.Get(keys[0]));

            d.Put(new DatastoreKey("test16"), value);

            foreach (var key in keys)
            {
                var v = child.Get(key);
                Assert.That(v, Is.EqualTo(value));
            }
        }
    }
}
