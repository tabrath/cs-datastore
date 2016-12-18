using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Datastore.Query;
using NUnit.Framework;

namespace Datastore.Tests
{
    [TestFixture]
    public class OrderTests
    {
        private static void TestKeyOrder<T>(QueryOrder<T> o, DatastoreKey[] datastoreKeys, DatastoreKey[] expected)
        {
            var e = datastoreKeys.Select(key => new DatastoreEntry<T>(key, default(T))).ToArray();
            var res = DatastoreResults<T>.WithEntries(new DatastoreQuery<T>(), e).NaiveOrder(o);
            var actualE = res.Rest();
            var actual = actualE.Select(x => x.DatastoreKey).ToArray();

            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void TestOrderByKey()
        {
            TestKeyOrder(QueryOrder<object>.ByKeyAscending(), FilterTests.SampleDatastoreKeys, new[]
            {
                new DatastoreKey("/a"),
                new DatastoreKey("/ab"),
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/abcf")
            });
            TestKeyOrder(QueryOrder<object>.ByKeyDescending(), FilterTests.SampleDatastoreKeys, new[]
            {
                new DatastoreKey("/abcf"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/ab/cd"),
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab"),
                new DatastoreKey("/a")
            });
        }
    }
}
