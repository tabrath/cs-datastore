using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datastore.Query;
using NUnit.Framework;

namespace Datastore.Tests
{
    [TestFixture]
    public class FilterTests
    {
        public static readonly DatastoreKey[] SampleDatastoreKeys = new[]
        {
            new DatastoreKey("/ab/c"),
            new DatastoreKey("/ab/cd"),
            new DatastoreKey("/a"), 
            new DatastoreKey("/abce"),
            new DatastoreKey("/abcf"),
            new DatastoreKey("/ab")
        };

        private static void TestKeyFilter<T>(QueryFilter<T> queryFilter, DatastoreKey[] datastoreKeys, DatastoreKey[] expected)
        {
            var e = datastoreKeys.Select(key => new DatastoreEntry<T>(key, default(T))).ToArray();
            var res = DatastoreResults<T>.WithEntries(new DatastoreQuery<T>(), e).NaiveFilter(queryFilter);
            var actualE = res.Rest();
            var actual = actualE.Select(x => x.DatastoreKey).ToArray();

            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void TestFilterKeyCompare()
        {
            TestKeyFilter(QueryFilter<object>.ByKey(Operator.Equal, new DatastoreKey("/ab")), SampleDatastoreKeys, new [] { new DatastoreKey("/ab") });
            TestKeyFilter(QueryFilter<object>.ByKey(Operator.GreaterThan, new DatastoreKey("/ab")), SampleDatastoreKeys, new [] { new DatastoreKey("/ab/c"), new DatastoreKey("/ab/cd"), new DatastoreKey("/abce"), new DatastoreKey("/abcf") });
            TestKeyFilter(QueryFilter<object>.ByKey(Operator.LessThanOrEqual, new DatastoreKey("/ab")), SampleDatastoreKeys, new [] { new DatastoreKey("/a"), new DatastoreKey("/ab") });
        }

        [Test]
        public void TesetFilterKeyPrefix()
        {
            TestKeyFilter(QueryFilter<object>.ByKeyPrefix("/a"), SampleDatastoreKeys, new[]
            {
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd"),
                new DatastoreKey("/a"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/abcf"),
                new DatastoreKey("/ab")
            });
            TestKeyFilter(QueryFilter<object>.ByKeyPrefix("/ab/"), SampleDatastoreKeys, new[]
            {
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd")
            });
        }
    }
}
