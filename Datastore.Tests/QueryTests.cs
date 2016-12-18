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
    public class QueryTests
    {
        private static void TestResults<T>(DatastoreResults<T> datastoreResults, DatastoreKey[] expected)
        {
            var actual = datastoreResults
                .Rest()
                .Select(e => e.DatastoreKey)
                .ToArray();

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static void TestKeyLimit<T>(int limit, DatastoreKey[] datastoreKeys, DatastoreKey[] expected)
        {
            var entries = datastoreKeys
                .Select(e => new DatastoreEntry<T>(e, default(T)))
                .ToArray();

            var res = DatastoreResults<T>.WithEntries(new DatastoreQuery<T>(), entries)
                .NaiveLimit(limit);

            TestResults(res, expected);
        }

        [Test]
        public void TestLimit()
        {
            TestKeyLimit<object>(0, FilterTests.SampleDatastoreKeys, new[]
            {
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd"),
                new DatastoreKey("/a"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/abcf"),
                new DatastoreKey("/ab"),
            });
            TestKeyLimit<object>(10, FilterTests.SampleDatastoreKeys, new[]
{
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd"),
                new DatastoreKey("/a"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/abcf"),
                new DatastoreKey("/ab"),
            });
            TestKeyLimit<object>(2, FilterTests.SampleDatastoreKeys, new[]
{
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd"),
            });
        }

        private static void TestKeyOffset<T>(int offset, DatastoreKey[] datastoreKeys, DatastoreKey[] expected)
        {
            var entries = datastoreKeys
                .Select(e => new DatastoreEntry<T>(e, default(T)))
                .ToArray();

            var res = DatastoreResults<T>.WithEntries(new DatastoreQuery<T>(), entries)
                .NaiveOffset(offset);

            TestResults(res, expected);
        }

        [Test]
        public void TestOffset()
        {
            TestKeyOffset<object>(0, FilterTests.SampleDatastoreKeys, new[]
            {
                new DatastoreKey("/ab/c"),
                new DatastoreKey("/ab/cd"),
                new DatastoreKey("/a"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/abcf"),
                new DatastoreKey("/ab"),
            });
            TestKeyOffset<object>(10, FilterTests.SampleDatastoreKeys, Array.Empty<DatastoreKey>());
            TestKeyOffset<object>(2, FilterTests.SampleDatastoreKeys, new[]
{
                new DatastoreKey("/a"),
                new DatastoreKey("/abce"),
                new DatastoreKey("/abcf"),
                new DatastoreKey("/ab"),
            });
        }
    }
}
