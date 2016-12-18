using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datastore.Query;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Datastore.Leveldb.Tests
{
    [TestFixture]
    public class LeveldbDatastoreTests
    {
        private static void UseTempDir(Action<string> action)
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "test-datastore-flatfs-" +
                Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace("-", "").ToLower());

            Directory.CreateDirectory(path);

            try
            {
                action(path);
            }
            finally
            {
                Directory.Delete(path, true);
            }
        }

        public static void UseDatastore(Action<LeveldbDatastore> action)
        {
            UseTempDir(temp =>
            {
                using (var ds = new LeveldbDatastore(temp))
                {
                    action(ds);
                }
            });
        }

        public static void AddTestCases(LeveldbDatastore ds, Dictionary<DatastoreKey, string> testcases)
        {
            foreach (var testcase in testcases)
            {
                ds.Put(testcase.Key, Encoding.UTF8.GetBytes(testcase.Value));
            }

            foreach (var testcase in testcases)
            {
                var v2 = ds.Get(testcase.Key);
                var v2s = Encoding.UTF8.GetString(v2);

                Assert.That(v2s, Is.EqualTo(testcase.Value));
            }
        }

        private static Dictionary<DatastoreKey, string> _testcases = new Dictionary<DatastoreKey, string>()
        {
            {"/a", "a"},
            {"/a/b", "ab"},
            {"/a/b/c", "abc"},
            {"/a/b/d", "a/b/d"},
            {"/a/c", "ac"},
            {"/a/d", "ad"},
            {"/e", "e"},
            {"/f", "f"},
        };

        private static void ExpectMatches<T>(DatastoreKey[] expected, DatastoreResults<T> results)
        {
            var actual = results.Rest().Select(r => r.DatastoreKey).ToArray();
            
            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void TestQueryPrefix()
        {
            UseDatastore(ds =>
            {
                AddTestCases(ds, _testcases);

                var rs = ds.Query(new DatastoreQuery<byte[]>(prefix: "/a/"));

                ExpectMatches(new DatastoreKey[]
                {
                    "/a/b",
                    "/a/b/c",
                    "/a/b/d",
                    "/a/c",
                    "/a/d",
                }, rs);
            });
        }

        [Test]
        public void TestQueryLimitAndOffset()
        {
            UseDatastore(ds =>
            {
                AddTestCases(ds, _testcases);

                var rs = ds.Query(new DatastoreQuery<byte[]>(prefix: "/a/", offset: 2, limit: 2));

                ExpectMatches(new DatastoreKey[]
                {
                    "/a/b/d",
                    "/a/c",
                }, rs);
            });
        }

        [Test]
        public void TestBatching()
        {
            UseDatastore(ds =>
            {
                var batch = ds.Batch();

                foreach (var testcase in _testcases)
                {
                    batch.Put(testcase.Key, Encoding.UTF8.GetBytes(testcase.Value));
                }

                batch.Commit();

                foreach (var testcase in _testcases)
                {
                    var value = Encoding.UTF8.GetString(ds.Get(testcase.Key));

                    Assert.That(value, Is.EqualTo(testcase.Value));
                }
            });
        }
    }
}
