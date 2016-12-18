using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Remoting;
using System.Text;
using System.Threading.Tasks;
using Datastore.Query;
using Datastore.Tests;
using NUnit.Framework;

namespace Datastore.Flatfs.Tests
{
    [TestFixture]
    public class FlatfsTests
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

        private static void UseTempFs(Action<FlatfsDatastore> action)
        {
            UseTempDir(temp =>
            {
                using (var fs = new FlatfsDatastore(temp, 2, false))
                {
                    action(fs);
                }
            });
        }

        [Test]
        public void TestBadPrefixLength()
        {
            UseTempDir(temp =>
            {
                for (var i = 0; i > -3; i--)
                {
                    Assert.Throws<ArgumentOutOfRangeException>(() => new FlatfsDatastore(temp, i, false));
                }
            });
        }

        [Test]
        public void TestPut()
        {
            UseTempDir(temp =>
            {
                Assert.DoesNotThrow(() =>
                {
                    var fs = new FlatfsDatastore(temp, 2, false);
                    fs.Put(new DatastoreKey("quux"), Encoding.UTF8.GetBytes("foobar"));
                });
            });
        }

        [Test]
        public void TestGet()
        {
            UseTempFs(fs =>
            {
                var input = Encoding.UTF8.GetBytes("foobar");
                fs.Put(new DatastoreKey("quux"), input);

                var data = fs.Get(new DatastoreKey("quux"));

                Assert.That(data, Is.EqualTo(input));
            });
        }

        [Test]
        public void TestPutOverwrite()
        {
            UseTempFs(fs =>
            {
                var loser = Encoding.UTF8.GetBytes("foobar");
                var winner = Encoding.UTF8.GetBytes("xyzzy");
                var key = new DatastoreKey("quux");

                fs.Put(key, loser);
                fs.Put(key, winner);

                var data = fs.Get(key);

                Assert.That(data, Is.EqualTo(winner));
            });
        }

        [Test]
        public void TestGetNotFoundError()
        {
            UseTempFs(fs =>
            {
                Assert.Throws<KeyNotFoundException>(() => fs.Get(new DatastoreKey("quux")));
            });
        }

        [Test]
        public void TestStorage()
        {
            UseTempDir(temp =>
            {
                var prefixLength = 2;
                var prefix = "qu";
                var target = prefix + Path.DirectorySeparatorChar + "quux.data";
                var fs = new FlatfsDatastore(temp, prefixLength, false);

                fs.Put(new DatastoreKey("quux"), Encoding.UTF8.GetBytes("foobar"));

                Assert.That(Directory.EnumerateFiles(temp, target).Any(), Is.True);
            });
        }

        [Test]
        public void TestHasNotFound()
        {
            UseTempFs(fs =>
            {
                Assert.That(fs.Has(new DatastoreKey("quux")), Is.False);
            });
        }

        [Test]
        public void TestHasFound()
        {
            UseTempFs(fs =>
            {
                fs.Put(new DatastoreKey("quux"), Encoding.UTF8.GetBytes("foobar"));

                Assert.That(fs.Has(new DatastoreKey("quux")), Is.True);
            });
        }

        [Test]
        public void TestDeleteNotFound()
        {
            UseTempFs(fs =>
            {
                Assert.Throws<KeyNotFoundException>(() => fs.Delete(new DatastoreKey("quux")));
            });
        }

        [Test]
        public void TestDeleteFound()
        {
            UseTempFs(fs =>
            {
                fs.Put(new DatastoreKey("quux"), Encoding.UTF8.GetBytes("foobar"));
                fs.Delete(new DatastoreKey("quux"));

                Assert.Throws<KeyNotFoundException>(() => fs.Get(new DatastoreKey("quux")));
            });
        }

        [Test]
        public void TestQuerySimple()
        {
            UseTempFs(fs =>
            {
                var myKey = new DatastoreKey("quux");
                fs.Put(myKey, Encoding.UTF8.GetBytes("foobar"));

                var res = fs.Query(new DatastoreQuery<byte[]>(keysOnly: true));
                var entries = res.Rest();

                Assert.That(entries.Any(e => e.DatastoreKey == myKey));
            });
        }

        [Test]
        public void TestBatchPut()
        {
            UseTempFs(TestUtils.RunBatchTest);
        }

        [Test]
        public void TestBatchDelete()
        {
            UseTempFs(TestUtils.RunBatchDeleteTest);
        }
    }
}
