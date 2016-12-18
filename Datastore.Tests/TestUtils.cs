using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Multiformats.Base;
using NUnit.Framework;

namespace Datastore.Tests
{
    public class TestUtils
    {
        public static void RunBatchTest(IBatchingDatastore<byte[]> ds)
        {
            var batch = ds.Batch();

            var blocks = new List<byte[]>();
            var keys = new List<DatastoreKey>();

            for (var i = 0; i < 20; i++)
            {
                var blk = new byte[256*1024];
                TestContext.CurrentContext.Random.NextBytes(blk);
                blocks.Add(blk);

                var key = new DatastoreKey(Multibase.EncodeRaw(Multibase.Base32, blk.Take(8).ToArray()));
                keys.Add(key);

                batch.Put(key, blk);
            }

            foreach (var key in keys)
            {
                Assert.Throws<KeyNotFoundException>(() => ds.Get(key));
            }

            batch.Commit();

            for (var i = 0; i < keys.Count; i++)
            {
                var blk = ds.Get(keys[i]);

                Assert.That(blk, Is.EqualTo(blocks[i]));
            }
        }

        public static void RunBatchDeleteTest(IBatchingDatastore<byte[]> ds)
        {
            var keys = new List<DatastoreKey>();
            for (var i = 0; i < 20; i++)
            {
                var blk = new byte[16];
                TestContext.CurrentContext.Random.NextBytes(blk);

                var key = new DatastoreKey(Multibase.EncodeRaw(Multibase.Base32, blk.Take(8).ToArray()));
                keys.Add(key);

                ds.Put(key, blk);
            }

            var batch = ds.Batch();

            foreach (var key in keys)
            {
                batch.Delete(key);
            }

            batch.Commit();

            foreach (var key in keys)
            {
                Assert.Throws<KeyNotFoundException>(() => ds.Get(key));
            }
        }
    }
}
