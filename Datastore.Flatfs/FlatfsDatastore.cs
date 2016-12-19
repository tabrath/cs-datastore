using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Extensions;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.Flatfs
{
    public class FlatfsDatastore : IBatchingDatastore<byte[]>, IThreadSafeDatastore<byte[]>
    {
        private const string extension = ".data";
        private const int maxPrefixLength = 16;
        private static readonly string padding = new string('_', maxPrefixLength);

        private readonly string _path;
        private readonly int _prefixLength;
        private readonly bool _sync;

        public FlatfsDatastore(string path, int prefixLength, bool sync)
        {
            if (prefixLength <= 0 || prefixLength > maxPrefixLength)
                throw new ArgumentOutOfRangeException(nameof(prefixLength));

            _path = path;
            _prefixLength = prefixLength;
            _sync = sync;
        }

        private void Encode(DatastoreKey key, out string dir, out string file)
        {
            var noslash = key.ToString().Substring(1);
            var prefix = (noslash + padding).Substring(0, _prefixLength);
            dir = Path.Combine(_path, prefix);
            file = Path.Combine(dir, noslash + extension);
        }

        private bool Decode(string file, out DatastoreKey key)
        {
            if (Path.GetExtension(file) != extension)
            {
                key = null;
                return false;
            }

            var name = Path.GetFileNameWithoutExtension(file);
            key = new DatastoreKey(name);
            return true;
        }

        private bool MakePrefixDir(string dir)
        {
            if (!MakePrefixDirNoSync(dir))
                return false;

            if (_sync)
            {
                if (!SyncDir(_path))
                    return false;
            }

            return true;
        }

        private const int putMaxRetries = 6;

        public const int SyncThreadsMax = 16;
        private static readonly SemaphoreSlim _syncSemaphore = new SemaphoreSlim(1, SyncThreadsMax);

        private static bool SyncDir(string dir)
        {
            return _syncSemaphore.Lock(() => true);
        }

        private static bool SyncFile(FileStream file)
        {
            return _syncSemaphore.Lock(() =>
            {
                file.Flush(true);
                return true;
            });
        }

        private bool MakePrefixDirNoSync(string dir)
        {
            if (Directory.Exists(dir))
                return true;

            return Directory.CreateDirectory(dir)?.Exists ?? false;
        }

        public IDatastoreBatch<byte[]> Batch() => new FlatfsBatch(this);

        public void Dispose()
        {
        }

        public void Put(DatastoreKey datastoreKey, byte[] value)
        {
            for (var i = 0; i < putMaxRetries; i++)
            {
                if (DoPut(datastoreKey, value))
                    break;

                Thread.Sleep(100*i);
            }
        }

        private bool DoPut(DatastoreKey key, byte[] value)
        {
            string dir, file;
            Encode(key, out dir, out file);
            if (!MakePrefixDir(dir))
                return false;

            var tmp = File.Create(Path.GetTempFileName());
            var closed = false;
            var removed = false;

            try
            {
                tmp.Write(value, 0, value.Length);
                if (_sync)
                {
                    if (!SyncFile(tmp))
                        return false;
                }
                tmp.Close();
                closed = true;

                if (File.Exists(file))
                    File.Delete(file);

                File.Move(tmp.Name, file);
                removed = true;

                if (_sync)
                {
                    if (!SyncDir(dir))
                        return false;
                }

                return true;
            }
            finally
            {
                if (!closed)
                    tmp.Close();

                if (!removed)
                    File.Delete(tmp.Name);
            }
        }

        private bool PutMany(IEnumerable<KeyValuePair<DatastoreKey, byte[]>> data)
        {
            var dirsToSync = new List<string>();
            var files = new Dictionary<FileStream, string>();

            foreach (var kv in data)
            {
                var val = kv.Value as byte[];
                if (val == null)
                    return false;

                string dir, file;
                Encode(kv.Key, out dir, out file);
                if (!MakePrefixDirNoSync(dir))
                    return false;

                dirsToSync.Add(dir);

                var tmp = File.Create(Path.GetTempFileName());
                tmp.Write(val, 0, val.Length);
                
                files.Add(tmp, file);
            }

            var ops = new Dictionary<FileStream, int>();

            try
            {
                foreach (var file in files)
                {
                    if (_sync)
                    {
                        if (!SyncFile(file.Key))
                            return false;
                    }
                    
                    file.Key.Close();
                    ops[file.Key] = 1;
                }

                foreach (var file in files)
                {
                    if (File.Exists(file.Value))
                        File.Delete(file.Value);

                    File.Move(file.Key.Name, file.Value);
                    ops[file.Key] = 2;
                }

                if (_sync)
                {
                    foreach (var dir in dirsToSync)
                    {
                        if (!SyncDir(dir))
                            return false;
                    }

                    if (!SyncDir(_path))
                        return false;
                }

                return true;
            }
            finally 
            {
                foreach (var fi in files)
                {
                    var val = ops[fi.Key];
                    if (val == 0)
                        fi.Key.Close();
                    
                    if (val == 1)
                        File.Delete(fi.Key.Name);   
                }
            }
        }

        public byte[] Get(DatastoreKey datastoreKey)
        {
            string dir, file;
            Encode(datastoreKey, out dir, out file);
            if (!File.Exists(file))
                throw new KeyNotFoundException();

            return File.ReadAllBytes(file);
        }

        public bool Has(DatastoreKey datastoreKey)
        {
            string dir, file;
            Encode(datastoreKey, out dir, out file);
            return File.Exists(file);
        }

        public void Delete(DatastoreKey datastoreKey)
        {
            string dir, file;
            Encode(datastoreKey, out dir, out file);
            if (!File.Exists(file))
                throw new KeyNotFoundException();

            File.Delete(file);
        }

        public DatastoreResults<byte[]> Query(DatastoreQuery<byte[]> q)
        {
            if ((!string.IsNullOrEmpty(q.Prefix) && q.Prefix != "/") ||
                q.QueryFilters.Length > 0 ||
                q.QueryOrders.Length > 0 ||
                q.Limit > 0 ||
                q.Offset > 0 ||
                !q.KeysOnly)
                throw new Exception("flatfs only supports listing all keys in random order");

            var reschan = new BlockingCollection<DatastoreResult<byte[]>>(DatastoreQuery<byte[]>.KeysOnlyBufferSize);

            Task.Run(() => WalkTopLevel(_path, reschan))
                .ContinueWith(_ => reschan.CompleteAdding());

            return DatastoreResults<byte[]>.WithCollection(q, reschan);
        }

        private void WalkTopLevel(string path, BlockingCollection<DatastoreResult<byte[]>> reschan)
        {
            foreach (var dir in Directory.EnumerateDirectories(path))
            {
                if (string.IsNullOrEmpty(dir) || dir[0] == '.')
                    continue;
                
                Walk(Path.Combine(path, dir), reschan);
            }
        }

        private void Walk(string path, BlockingCollection<DatastoreResult<byte[]>> reschan)
        {
            foreach (var file in Directory.EnumerateFiles(path))
            {
                if (string.IsNullOrEmpty(file) || file[0] == '.')
                    continue;

                DatastoreKey key;
                if (!Decode(file, out key))
                    continue;
               
                if (!reschan.TryAdd(new DatastoreResult<byte[]>(key, null)))
                    break;
            }
        }

        public IThreadSafeDatastore<byte[]> Synchronized() => new SynchronizedDatastore<byte[]>(this);

        public bool IsThreadSafe => true;

        private class FlatfsBatch : IDatastoreBatch<byte[]>
        {
            private readonly FlatfsDatastore _ds;
            private readonly Dictionary<DatastoreKey, byte[]> _puts;
            private readonly List<DatastoreKey> _deletes;

            public FlatfsBatch(FlatfsDatastore ds)
            {
                _ds = ds;
                _puts = new Dictionary<DatastoreKey, byte[]>();
                _deletes = new List<DatastoreKey>();
            }

            public void Put(DatastoreKey datastoreKey, byte[] value)
            {
                _puts.Add(datastoreKey, value);
            }

            public void Delete(DatastoreKey datastoreKey)
            {
                _deletes.Add(datastoreKey);
            }

            public void Commit()
            {
                if (!_ds.PutMany(_puts))
                    return;

                _deletes.ForEach(d => _ds.Delete(d));
            }
        }
    }
}
