using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using Datastore.Query;
using Datastore.Sync;

namespace Datastore.Filesystem
{
    public class FilesystemDatastore<T> : IBatchingDatastore<T>
    {
        public const string ObjectKeySuffix = ".dsobject";

        private readonly string _path;
        private readonly BinaryFormatter _bf;

        public FilesystemDatastore(string path)
        {
            _path = path;
            _bf = new BinaryFormatter();
        }

        public static string FixSeparator(string path) => path.Replace('/', Path.DirectorySeparatorChar);

        public string GetKeyFilename(DatastoreKey datastoreKey) => _path + FixSeparator(datastoreKey.ToString()) + ObjectKeySuffix;

        public IDatastoreBatch<T> Batch() => new BasicDatastoreBatch<T>(this);

        public void Dispose()
        {
        }

        public void Put(DatastoreKey datastoreKey, T value)
        {
            var fn = GetKeyFilename(datastoreKey);
            var dir = Path.GetDirectoryName(fn);
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            using (var stream = File.Create(fn))
            {
                var bytes = value as byte[];
                if (bytes != null)
                    stream.Write(bytes, 0, bytes.Length);
                else
                    _bf.Serialize(stream, value);
            }
        }

        public T Get(DatastoreKey datastoreKey)
        {
            var fn = GetKeyFilename(datastoreKey);
            if (!File.Exists(fn))
                throw new KeyNotFoundException();

            using (var stream = File.OpenRead(fn))
            {
                if (typeof(T) is byte[])
                {
                    var value = new byte[stream.Length];
                    stream.Read(value, 0, value.Length);
                    return (T)(object) value;
                }

                return (T)_bf.Deserialize(stream);
            }
        }

        public bool Has(DatastoreKey datastoreKey) => File.Exists(GetKeyFilename(datastoreKey));

        public void Delete(DatastoreKey datastoreKey)
        {
            var fn = GetKeyFilename(datastoreKey);
            if (!File.Exists(fn))
                throw new KeyNotFoundException();

            File.Delete(fn);
        }

        public DatastoreResults<T> Query(DatastoreQuery<T> q)
        {
            var results = new BlockingCollection<DatastoreResult<T>>();

            Task.Factory.StartNew(() =>
                {
                    foreach (var item in new DirectoryInfo(_path).EnumerateFiles("*" + ObjectKeySuffix, SearchOption.AllDirectories)
                        .Select(f =>
                        {
                            var path = f.FullName;
                            if (path.StartsWith(_path))
                                path = path.Substring(_path.Length);

                            if (Path.IsPathRooted(path))
                                path = path.TrimStart(Path.DirectorySeparatorChar);

                            var key = new DatastoreKey(path.Substring(0, path.IndexOf(ObjectKeySuffix)));
                            var entry = new DatastoreEntry<T>(key, default(T));
                            return new DatastoreResult<T>(entry);
                        }))
                    {
                        if (!results.TryAdd(item, Timeout.Infinite))
                            break;
                    }
                })
                .ContinueWith(_ => results.CompleteAdding());

            return DatastoreResults<T>.WithCollection(q, results).NaiveQueryApply();
        }

        public IThreadSafeDatastore<T> Synchronized() => new SynchronizedDatastore<T>(this);
    }
}