using System;
using System.Linq;
using System.Text;

namespace Datastore
{
    public class DatastoreKey : IEquatable<DatastoreKey>, IComparable<DatastoreKey>
    {
        private string _value;

        public DatastoreKey(string value, bool raw = false)
        {
            if (raw)
            {
                if (string.IsNullOrEmpty(value))
                    _value = "/";
                else
                {
                    if (value[0] != '/' || (value.Length > 1 && value[value.Length - 1] == '/'))
                        throw new ArgumentException($"Invalid datastore key: {value}", nameof(value));

                    _value = value;
                }
            }
            else
            {
                _value = value;

                Clean();
            }
        }

        public DatastoreKey(params string[] ns)
            : this(string.Join("/", ns))
        {
        }

        public void Clean()
        {
            if (string.IsNullOrEmpty(_value))
                _value = "/";
            else if (_value[0] == '/')
                _value = CleanPath(_value);
            else
                _value = CleanPath("/" + _value);
        }

        private static string CleanPath(string value)
        {
            if (System.IO.Path.DirectorySeparatorChar != '/')
                value = value.Replace(System.IO.Path.DirectorySeparatorChar, '/');

            return value;
        }

        public bool Equals(DatastoreKey other) => other != null && _value.Equals(other._value);

        public int CompareTo(DatastoreKey other)
        {
            var list1 = List();
            var list2 = other.List();

            for (var i = 0; i < list1.Length; i++)
            {
                var c1 = list1[i];
                if (list2.Length < (i + 1))
                    return 1;

                var c2 = list2[i];
                var c = c1.CompareTo(c2);
                //var c = CompareStrings(c1, c2);
                if (c != 0)
                    return c;
            }

            if (list1.Length < list2.Length)
                return -1;
            if (list1.Length > list2.Length)
                return 1;

            return 0;
        }

        private static int CompareStrings(string a, string b)
        {
            for (var i = 0; i < a.Length; i++)
            {
                if (b.Length < i + 1)
                    return 1;

                if (a[i] > b[i])
                    return 1;
                if (a[i] < b[i])
                    return -1;
            }

            if (a.Length < b.Length)
                return -1;
            if (a.Length > b.Length)
                return 1;

            return 0;
        }

        public override string ToString() => _value;
        public byte[] ToBytes() => Encoding.UTF8.GetBytes(_value);
        public override bool Equals(object obj) => Equals(obj as DatastoreKey);
        public string[] List() => _value.Split(new[] {'/'}, StringSplitOptions.RemoveEmptyEntries);
        public DatastoreKey Reverse() => new DatastoreKey(List().Reverse().ToArray());
        public string[] Namespaces => List();
        public string BaseNamespace => Namespaces.Last();
        public string Type => NamespaceType(BaseNamespace);
        public string Name => NamespaceValue(BaseNamespace);
        public DatastoreKey Instance(string s) => new DatastoreKey($"{_value}:{s}");
        public DatastoreKey Path => new DatastoreKey($"{Parent}/{NamespaceType(BaseNamespace)}");

        public DatastoreKey Parent
        {
            get
            {
                var n = List();
                return n.Length == 1 ? new DatastoreKey("/", true) : new DatastoreKey(string.Join("/", n, 0, n.Length - 1));
            }
        }

        public DatastoreKey Child(DatastoreKey other)
        {
            if (_value == "/")
                return other;
            if (other._value == "/")
                return this;

            return new DatastoreKey(_value + other._value, true);
        }

        public DatastoreKey Child(string s) => new DatastoreKey($"{_value}/{s}");

        public bool IsAncestorOf(DatastoreKey other)
        {
            if (other._value == _value)
                return false;

            return other._value.StartsWith(_value);
        }

        public bool IsDescendantOf(DatastoreKey other)
        {
            if (other._value == _value)
                return false;

            return _value.StartsWith(other._value);
        }

        public bool IsTopLevel => List().Length == 1;

        public static DatastoreKey RandomKey() => new DatastoreKey(Guid.NewGuid().ToString().Replace("-",""));

        public static string NamespaceType(string ns)
        {
            var parts = ns.Split(':');
            if (parts.Length < 2)
                return string.Empty;

            return string.Join(":", parts, 0, parts.Length - 1);
        }

        public static string NamespaceValue(string ns) => ns.Split(':').Last();

        public static implicit operator DatastoreKey(string s) => new DatastoreKey(s, true);
        public static implicit operator string(DatastoreKey datastoreKey) => datastoreKey._value;

        public static bool operator >(DatastoreKey a, DatastoreKey b) => a.CompareTo(b) == 1;
        public static bool operator >=(DatastoreKey a, DatastoreKey b) => a.CompareTo(b) >= 0;
        public static bool operator <(DatastoreKey a, DatastoreKey b) => a.CompareTo(b) == -1;
        public static bool operator <=(DatastoreKey a, DatastoreKey b) => a.CompareTo(b) <= 0;
        public static bool operator ==(DatastoreKey a, DatastoreKey b) => a?.Equals(b) ?? false;
        public static bool operator !=(DatastoreKey a, DatastoreKey b) => !a?.Equals(b) ?? false;

    }
}