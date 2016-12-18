using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Datastore.Mount
{
    public class DatastoreMount<T>
    {
        public DatastoreKey Prefix { get; }
        public IDatastore<T> Datastore { get; }

        public DatastoreMount(DatastoreKey prefix, IDatastore<T> datastore)
        {
            Prefix = prefix;
            Datastore = datastore;
        }
    }
}
