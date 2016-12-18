using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Datastore.Extensions
{
    public static class ByteArrayExtensions
    {
        public static bool HasPrefix(this byte[] buffer, byte[] prefix)
        {
            for (var i = 0; i < prefix.Length; i++)
            {
                if (i > buffer.Length - 1)
                    return false;

                if (buffer[i] != prefix[i])
                    return false;
            }

            return true;
        }
    }
}
