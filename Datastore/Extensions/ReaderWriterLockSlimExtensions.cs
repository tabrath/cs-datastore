using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Datastore.Extensions
{
    internal static class ReaderWriterLockSlimExtensions
    {
        public static T Read<T>(this ReaderWriterLockSlim rwl, Func<T> func)
        {
            rwl.EnterReadLock();
            try
            {
                return func();
            }
            finally
            {
                rwl.ExitReadLock();
            }
        }

        public static void Write(this ReaderWriterLockSlim rwl, Action action)
        {
            rwl.EnterWriteLock();
            try
            {
                action();
            }
            finally
            {
                rwl.ExitWriteLock();
            }
        }
    }
}
