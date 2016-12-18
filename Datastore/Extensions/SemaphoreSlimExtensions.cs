using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Datastore.Extensions
{
    public static class SemaphoreSlimExtensions
    {
        public static void Lock(this SemaphoreSlim sm, Action action)
        {
            sm.Wait();
            try
            {
                action();
            }
            finally
            {
                sm.Release();
            }
        }

        public static T Lock<T>(this SemaphoreSlim sm, Func<T> func)
        {
            sm.Wait();
            try
            {
                return func();
            }
            finally
            {
                sm.Release();
            }
        }
    }
}
