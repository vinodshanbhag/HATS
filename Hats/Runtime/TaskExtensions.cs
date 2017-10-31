using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Table
{

    public static class MyExtensions
    {
        public static void ObserveAsyncException(this Task task)
        {
            task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    Trace.TraceInformation(string.Format("An async operation returned failure in HATS - {0}", t.Exception.ToString()));
                    t.Exception.Handle(e => true);
                }
            });
        }
    }
}
    
