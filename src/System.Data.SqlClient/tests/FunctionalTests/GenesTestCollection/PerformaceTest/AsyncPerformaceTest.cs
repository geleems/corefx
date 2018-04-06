using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;

namespace System.Data.SqlClient.Tests
{
    public class AsyncPerformaceTest
    {
        public class Worker
        {
            private static Dictionary<int, Thread> map = new Dictionary<int, Thread>();
            private Thread thread;
            private int threadId;

            public Worker()
            {
                thread = new Thread(new ThreadStart(Run));
                threadId = thread.GetHashCode();
                map.Add(threadId, thread);
            }

            public void Start()
            {
                thread.Start();
            }

            public static void Stop()
            {
                foreach(Thread t in map.Values)
                {
                    if(!t.Join(300))
                    {
                        t.Interrupt();
                    }
                }
            }

            private void Run()
            {
                Console.WriteLine($"Thread {threadId} started..");

                using (var connection = new SqlConnection(Secret.SqlClientConnectionString))
                {
                    try
                    {
                        connection.Open();
                        SqlCommand command = connection.CreateCommand();
                        command.CommandText = @"SELECT * FROM MyAmazingTest";
                        var reader = command.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
                        var count = 0;
                        while (reader.Read())
                        {
                            count += 1;
                        }
                        Console.WriteLine("Count: " + count);
                    }
                    catch (Exception x)
                    {
                        Console.WriteLine(x.ToString());
                    }
                }
                map.Remove(threadId);
            }
        }

        public static void Run()
        {
            /*
            for(int i=0; i<200; ++i)
            {
                Worker w = new Worker();
                w.Start();
            }

            Worker.Stop();
            */

            List<DnsWorker> list = new List<DnsWorker>();
            for (int i = 0; i < 200; ++i)
            {
                list.Add(new DnsWorker());
            }

            DnsWorker.Start();
            DnsWorker.Stop();


            double theMax = 0;
            foreach (DnsWorker w in list)
            {
                if (theMax < w.MaxDelay)
                {
                    theMax = w.MaxDelay;
                }
            }

            Console.WriteLine("theMax: " + theMax);
        }

        public class DnsWorker
        {
            private static ManualResetEventSlim startEvent = new ManualResetEventSlim(false);
            private static List<DnsWorker> workerList = new List<DnsWorker>();
            private ManualResetEventSlim doneEvent = new ManualResetEventSlim(false);
            private double maxDelay;
            private Thread thread;

            public DnsWorker()
            {
                workerList.Add(this);
                //thread = new Thread(new ThreadStart(ResolveHostAsync));
                thread = new Thread(new ThreadStart(SqlConnectionOpen));
                thread.Start();
            }

            public double MaxDelay
            {
                get
                {
                    return maxDelay;
                }
            }

            public static void Start()
            {
                startEvent.Set();
            }

            public static void Stop()
            {
                foreach (DnsWorker w in workerList)
                {
                    w.doneEvent.Wait();
                }
            }

            public void SqlConnectionOpen()
            {
                startEvent.Wait();

                Stopwatch sw = new Stopwatch();
                for (int i = 0; i < 1; ++i)
                {
                    using (SqlConnection con = new SqlConnection(Secret.SqlClientConnectionString))
                    {
                        sw.Start();
                        con.Open();
                        sw.Stop();
                    }
                    
                    double elapsed = sw.Elapsed.TotalMilliseconds;
                    if (maxDelay < elapsed)
                    {
                        maxDelay = elapsed;
                    }

                    sw.Reset();
                }

                doneEvent.Set();
            }

            public void ResolveHost()
            {
                startEvent.Wait();

                Stopwatch sw = new Stopwatch();
                for (int i = 0; i < 1; ++i)
                {
                    sw.Start();
                    IPAddress[] addresses = Dns.GetHostAddresses("gelee-vm-win10a");
                    sw.Stop();

                    double elapsed = sw.Elapsed.TotalMilliseconds;
                    if (maxDelay < elapsed)
                    {
                        maxDelay = elapsed;
                    }

                    sw.Reset();
                }

                doneEvent.Set();
            }

            public async void ResolveHostAsync()
            {
                startEvent.Wait();

                Stopwatch sw = new Stopwatch();
                for (int i = 0; i < 1; ++i)
                {
                    sw.Start();
                    IPAddress[] addresses = await Dns.GetHostAddressesAsync("gelee-vm-win10a");
                    sw.Stop();

                    double elapsed = sw.Elapsed.TotalMilliseconds;
                    if (maxDelay < elapsed)
                    {
                        maxDelay = elapsed;
                    }

                    sw.Reset();
                }

                doneEvent.Set();
            }
        }
    }
}
