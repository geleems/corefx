using System.Collections.Generic;
using System.Threading;
using static System.Data.SqlClient.Tests.TestUtils;

namespace System.Data.SqlClient.Tests
{
    public class PerformanceTestWithThreads
    {
        public static void MainRun()
        {
            TableScheme ts = new TableScheme("MyTestTable");
            ts.columns.Add(new TableColumn("col1", "int"));
            ts.columns.Add(new TableColumn("col2", "text"));

            bool dropTable = false;
            bool createTable = false;
            bool testInsert = false;
            bool testSelect = true;

            if (dropTable)
            {
                TestUtils.RunNonQuery(Secret.SqlClientConnectionString, $"DROP TABLE {ts.tableName}");
            }

            if (createTable)
            {
                TestUtils.RunNonQuery(Secret.SqlClientConnectionString, TestUtils.GenerateCreateTableSql(ts));
            }

            if (testInsert)
            {
                for (int i = 0; i < 50; ++i)
                {
                    new InsertWorker(Secret.SqlClientConnectionString, ts, 1000);
                }
                InsertWorker.Start();
                InsertWorker.Stop();
                InsertWorker.Clear();
                Console.WriteLine("InsertWorker Number of failure: " + InsertWorker.GetNumOfFailure());
            }

            if (testSelect)
            {
                for (int i = 0; i < 10; ++i)
                {
                    new SelectWorker(Secret.SqlClientConnectionString, ts, 10);
                }
                SelectWorker.Start();
                SelectWorker.Stop();
                SelectWorker.Clear();
                Console.WriteLine("SelectWorker Number of failure: " + SelectWorker.GetNumOfFailure());
            }

            //Console.WriteLine("Press any key.....");
            //Console.ReadLine();
        }


        public class InsertWorker
        {
            private static ManualResetEventSlim startEvent = new ManualResetEventSlim(false);
            private static List<InsertWorker> workerList = new List<InsertWorker>();
            private ManualResetEventSlim doneEvent = new ManualResetEventSlim(false);
            private static int numOfFailure = 0;
            private string connectionString;
            private TableScheme tableScheme;
            private int numOfRows;
            private Thread thread;


            public InsertWorker(string connectionString, TableScheme tableScheme, int numOfRows)
            {
                this.connectionString = connectionString;
                this.tableScheme = tableScheme;
                this.numOfRows = numOfRows;

                workerList.Add(this);
                thread = new Thread(new ThreadStart(ThreadRun));
                thread.Start();
            }

            public static void Clear()
            {
                workerList.Clear();
                workerList = null;
                startEvent = null;
            }

            public static void Start()
            {
                startEvent.Set();
            }

            public static List<InsertWorker> GetWorkerList()
            {
                return workerList;
            }

            public static int GetNumOfFailure()
            {
                return numOfFailure;
            }

            public static void Stop()
            {
                foreach (InsertWorker w in workerList)
                {
                    w.doneEvent.Wait();
                }
            }

            public void ThreadRun()
            {
                startEvent.Wait();

                string sql = null;
                try
                {
                    for (int i = 0; i < numOfRows; ++i)
                    {
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            using (SqlCommand command = connection.CreateCommand())
                            {
                                sql = TestUtils.GererateRandomInsertSql(tableScheme);
                                command.CommandText = sql;
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e.Message);
                    Console.WriteLine($"SQL \"{sql}\" was skipped.");
                    Interlocked.Increment(ref numOfFailure);
                }

                doneEvent.Set();
            }
        }


        public class SelectWorker
        {
            private static ManualResetEventSlim startEvent = new ManualResetEventSlim(false);
            private static List<SelectWorker> workerList = new List<SelectWorker>();
            private ManualResetEventSlim doneEvent = new ManualResetEventSlim(false);
            private static int numOfFailure = 0;
            private string connectionString;
            private TableScheme tableScheme;
            private int numOfRows;
            private Thread thread;


            public SelectWorker(string connectionString, TableScheme tableScheme, int numOfRows)
            {
                this.connectionString = connectionString;
                this.tableScheme = tableScheme;
                this.numOfRows = numOfRows;

                workerList.Add(this);
                thread = new Thread(new ThreadStart(ThreadRun));
                thread.Start();
            }

            public static void Clear()
            {
                workerList.Clear();
                workerList = null;
                startEvent = null;
            }

            public static void Start()
            {
                startEvent.Set();
            }

            public static List<SelectWorker> GetWorkerList()
            {
                return workerList;
            }

            public static int GetNumOfFailure()
            {
                return numOfFailure;
            }

            public static void Stop()
            {
                foreach (SelectWorker w in workerList)
                {
                    w.doneEvent.Wait();
                }
            }

            public void ThreadRun()
            {
                startEvent.Wait();

                string rows = numOfRows > 0 ? $"TOP({numOfRows}) " : "";
                string sql = $"SELECT {rows}* FROM {tableScheme.tableName}";
                try
                {
                    List<object[]> result = null;
                    for(int n=0; n<10; ++n)
                    {
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            using (SqlCommand command = connection.CreateCommand())
                            {
                                command.CommandText = sql;
                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    result = new List<object[]>();
                                    if (reader.HasRows)
                                    {
                                        int numOfColumns = reader.FieldCount;
                                        object[] row = null;
                                        while (reader.Read())
                                        {
                                            row = new object[numOfColumns];
                                            for (int i = 0; i < numOfColumns; ++i)
                                            {
                                                row[i] = reader[i];
                                                //Console.Write(row[i]+" ");
                                            }
                                            //Console.WriteLine();
                                            result.Add(row);
                                        }
                                    }
                                }
                            }
                        }
                        Console.WriteLine(result.Count);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e.Message);
                    Console.WriteLine($"SQL \"{sql}\" was skipped.");
                    Interlocked.Increment(ref numOfFailure);
                }

                doneEvent.Set();
            }
        }
    }
}
