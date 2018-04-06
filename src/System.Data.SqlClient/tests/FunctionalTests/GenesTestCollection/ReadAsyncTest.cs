using System.Threading.Tasks;
using System.Data.SqlClient.ManualTesting.Tests;
using System.Diagnostics;

namespace System.Data.SqlClient.Tests
{
    public class ReadAsyncTest
    {
        public static void Run()
        {
            Stopwatch s = new Stopwatch();
            s.Start();
            TestAsync2();
            s.Stop();
            Console.WriteLine(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "+s.Elapsed.TotalMilliseconds);
            //Sample();
        }

        private static void Sample()
        {
            DataTable schema = null;

            using (var con = new SqlConnection(Secret.SqlClientConnectionString))
            {
                con.Open();

                using (var command = new SqlCommand("drop table test_table", con))
                {
                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch { }
                }

                using (var command = new SqlCommand("create table test_table (col int)", con))
                {
                    command.ExecuteNonQuery();
                }

                for(int i=0; i<10; ++i)
                {
                    using (var command = new SqlCommand($"insert into test_table (col) values ({i})", con))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                
                using (var command = new SqlCommand("select count(col) from test_table", con))
                {
                    using (var reader = command.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        schema = reader.GetSchemaTable();
                    }
                }
            }

            foreach (DataRow column in schema.Rows)
            {
                Console.WriteLine("ColumnName={0}", column.Field<String>("ColumnName"));
                Console.WriteLine("Is this columnName null or empty? : {0}", String.IsNullOrEmpty(column.Field<String>("ColumnName")));
            }
        }

        private static async void TestAsync()
        {
            // Declare your connection string in app.config like
            // <connectionStrings><remove name="LocalSqlServer"/><add name="LocalSqlServer" connectionString="Data Source=localhost\SQLEXPRESS;Integrated Security=true"/></connectionStrings>
            using (SqlConnection connection = new SqlConnection(Secret.SqlClientConnectionString))
            {
                Console.WriteLine("connecting…");
                await connection.OpenAsync();
                Console.WriteLine("connected!");

                // Install a stored procedure.
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SET NOCOUNT ON"
                                        + " SELECT 'a'"
                                        + " DECLARE @t DATETIME = SYSDATETIME()"
                                        + " WHILE DATEDIFF(s, @t, SYSDATETIME()) < 20 BEGIN"
                                        + "   SELECT 2 x INTO #y"
                                        + "   DROP TABLE #y"
                                        + " END"
                                        + " SELECT 'b'";

                    Console.WriteLine("executing…");
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        Console.WriteLine("reading…");
                        do
                        {
                            while (await reader.ReadAsync())
                            {
                            }
                        } while (await reader.NextResultAsync());
                        Console.WriteLine("done!");
                    }
                }
            }
            await Task.Delay(TimeSpan.FromSeconds(5));
        }

        private static async void TestAsync1()
        {
            // Declare your connection string in app.config like
            // <connectionStrings><remove name="LocalSqlServer"/><add name="LocalSqlServer" connectionString="Data Source=localhost\SQLEXPRESS;Integrated Security=true"/></connectionStrings>
            using (SqlConnection connection = new SqlConnection(Secret.SqlClientConnectionString))
            {
                Console.WriteLine("connecting…");
                await connection.OpenAsync();
                Console.WriteLine("connected!");

                // Install a stored procedure.
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "BEGIN WAITFOR DELAY '00:01'; select 'a'; END; GO";

                    Console.WriteLine("executing…");
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        Console.WriteLine("reading…");
                        do
                        {
                            while (await reader.ReadAsync())
                            {
                            }
                        } while (await reader.NextResultAsync());
                        Console.WriteLine("done!");
                    }
                }
            }
            await Task.Delay(TimeSpan.FromSeconds(5));
        }

        private static async void TestAsync2()
        {
            // Declare your connection string in app.config like
            // <connectionStrings><remove name="LocalSqlServer"/><add name="LocalSqlServer" connectionString="Data Source=localhost\SQLEXPRESS;Integrated Security=true"/></connectionStrings>
            using (SqlConnection connection = new SqlConnection(Secret.SqlClientConnectionString))
            {
                Console.WriteLine("connecting…");
                await connection.OpenAsync();
                Console.WriteLine("connected!");

                // Install a stored procedure.
                using (var command = connection.CreateCommand())
                {
                    
                    command.CommandText = "SET NOCOUNT ON"
                                        + " SELECT 'a'"
                                        + " DECLARE @t DATETIME = SYSDATETIME()"
                                        + " WHILE DATEDIFF(s, @t, SYSDATETIME()) < 20 BEGIN"
                                        + "   SELECT 2 x INTO #y"
                                        + "   DROP TABLE #y"
                                        + " END"
                                        + " SELECT 'b'";
                    
                    /*
                    command.CommandText = "SELECT 'a'; "
                                        + "WAITFOR DELAY '00:00:05'; "
                                        + "SELECT 'b';";
                    */

                    Console.WriteLine("executing…");
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        Console.WriteLine("reading…");

                        Task<bool> x = null;
                        do
                        {
                            Task<bool> t1 = null;
                            //Task<bool> t2 = null;
                            do
                            {
                                if (t1 != null)
                                {
                                    //Console.WriteLine(reader[0]);
                                }

                                t1 = reader.ReadAsync();
                                //t2 = reader.ReadAsync();

                                for (int i = 0; i < 3; ++i)
                                {
                                    Console.WriteLine("haha "+ (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond));
                                }
                            }
                            while (await t1);
                            Console.WriteLine("==========================================");

                            x = reader.NextResultAsync();
                            for (int i = 0; i < 3; ++i)
                            {
                                Console.WriteLine("hoho " + (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond));
                            }
                            Console.WriteLine("==========================================");
                        }
                        while (await x);

                        Console.WriteLine("done!");
                        Console.WriteLine("==========================================");
                    }
                }
            }
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }
}
