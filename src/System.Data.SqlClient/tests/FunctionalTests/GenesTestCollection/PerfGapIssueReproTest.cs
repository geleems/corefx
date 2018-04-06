using System.Diagnostics;

namespace System.Data.SqlClient.Tests
{
    public class PerfGapIssueReproTest
    {
        public static void Run()
        {
            for (int i = 0; i < 10; ++i)
            {
                InsertToTableAsync();
            }
        }

        public static async void InsertToTableAsync()
        {
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            string testTableName = TestUtils.GenerateTableName();
            TestUtils.RunNonQuery(connectionString, "create table " + testTableName + " (col1 int)");

            int count = 1000;
            Stopwatch stopwatch = new Stopwatch();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        stopwatch.Start();
                        for (int i = 0; i < count; ++i)
                        {
                            command.CommandText = "INSERT INTO " + testTableName + " VALUES (" + i + ")";
                            await command.ExecuteNonQueryAsync();
                        }
                        stopwatch.Stop();
                    }
                }
            }
            finally
            {
                TestUtils.RunNonQuery(connectionString, "drop table " + testTableName);
            }

            float average_time = (float)stopwatch.ElapsedMilliseconds / 1000F;
            Console.WriteLine("average_time: " + average_time + "ms");
        }
    }
}
