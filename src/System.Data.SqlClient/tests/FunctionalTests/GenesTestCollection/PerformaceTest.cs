using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace System.Data.SqlClient.Tests
{
    public class SimplePerformaceTest
    {
        private static int port = 13000;
        private static string ipAddStr = "127.0.0.1";
        private static int bufferSize = 8000;
        private static string dummyData = GetRandomString(1000);
        static SimplePerformaceTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                dummyData += dummyData;
            }
        }

        public static void Run()
        {
            StartTcpServer();
            StartTcpClient();
        }

        private static async void StartTcpServer()
        {
            Console.WriteLine("StartTcpServer");
            TcpListener serverSocket = new TcpListener(IPAddress.Parse(ipAddStr), port);
            serverSocket.Start();
            Console.WriteLine(" >> Server Started");
            TcpClient clientSocket = await serverSocket.AcceptTcpClientAsync();
            Console.WriteLine(" >> Accept connection from client");

            NetworkStream networkStream = clientSocket.GetStream();

            while (true)
            {
                try
                {
                    string serverResponse = dummyData;
                    Byte[] bytesToSend = Encoding.ASCII.GetBytes(serverResponse);
                    networkStream.Write(bytesToSend, 0, bytesToSend.Length);
                    networkStream.Flush();
                    Console.WriteLine(" >> server sent bytes");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    clientSocket.Dispose();
                    serverSocket.Stop();
                }
            }
        }

        private static void StartTcpClient()
        {
            byte[] readBuffer = new byte[bufferSize];
            Socket clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            clientSocket.SendBufferSize = bufferSize;
            clientSocket.ReceiveBufferSize = bufferSize;
            clientSocket.Connect(IPAddress.Parse(ipAddStr), port);
            Console.WriteLine(" >> Connected to Server");
            NetworkStream tcpStream = new NetworkStream(clientSocket, true);
            while (true)
            {
                int length = tcpStream.Read(readBuffer, 0, bufferSize);
                Console.WriteLine($" >> {length} bytes received");
            }
        }

        private static string GetRandomString(int length)
        {
            string result = "";

            do
            {
                result += Guid.NewGuid().ToString("N");
            }
            while (result.Length < length);

            if (result.Length > length)
            {
                result = result.Substring(0, length);
            }

            return result;
        }
    }
    

    public class PerformaceTest
    {
        private static string connString = Secret.SqlClientConnectionString;
        private static string testTableName = Secret.testTableName;

        public static void Run()
        {
            //SimplePerformaceTest.Run();

            //PacketSizeMod.Change(9000);

            //RunInsertTableTests(1, 36124);

            /*
            string sqlHowManyRowsExists = $"SELECT COUNT(*) FROM {testTableName}";
            List<object[]> rows = RunQuery(connString, sqlHowManyRowsExists);
            Console.WriteLine(sqlHowManyRowsExists + " --> " + (rows.Count > 0 ? rows[0][0] : "0"));
            */

            //RunSelectTableTests(1, 500000);

            //PerfTestWithThread.Run();

            //OdbcParameterTest.RunTest();

            PerfTestWithThread.Run(100, 1, 10000);
        }

        private static void RunInsertTableTests(int numOfTestRun, int numOfRows)
        {
            long timeConsumedAllTests = 0;

            for (int i = 0; i < numOfTestRun; ++i)
            {
                long timeConsumedSingleTest = InsertTableTest(numOfRows);
                timeConsumedAllTests += timeConsumedSingleTest;
                Console.WriteLine($"Test {i} -- Time Elapsed for Inserting {numOfRows} Rows: {timeConsumedSingleTest} ms");
            }

            float timeConsumedSingleTestAvg = ((float)timeConsumedAllTests) / ((float)numOfTestRun);
            float timeConsumedEachRowAvg = timeConsumedSingleTestAvg / ((float)numOfRows);

            Console.WriteLine($"Average Time Elapsed for Inserting {numOfRows} Rows: {timeConsumedSingleTestAvg} ms");
            Console.WriteLine($"Average Time Elapsed for Inserting Single Row: {timeConsumedEachRowAvg} ms");
        }

        private static long InsertTableTest(int count)
        {
            Console.WriteLine("InsertTableTest started....");

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            //RunNonQuery(connectionString, "drop table " + testTableName);
            //RunNonQuery(connectionString, "create table " + testTableName + " (id int, name VARCHAR(10), address CHAR(32))");

            Stopwatch stopwatch = new Stopwatch();
            long totalTimeConsumed = 0;

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        for (int i = 0; i < count; ++i)
                        {
                            string name = GetRandomString(10);
                            string address = GetRandomString(32);

                            command.CommandText = "INSERT INTO " + testTableName + " VALUES (" + i + ", '" + name + "', '" + address + "')";
                            stopwatch.Start();
                            command.ExecuteNonQuery();
                            stopwatch.Stop();
                            totalTimeConsumed += stopwatch.ElapsedMilliseconds;
                            stopwatch.Reset();
                        }
                    }
                }
            }
            catch
            {
                //RunNonQuery(connectionString, "drop table " + testTableName);
            }

            return totalTimeConsumed;
        }

        private static void RunSelectTableTests(int numOfTestRun, int numOfRows)
        {
            long timeConsumedAllTests = 0;

            for (int i = 0; i < numOfTestRun; ++i)
            {
                long timeConsumedSingleTest = SelectTableTest(numOfRows);
                timeConsumedAllTests += timeConsumedSingleTest;
                Console.WriteLine($"Test {i} -- Time Elapsed for Selecting {numOfRows} Rows: {timeConsumedSingleTest} ms");
            }

            float timeConsumedSingleTestAvg = ((float)timeConsumedAllTests) / ((float)numOfTestRun);
            float timeConsumedEachRowAvg = timeConsumedSingleTestAvg / ((float)numOfRows);

            Console.WriteLine($"Average Time Elapsed for Selecting {numOfRows} Rows: {timeConsumedSingleTestAvg} ms");
            Console.WriteLine($"Average Time Elapsed for Selecting Single Row: {timeConsumedEachRowAvg} ms");
        }

        private static long SelectTableTest(int count)
        {
            Console.WriteLine("SelectTableTest started....");

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            Stopwatch stopwatch = new Stopwatch();
            long totalTimeConsumed = 0;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                stopwatch.Start();
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = $"select TOP ({count}) * from {testTableName}";

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        int fieldCount = 0;
                        while (reader.Read())
                        {
                            if (fieldCount == 0)
                            {
                                fieldCount = reader.FieldCount;
                            }

                            for (int i = 0; i < fieldCount; ++i)
                            {
                                object temp = reader.GetSqlValue(i);
                            }
                        }
                    }
                    totalTimeConsumed = stopwatch.ElapsedMilliseconds;
                }
                stopwatch.Stop();
            }

            return totalTimeConsumed;
        }

        private static void RunNonQuery(string connectionString, string sql)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch
            {
                Console.WriteLine("SQL Skipped: " + sql);
            }
        }

        private static List<object[]> RunQuery(string connectionString, string sql)
        {
            List<object[]> result = null;
            try
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
                                    }
                                    result.Add(row);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("SQL Failed: " + sql);
                Console.WriteLine(e.Message);
            }

            return result;
        }

        private static string GetRandomString(int length)
        {
            string result = "";

            do
            {
                result += Guid.NewGuid().ToString("N");
            }
            while (result.Length < length);

            if (result.Length > length)
            {
                result = result.Substring(0, length);
            }

            return result;
        }
    }

    public class PerfTestWithThread
    {
        private class SelectWorker
        {
            private static ManualResetEvent _startEvent = new ManualResetEvent(false);
            private static int numOfThread = 0;

            private string _testTableName;
            private string _connString;
            private int _numOfTestToRun;
            private int _numOfRowsToSelect;

            private ManualResetEvent _doneEvent;
            private int _id;

            public SelectWorker(string connectionString, string tableName, int numOfTestToRun, int numOfRowsToSelect)
            {
                _connString = connectionString;
                _testTableName = tableName;
                _numOfTestToRun = numOfTestToRun;
                _numOfRowsToSelect = numOfRowsToSelect;
                _doneEvent = new ManualResetEvent(false);
                numOfThread++;
                _id = numOfThread;
            }

            public static ManualResetEvent StartEvent
            {
                get
                {
                    return _startEvent;
                }
            }

            public ManualResetEvent DoneEvent
            {
                get
                {
                    return _doneEvent;
                }
            }

            public void Run()
            {
                _startEvent.WaitOne();
                Console.WriteLine("Thread" + _id + " started!");
                RunSelectTableTests(_numOfTestToRun, _numOfRowsToSelect, _connString, _testTableName, _id);
                _doneEvent.Set();
            }

            private static void RunSelectTableTests(int numOfTestRun, int numOfRows, string connString, string tableName, int id)
            {
                long timeConsumedAllTests = 0;

                for (int i = 0; i < numOfTestRun; ++i)
                {
                    long timeConsumedSingleTest = SelectTableTest(numOfRows, connString, tableName);
                    timeConsumedAllTests += timeConsumedSingleTest;
                    Console.WriteLine($"Test {i} -- Time Elapsed for Selecting {numOfRows} Rows: {timeConsumedSingleTest} ms");
                }

                float timeConsumedSingleTestAvg = ((float)timeConsumedAllTests) / ((float)numOfTestRun);
                float timeConsumedEachRowAvg = timeConsumedSingleTestAvg / ((float)numOfRows);

                Console.WriteLine($"[Thread {id}]: Average Time Elapsed for Selecting {numOfRows} Rows: {timeConsumedSingleTestAvg} ms");
                Console.WriteLine($"[Thread {id}]: Average Time Elapsed for Selecting Single Row: {timeConsumedEachRowAvg} ms");
            }

            private static long SelectTableTest(int count, string connString, string tableName)
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
                string connectionString = builder.ConnectionString;

                Stopwatch stopwatch = new Stopwatch();
                long totalTimeConsumed = 0;

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    stopwatch.Start();
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = $"select TOP ({count}) * from {tableName}";

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            int fieldCount = 0;
                            while (reader.Read())
                            {
                                if (fieldCount == 0)
                                {
                                    fieldCount = reader.FieldCount;
                                }

                                for (int i = 0; i < fieldCount; ++i)
                                {
                                    object temp = reader.GetSqlValue(i);
                                }
                            }
                        }
                        totalTimeConsumed = stopwatch.ElapsedMilliseconds;
                    }
                    stopwatch.Stop();
                }

                return totalTimeConsumed;
            }
        }

        private static List<SelectWorker> workerList = new List<SelectWorker>();
        private static List<Thread> threadList = new List<Thread>();

        public static void Run(int numOfThreads, int numOfTestToRun, int numOfRowsToSelect)
        {
            RunSelectWorkers(numOfThreads, numOfTestToRun, numOfRowsToSelect);
        }

        private static void RunSelectWorkers(int numOfThreads, int numOfTestToRun, int numOfRowsToSelect)
        {
            for (int i = 0; i < numOfThreads; ++i)
            {
                SelectWorker worker = new SelectWorker(Secret.SqlClientConnectionString, Secret.testTableName, numOfTestToRun, numOfRowsToSelect);
                workerList.Add(worker);
                Thread childThread = new Thread(() => worker.Run());
                threadList.Add(childThread);
                childThread.Start();
            }

            SelectWorker.StartEvent.Set();

            foreach (SelectWorker worker in workerList)
            {
                worker.DoneEvent.WaitOne();
            }

            foreach (Thread childThread in threadList)
            {
                childThread.Join();
            }
        }
    }
}
