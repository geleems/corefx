using System.Data.Common;
using System.Reflection;
using Xunit;
using System.Net;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System;
using System.Threading;
using System.Collections;
using System.Transactions;

namespace System.Data.SqlClient.Tests
{
    public class GenesTest
    {
        public static void IntegratedAuthTest()
        {
            List<string> dataSources = new List<string>();
            dataSources.AddRange(GenerateHostNames());
            //dataSources.Add("\\\\.\\pipe\\sql\\query"); // \\.\pipe\sql\query --> MSSQLSvc/..redmond.corp.microsoft.com
            //dataSources.Add("\\\\.\\pipe\\MSSQL$INSTANCE1\\sql\\query"); // \\.\pipe\MSSQL$INSTANCE1\sql\query --> MSSQLSvc/..redmond.corp.microsoft.com:instance1
            //dataSources.Add("np:\\\\.\\pipe\\gene\\is\\awesome");
            //dataSources.Add("admin:.\\INSTANCE1");
            //dataSources.Add("admin:.");
            //dataSources.Add(".\\instance1");
            //dataSources.Add("admin:sausing-desktop\\myinstance");
            //dataSources.Add("");

            for (int i = 0; i < dataSources.Count; ++i)
            {
                PrintDataSourceInfo(dataSources[i]);

                string connectionString = MakeConnectionString(dataSources[i]);

                for (int k = 0; k < 1000; ++k)
                {
                    try
                    {
                        //System.GC.Collect();
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            connection.Close();
                            //connection.Dispose();
                        }
                    }
                    catch
                    {
                        Console.WriteLine("count: " + k);
                        throw;
                    }
                }

                Console.WriteLine("ConnectionString: " + connectionString + " --> Success!! haha");
            }
        }

        private static string MakeConnectionString(string dataSource)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = dataSource;
            builder.IntegratedSecurity = true;
            //builder.Pooling = true;
            return builder.ConnectionString;
        }

        private static void PrintDataSourceInfo(string dataSource)
        {
            Console.WriteLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            Console.WriteLine("  * DataSource: " + dataSource);

            string hostName = RemoveHeaderAndTail(dataSource.Replace(" ", "").Replace("\t", ""));
            Console.WriteLine("  * HostName: " + hostName);

            IPHostEntry hostEntry = Dns.GetHostEntry(hostName);
            Console.WriteLine("  * Full Qualified Domain Name: {0}", hostEntry.HostName);

            string connectionString = MakeConnectionString(dataSource);

            Console.WriteLine("  * ConnectionString: " + connectionString);
            Console.WriteLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }

        private static string RemoveHeaderAndTail(string str)
        {
            string result = RemoveProtocol(str);
            result = RemovePortOrInstanceName(result);
            return result;
        }

        private static string RemoveProtocol(string str)
        {
            string result = str;
            if (!String.IsNullOrEmpty(str))
            {
                string[] protocols = new string[] { "tcp:", "admin:", "np:" };
                for (int i = 0; i < protocols.Length; ++i)
                {
                    if (str.StartsWith(protocols[i]))
                    {
                        int start = protocols[i].Length;
                        result = (start == str.Length ? "" : str.Substring(start, str.Length - start));
                        break;
                    }
                }
            }
            return result;
        }

        private static string RemovePortOrInstanceName(string str)
        {
            string result = str;
            if (!String.IsNullOrEmpty(str))
            {
                int commaIndex = str.IndexOf(",");
                int backSlashIndex = str.IndexOf("\\");
                int index = (commaIndex >= 0 && backSlashIndex >= 0 ? Math.Min(commaIndex, backSlashIndex) : Math.Max(commaIndex, backSlashIndex));

                if (index >= 0)
                {
                    result = (index == 0 ? "" : str.Substring(0, index));
                }
            }
            return result;
        }


        private static List<string> GenerateHostNames()
        {
            bool isWindows = System.Environment.UserDomainName.Equals("REDMOND");

            string[] hostnames = new string[]{
                "..redmond.corp.microsoft.com",
                ".",
				//isWindows ? "2001:4898:f0:1025:64b5:fcf4:c08a:9f7a" : null,
				//isWindows ? "10.120.218.58" : null,
			};

            string[] protocols = new string[] {
                "",
                "tcp"
            };

            string[] instanceNames = new string[] {
                "",
				//"MSSQLSERVER",
				"INSTANCE1",
            };

            string[] ports = new string[] {
                "",
                "1433",
				//"1423",
			};

            HashSet<string> hostList = new HashSet<string>();
            for (int i = 0; i < hostnames.Length; ++i)
            {
                for (int j = 0; j < protocols.Length; ++j)
                {
                    for (int k = 0; k < instanceNames.Length; ++k)
                    {
                        if (hostnames[i] != null && protocols[j] != null && instanceNames[k] != null)
                        {
                            string currentHost = protocols[j] + (!"".Equals(protocols[j]) ? ":" : "") + hostnames[i] +
                                                    (!"".Equals(instanceNames[k]) ? "\\" : "") + instanceNames[k];
                            hostList.Add(currentHost);
                            Console.WriteLine("HostName added: " + currentHost);
                        }
                    }

                    for (int r = 0; r < ports.Length; ++r)
                    {
                        if (hostnames[i] != null && protocols[j] != null && ports[r] != null)
                        {
                            string currentHost = protocols[j] + (!"".Equals(protocols[j]) ? ":" : "") + hostnames[i] +
                                                    (!"".Equals(ports[r]) ? "," : "") + ports[r];
                            hostList.Add(currentHost);
                            Console.WriteLine("HostName added: " + currentHost);
                        }
                    }
                }
            }

            return hostList.ToList();
        }



        public static void MyBasicConnectionTest()
        {
            string connString = "";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {
                connection.Open();
            }
        }


        public static void QuickTest()
        {
            bool isWindows = System.Environment.UserDomainName.Equals("REDMOND");
            Console.WriteLine(">>>> isWindows: " + isWindows);
        }

        private static void PrintIsolationLevel(SqlConnection conn, SqlTransaction transaction = null)
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = @"SELECT CASE transaction_isolation_level 
						WHEN 0 THEN 'Unspecified' 
						WHEN 1 THEN 'ReadUncommitted' 
						WHEN 2 THEN 'ReadCommitted' 
						WHEN 3 THEN 'Repeatable' 
						WHEN 4 THEN 'Serializable' 
						WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL 
						FROM sys.dm_exec_sessions 
						where session_id = @@SPID";
            Console.WriteLine(" ====> " + cmd.ExecuteScalar());
        }


        public static void Test3()
        {
            Console.WriteLine("Test3");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;";// pooling=false";

            using (var c1 = new SqlConnection(connString))
            {
                c1.Open();
                using (var c2 = new SqlConnection(connString))
                {
                    c2.Open();

                    /*
					Exec(c2, "BEGIN TRANSACTION;");
					Exec(c2, "UPDATE Scratch SET ID=11 WHERE COL=11;");
					*/

                    /*
					using (var trans = c2.BeginTransaction())
					{
						using (var cmd = new SqlCommand("UPDATE Scratch SET ID=22 WHERE COL=22;", c2, trans))
						{
							cmd.ExecuteNonQuery();
						}
						// Console.WriteLine("------> transaction: " + (Transaction.Current==null ? "null" : "not null"));
						// trans.Commit();
					}
					*/

                    //Exec(c2, "COMMIT;");

                    var trans = c2.BeginTransaction();
                    using (var cmd = new SqlCommand("UPDATE Scratch SET ID=22 WHERE COL=22;", c2, trans))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                Exec(c1, "UPDATE Scratch SET ID=33 WHERE COL=33;");

                /*
				using (var trans = c1.BeginTransaction())
				{
					using (var cmd = new SqlCommand("UPDATE Scratch SET ID=4 WHERE COL=22;", c1, trans))
					{
						cmd.ExecuteNonQuery();
					}
					//trans.Commit();
				}
				*/
            }
        }

        private static void Exec(SqlConnection c, string s)
        {
            using (var m = c.CreateCommand())
            {
                m.CommandText = s;
                m.ExecuteNonQuery();
            }
        }


        public static void Test3_1()
        {
            Console.WriteLine("Test3_1");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;";// pooling=true; Max Pool Size=1; Min Pool Size=1";
            SqlConnection.ClearAllPools();

            var c = new SqlConnection(connString);
            c.Open();
            SqlTransaction trans = c.BeginTransaction();
            using (var cmd = new SqlCommand("UPDATE Scratch SET ID=11 WHERE COL=11;", c, trans))
            {
                cmd.ExecuteNonQuery();
            }

            c.Close();
            c.Open();

            SqlTransaction trans1 = c.BeginTransaction();
            using (var cmd = new SqlCommand("UPDATE Scratch SET ID=11 WHERE COL=11;", c, trans1))
            {
                cmd.ExecuteNonQuery();
            }
            trans1.Commit();
        }


        public static void Test4()
        {
            Console.WriteLine("Test4");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;";// pooling=false";

            SqlConnection.ClearAllPools();
            var conn = new SqlConnection(new SqlConnectionStringBuilder(connString).ConnectionString);

            Action<SqlTransaction> printIsolationLevel = (SqlTransaction transaction) =>
            {
                var cmd = conn.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = @"SELECT CASE transaction_isolation_level 
						WHEN 0 THEN 'Unspecified' 
						WHEN 1 THEN 'ReadUncommitted' 
						WHEN 2 THEN 'ReadCommitted' 
						WHEN 3 THEN 'Repeatable' 
						WHEN 4 THEN 'Serializable' 
						WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL 
						FROM sys.dm_exec_sessions 
						where session_id = @@SPID";
                Console.WriteLine(" ====> " + cmd.ExecuteScalar());
            };

            conn.Open();

            Console.Write("Before Serializable transaction");
            printIsolationLevel(null); // ReadCommitted

            using (var transaction = conn.BeginTransaction(System.Data.IsolationLevel.Serializable))
            {
                Console.Write("In Serializable transaction");
                printIsolationLevel(transaction); // Serializable

                Console.WriteLine("transaction.IsolationLevel: " + transaction.IsolationLevel);
            }

            Console.Write("After Serializable transaction");
            printIsolationLevel(null); // Serializable

            using (var transaction = conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
            {
                Console.Write("In x transaction");
                printIsolationLevel(transaction); // Serializable
            }

            Console.Write("After x transaction");
            printIsolationLevel(null); // Serializable

            conn.Close();
        }


        public static void Test5()
        {
            Console.WriteLine("Test5");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;";// pooling=false";

            SqlConnection.ClearAllPools();
            SqlConnection conn1 = new SqlConnection(new SqlConnectionStringBuilder(connString).ConnectionString);
            SqlConnection conn2 = new SqlConnection(new SqlConnectionStringBuilder(connString).ConnectionString);
            conn1.Open();
            conn2.Open();

            using (var transaction1 = conn1.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
            {
                using (var cmd = new SqlCommand("select * from Scratch;", conn1, transaction1))
                {
                    SqlDataReader reader = cmd.ExecuteReader();
                    reader.Close();
                }

                using (var cmd = conn2.CreateCommand())
                {
                    cmd.CommandText = "UPDATE Scratch SET ID=22 WHERE COL=22;";
                    cmd.ExecuteNonQuery();
                }
            }

            conn1.Close();
            conn2.Close();
        }


        public static void Test6()
        {
            Console.WriteLine("Test6");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;";

            SqlConnection.ClearAllPools();
            SqlConnection conn1 = new SqlConnection(new SqlConnectionStringBuilder(connString).ConnectionString);
            SqlConnection conn2 = new SqlConnection(new SqlConnectionStringBuilder(connString).ConnectionString);
            conn1.Open();
            conn2.Open();

            using (var transaction2 = conn2.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
            {

            }

            using (var transaction1 = conn1.BeginTransaction())
            {
                using (var cmd = new SqlCommand("UPDATE Scratch SET ID=22 WHERE COL=22;", conn1, transaction1))
                {
                    SqlDataReader reader = cmd.ExecuteReader();
                    reader.Close();
                }

                using (var cmd = conn2.CreateCommand())
                {
                    cmd.CommandText = "select * from Scratch;";
                    var reader = cmd.ExecuteReader(); // should hang here...
                    reader.Close();
                }
            }

            conn1.Close();
            conn2.Close();
        }


        public static void Test7()
        {
            Console.WriteLine("Test7");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;";
            SqlConnection conn = new SqlConnection(new SqlConnectionStringBuilder(connString).ConnectionString);
            conn.Open();
            conn.BeginTransaction(System.Data.IsolationLevel.Serializable).Dispose();
            conn.Close();
            conn.Open();
            PrintIsolationLevel(conn);
            conn.Dispose();
        }


        public static void Test8___()
        {
            Console.WriteLine("Test8");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;pooling=true;Enlist=true";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            using (TransactionScope txScope = new TransactionScope())
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    /*
					try
					{
						using (SqlCommand command = connection.CreateCommand())
						{
							command.CommandText = "drop table mytable";
							command.ExecuteNonQuery();
						}
					}
					catch { }
					using (SqlCommand command = connection.CreateCommand())
					{
						command.CommandText = "create table mytable (col1 text, col2 text)";
						command.ExecuteNonQuery();
					}
					using (SqlCommand command = connection.CreateCommand())
					{
						command.CommandText = "INSERT INTO mytable VALUES ('11', '22')";
						command.ExecuteNonQuery();
					}
					*/
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "INSERT INTO mytable VALUES ('33', '44')";
                        command.ExecuteNonQuery();
                    }
                }
                txScope.Complete();
            }
        }


        public static void Test9()
        {
            Console.WriteLine("Test9");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;pooling=true";

            using (var connection = new SqlConnection(connString))
            {
                connection.Open();
                var trans = connection.BeginTransaction();
                using (var cmd = new SqlCommand("INSERT INTO mytable VALUES ('qq', 'ww')", connection, trans))
                {
                    cmd.ExecuteNonQuery();
                }
                trans.Commit();
            }
        }


        public static void Test10()
        {
            Console.WriteLine("Test10");
            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Connect Timeout=600;pooling=true;Enlist=true";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            using (TransactionScope txScope = new TransactionScope())
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    /*
                    try
                    {
                        using (SqlCommand command = connection.CreateCommand())
                        {
                            command.CommandText = "drop table mytable";
                            command.ExecuteNonQuery();
                        }
                    }
                    catch { }
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "create table mytable (col1 text, col2 text)";
                        command.ExecuteNonQuery();
                    }
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "INSERT INTO mytable VALUES ('11', '22')";
                        command.ExecuteNonQuery();
                    }
                    */
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "INSERT INTO mytable VALUES ('33', '44')";
                        command.ExecuteNonQuery();
                    }
                }
                txScope.Complete();
            }
        }


        public static void Test11()
        {
            Console.WriteLine("Test11");

            string connString = @"Server=tcp:.;User ID=testuser;Password=test1234;Connect Timeout=5";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            connString = builder.ConnectionString;

            using (var connection = new SqlConnection(connString))
            {
                connection.Open();
                Console.WriteLine("Yo!!");
            }
        }

        

        class MyEnlistmentClass : IEnlistmentNotification
        {
            public void Prepare(PreparingEnlistment preparingEnlistment)
            {
                Console.WriteLine("Prepare notification received");

                //Perform transactional work

                //If work finished correctly, reply prepared
                preparingEnlistment.Prepared();

                // otherwise, do a ForceRollback
                preparingEnlistment.ForceRollback();
            }

            public void Commit(Enlistment enlistment)
            {
                Console.WriteLine("Commit notification received");

                //Do any work necessary when commit notification is received

                //Declare done on the enlistment
                enlistment.Done();
            }

            public void Rollback(Enlistment enlistment)
            {
                Console.WriteLine("Rollback notification received");

                //Do any work necessary when rollback notification is received

                //Declare done on the enlistment
                enlistment.Done();
            }

            public void InDoubt(Enlistment enlistment)
            {
                Console.WriteLine("In doubt notification received");

                //Do any work necessary when indout notification is received

                //Declare done on the enlistment
                enlistment.Done();
            }
        }

        /*
        public static void Test12()
        {
            Console.WriteLine("Test12");

            string connString = "Server=tcp:.;User ID=testuser;Password=test1234;Connect Timeout=5;pooling=true;Enlist=true";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            string testTableName = "MyTestTable";
            RunNonQuery(connectionString, $"create table {testTableName} (col1 int, col2 text)");
            RunNonQuery(connectionString, $"truncate table {testTableName}");

            try
            {
                CommittableTransaction ct = new CommittableTransaction();
                MyEnlistmentClass myEnlistment = new MyEnlistmentClass();
                ct.EnlistVolatile(myEnlistment, EnlistmentOptions.None);
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    connection.EnlistTransaction(ct);
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = $"INSERT INTO {testTableName} VALUES (1, 'one')";
                        command.ExecuteNonQuery();
                    }
                    ct.Commit();
                }
                Console.WriteLine("successful");
            }
            finally
            {
                //RunNonQuery(connectionString, $"drop table {testTableName}");
            }
        }
        */


        public static void TestTransaction()
        {
            Console.WriteLine("Test started.");

            string connString = "Server=tcp:.;User ID=testuser;Password=test1234";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            CommittableTransaction ct = new CommittableTransaction();
            MyEnlistmentClass myEnlistment = new MyEnlistmentClass();
            ct.EnlistVolatile(myEnlistment, EnlistmentOptions.None);
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            connection.EnlistTransaction(ct);

            Console.WriteLine("successful");
        }

        public static void TestAmbiantTransaction_EnlistFalse()
        {
            Console.WriteLine("TestAmbiantTransaction_EnlistFalse");

            string connString = "Server=tcp:.,1433;User ID=testuser;Password=test1234;Enlist=false";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connString);
            string connectionString = builder.ConnectionString;

            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();

            using (TransactionScope txScope = new TransactionScope())
            {
                connection.EnlistTransaction(Transaction.Current);
                try
                {
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "drop table mytable";
                        command.ExecuteNonQuery();
                    }
                }
                catch { }
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "create table mytable (col1 text, col2 text)";
                    command.ExecuteNonQuery();
                }
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "INSERT INTO mytable VALUES ('11', '22')";
                    command.ExecuteNonQuery();
                }

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "INSERT INTO mytable VALUES ('33', '44')";
                    command.ExecuteNonQuery();
                }
            }
        }
        

        [Fact]
        public static void TestMain()
        {
            //PerformaceTest.Run();
            //AsyncPerformaceTest.Run();
            //ReadAsyncTest.Run();
            //PerformanceTestWithThreads.MainRun();

            ConnectionPoolingTest.TestMain();
        }
    }
}
