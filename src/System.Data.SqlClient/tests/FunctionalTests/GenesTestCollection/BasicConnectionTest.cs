using System.Collections.Generic;
using System.Data.Odbc;
using System.Threading;

namespace System.Data.SqlClient.Tests
{
    public class BasicConnectionTest
    {
        public static void Run()
        {
            TestRun();
        }

        static void TestRun()
        {
            //try
            {
                string clobstr = "";
                for (int i = 0; i < 800; i++)
                {
                    clobstr += "0123456789";
                }
                string v1 = clobstr;
                string v2 = "";

                Console.WriteLine("====!!!!: "+System.Text.ASCIIEncoding.ASCII.GetByteCount(v1));

                int v3 = 0;
                int v4 = 0;

                DbAccessor dbUtil = new DbAccessor();
                dbUtil.connectSqlServer("testuser", "mytest1234", "sampleDB"); //shared sample DB

                dbUtil.callProc("{ call testCSProc1(?,?,?,?) }", v1, v2, v3, v4);
                dbUtil.callProc("{ call testCSProc2(?,?,?,?) }", v1, v2, v3, v4);
                dbUtil.commit();
                dbUtil.disconnect();
            }
            /*
            catch (Exception e)
            {
                Console.Write(e.Message);
            }
            */
        }

        private static class ConnPoolFragTest
        {
            private static int numOfThread = 100;

            public static void TestRun()
            {
                List<Thread> threadList = new List<Thread>();

                for (int i = 0; i < numOfThread; ++i)
                {
                    Thread myThead = new Thread(new ThreadStart(ThreadRun));
                    myThead.Start();

                    // Loop until worker thread activates.
                    while (!myThead.IsAlive);

                    threadList.Add(myThead);
                }

                foreach(Thread t in threadList)
                {
                    // Use the Join method to block the current thread until the object's thread terminates.
                    t.Join();
                }
            }

            private static void ThreadRun()
            {
                SqlConnection con = new SqlConnection("Server=tcp:.;User ID=testuser;Password=test1234");
                con.Open();
                con.Dispose();
            }
        }
        

        private static class ConnHangTest
        {
            private static string GetConnStr(bool mirroring)
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
                builder.DataSource = (mirroring ? "tcp:." : "tcp:fxbvt4,1432");
                builder.InitialCatalog = (mirroring ? "GeneDB" : "Northwind");
                builder.IntegratedSecurity = true;
                builder.ConnectTimeout = 0;
                if (mirroring)
                {
                    builder.FailoverPartner = "tcp:.\\instance1";
                }

                return  builder.ConnectionString;
            }

            public static void TestRun()
            {
                string connStr = GetConnStr(true);

                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connStr);
                string db = builder.InitialCatalog;

                DataTable dt = TestUtils.RunQuery(connStr, $"select mirroring_state from sys.database_mirroring where database_id = DB_ID('{db}')");
                int value = Int32.Parse(dt.Rows[0][0].ToString());
                Console.WriteLine("dt.Rows[0][0]: " + value);

                dt = TestUtils.RunQuery(connStr, $"select mirroring_partner_name from sys.database_mirroring where database_id = DB_ID('{db}')");
                Console.WriteLine("dt.Rows[0][0]: " + dt.Rows[0][0]);

                SqlConnection conn1 = new SqlConnection(connStr);
                conn1.Open();
                Console.WriteLine("opened 1");
                SqlConnection conn2 = new SqlConnection(connStr);
                conn2.Open();
                Console.WriteLine("opened 2");
            }
        }
    }



    class DbAccessor
    {
        private OdbcConnection con = null;
        private OdbcTransaction trn = null;

        public bool connectSqlServer(string UserName, string PassWord, string dbName)
        {
            //try
            {
                if (con == null)
                {
                    con = new OdbcConnection();
                }

                //OdbcConnectionStringBuilder builder = new OdbcConnectionStringBuilder();

                string cstr = "";
                cstr += ";Server={localhost}";
                cstr += ";UID=" + UserName;
                cstr += ";PWD=" + PassWord;
                cstr += ";Database=" + dbName;
                cstr += ";Driver={SQL Server}";
                cstr += ";MARS_Connection=yes";
                Console.WriteLine("Connection String: " + cstr);

                con.ConnectionString = cstr;
                con.ConnectionTimeout = 0;
                Console.WriteLine("Connection Opening.");
                con.Open();
                Console.WriteLine("Connection Opened successfully.");

                trn = con.BeginTransaction();

                printLog("timeout = " + con.ConnectionTimeout.ToString());
                printLog("Database = " + con.Database.ToString());
                printLog("DataSource = " + con.DataSource.ToString());
                printLog("Server version = " + con.ServerVersion.ToString());
                printLog("Iso level = " + trn.IsolationLevel.ToString());
            }
            /*
            catch (InvalidOperationException e)
            {
                printLog("Connection failed.");
                printLog(e.Message);
                return false;
            }
            catch (OdbcException e)
            {
                printLog("Connection failed.");
                outputOdbcException(e);
                return false;
            }
            catch (Exception e)
            {
                printLog("Connection failed.");
                printLog("Unexpected error : " + e.Message);
                throw e;
            }
            */
            return true;
        }

        public void disconnect()
        {
            //try
            {
                if (trn != null)
                {
                    trn.Rollback();
                    trn.Dispose();
                    trn = null;
                }
                if (con != null)
                {
                    con.Close();
                    con.Dispose();
                    con = null;
                }
            }
            /*
            catch (Exception e)
            {
                printLog("Unexpected error: " + e.Message);
            }
            */
        }

        public void callProc(string sql, string v1, string v2, int v3, int v4)
        {
            //try
            {
                if (con == null || con.State != ConnectionState.Open)
                {
                    printLog("Connection status error: ConnectionState -- " + con.State.ToString());
                }

                OdbcCommand command = new OdbcCommand(sql, con, trn);
                command.Parameters.Clear();
                command.CommandType = CommandType.StoredProcedure;

                command.Parameters.Add("@v1", OdbcType.VarChar, 8000); // Input
                command.Parameters.Add("@v2", OdbcType.VarChar, 8000); //Output. The error will not occur when 4000 is 3999.

                command.Parameters.Add("@v3", OdbcType.Int);
                command.Parameters.Add("@v4", OdbcType.Int);
                command.Parameters["@v1"].Direction = ParameterDirection.Input;
                command.Parameters["@v2"].Direction = ParameterDirection.Output;
                command.Parameters["@v3"].Direction = ParameterDirection.InputOutput;
                command.Parameters["@v4"].Direction = ParameterDirection.InputOutput;
                command.Parameters["@v1"].Value = v1;
                command.Parameters["@v2"].Value = v2;
                command.Parameters["@v3"].Value = v3;
                command.Parameters["@v4"].Value = v4;

                command.ExecuteNonQuery();

                printLog("================================================================");
                printLog("v1 value = " + command.Parameters["@v1"].Value.ToString());
                printLog("v1 length = " + command.Parameters["@v1"].Value.ToString().Length);
                printLog("v2 value = " + command.Parameters["@v2"].Value.ToString());
                printLog("v2 length = " + command.Parameters["@v2"].Value.ToString().Length);
                printLog("v3 value  = " + command.Parameters["@v3"].Value.ToString());
                printLog("v4 value  = " + command.Parameters["@v4"].Value.ToString());
                printLog("================================================================");
            }
            /*
            catch (InvalidOperationException e)
            {
                printLog("SQL Failed to execute.");
                printLog(e.Message);
            }
            catch (OdbcException e)
            {
                outputOdbcException(e);
            }
            catch (Exception e)
            {
                printLog("Unexpected error : " + e.Message);
            }
            */
        }

        public bool commit()
        {
            //try
            {
                if (trn == null)
                {
                    return false;
                }
                trn.Commit();
                //trn = con.BeginTransaction();
            }
            /*
            catch (InvalidOperationException e)
            {
                printLog("Transaction processing failed");
                printLog(e.Message);
                return false;
            }
            catch (OdbcException e)
            {
                outputOdbcException(e);
                trn = null;
                return false;
            }
            catch (Exception e)
            {
                printLog("Unexpected error : " + e.Message);
                trn = null;
                return false;
            }
            */
            trn = null;
            return true;

        }

        public bool rollback()
        {
            //try
            {
                if (trn == null)
                {
                    return false;
                }
                trn.Rollback();
                //trn = con.BeginTransaction();
            }
            /*
            catch (InvalidOperationException e)
            {
                printLog("Transaction processing failed");
                printLog(e.Message);
                return false;
            }
            catch (OdbcException e)
            {
                outputOdbcException(e);
                trn = null;
                return false;
            }
            catch (Exception e)
            {
                printLog("Unexpected error : " + e.Message);
                trn = null;
                return false;
            }
            */
            trn = null;
            return true;
        }

        void outputOdbcException(OdbcException e)
        {
            for (int i = 0; i < e.Errors.Count; i++)
            {
                printLog("[" + i.ToString() + "] " + e.Errors[i].Message + ", " + e.Errors[i].NativeError.ToString() + ", " + e.Errors[i].Source + ", " + e.Errors[i].SQLState + ", " + e.ErrorCode + ", " + e.Message);
            }
        }

        void printLog(string val)
        {
            Console.Write(val + System.Environment.NewLine);
        }
    }
}
