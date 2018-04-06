// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Data.SqlClient;
using System.Text;
using Xunit;

namespace System.Data.Odbc.Tests
{
    public class CustomerRepro
    {
        private const string uid = "testuser";
        private const string pwd = "mytest1234!";
        private static string OdbcConnStr = "server=tcp:.;Driver={SQL Server};" + $"uid={uid};pwd={pwd}";
        
        public static void RunTest()
        {
            string inputStr = "";
            for (int i = 0; i < 10000; ++i)
            {
                inputStr += "0123456789";
            }

            string procName = "VarcharIssueReproProc";

            string removeExistingStoredProcSql =
                $"IF OBJECT_ID('{procName}', 'P') IS NOT NULL " +
                    $"DROP PROCEDURE {procName};";
            
            string createTestStoredProcSql =
                $"CREATE PROCEDURE {procName} (" +
                    $"@v1 VARCHAR(MAX), " +
                    $"@v2 VARCHAR(MAX) OUT, " +
                    "@v3 INTEGER OUT, " +
                    "@v4 INTEGER OUT) " +
                "AS BEGIN " +
                    "SET @v2 = @v1 + @v1; " +
                    "SET @v3 = datalength(@v1); " +
                    "SET @v4 = datalength(@v2); " +
                "END;";

            object v1 = inputStr;
            object v2 = null;
            int v3 = 0;
            int v4 = 0;

            try
            {
                RunNonQuery(OdbcConnStr, removeExistingStoredProcSql);
                RunNonQuery(OdbcConnStr, createTestStoredProcSql);

                using (OdbcConnection odbcConnection = new OdbcConnection(OdbcConnStr))
                {
                    using (OdbcCommand command = new OdbcCommand("{ call " + procName + "(?,?,?,?) }", odbcConnection))
                    {
                        odbcConnection.Open();

                        command.Parameters.Clear();
                        command.CommandType = CommandType.StoredProcedure;

                        command.Parameters.Add("@v1", OdbcType.VarChar, v1.ToString().Length);
                        command.Parameters.Add("@v2", OdbcType.VarChar, 8000);
                        command.Parameters.Add("@v3", OdbcType.Int);
                        command.Parameters.Add("@v4", OdbcType.Int);

                        command.Parameters["@v1"].Direction = ParameterDirection.Input;
                        command.Parameters["@v2"].Direction = ParameterDirection.Output;
                        command.Parameters["@v3"].Direction = ParameterDirection.Output;
                        command.Parameters["@v4"].Direction = ParameterDirection.Output;

                        command.Parameters["@v1"].Value = v1;
                        command.ExecuteNonQuery();

                        v2 = command.Parameters["@v2"].Value;
                        v3 = Int32.Parse(command.Parameters["@v3"].Value.ToString());
                        v4 = Int32.Parse(command.Parameters["@v4"].Value.ToString());
                    }
                }

                Console.WriteLine("Result:\n");
                Console.WriteLine("v1: " + v1 + "\n");
                Console.WriteLine("v2: " + v2 + "\n");
                Console.WriteLine("v3: " + v3 + "\n");
                Console.WriteLine("v4: " + v4 + "\n");
                Console.WriteLine("v2.ToString().Length: " + v2.ToString().Length + "\n");
            }
            finally
            {
                RunNonQuery(OdbcConnStr, removeExistingStoredProcSql);
            }
        }

        public static void RunNonQuery(string connectionString, string sql)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                using (OdbcCommand command = new OdbcCommand(sql, connection))
                {
                    connection.Open();
                    command.ExecuteNonQuery();
                }
            }
        }
    }
    
    public class MyWorkaround1
    {
        private const string uid = "testuser";
        private const string pwd = "mytest1234!";
        private static string OdbcConnStr = "server=tcp:.;Driver={SQL Server};" + $"uid={uid};pwd={pwd}";

        public static void RunTest()
        {
            string inputStr = "";
            for (int i = 0; i < 10000; ++i)
            {
                inputStr += "0123456789";
            }

            string procName = "VarcharIssueReproProc";

            string removeExistingStoredProcSql =
                $"IF OBJECT_ID('{procName}', 'P') IS NOT NULL " +
                    $"DROP PROCEDURE {procName};";
            
            string createTestStoredProcSql =
                $"CREATE PROCEDURE {procName} (" +
                    "@word VARCHAR(MAX), " +
                    "@repeat INT = 1, " +
                    "@result VARCHAR(MAX) OUTPUT) " +
                "AS BEGIN " +
                    "SET NOCOUNT ON; " +
                    //"DECLARE @temp VARCHAR(MAX); " +
                    //"SET @temp = 'HaHaHaha!'; " +
                    "SET @result = REPLICATE(@word, @repeat); " +
                "END;";

            try
            {
                RunNonQuery(OdbcConnStr, removeExistingStoredProcSql);
                RunNonQuery(OdbcConnStr, createTestStoredProcSql);

                using (OdbcConnection odbcConnection = new OdbcConnection(OdbcConnStr))
                {
                    odbcConnection.Open();
                    using (OdbcCommand command = new OdbcCommand())
                    {
                        command.Connection = odbcConnection;
                        command.CommandType = System.Data.CommandType.Text;
                        command.CommandText =
                            "SET NOCOUNT ON; " +
                            "DECLARE @out VARCHAR(MAX); " +
                            $"EXEC {procName} @word=?, @repeat=?, @result=@out OUTPUT; " +
                            "SELECT @out;";
                        command.Parameters.Add("?", OdbcType.VarChar).Value = inputStr;//"HIHI~!!";
                        command.Parameters.Add("?", OdbcType.Int).Value = 2;
                        string result = command.ExecuteScalar().ToString();
                        Console.WriteLine("{0} characters were returned", result.Length);
                        //Console.WriteLine("result: {0}", result);
                    }
                }
            }
            finally
            {
                RunNonQuery(OdbcConnStr, removeExistingStoredProcSql);
            }
        }

        public static void RunNonQuery(string connectionString, string sql)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                using (OdbcCommand command = new OdbcCommand(sql, connection))
                {
                    connection.Open();
                    command.ExecuteNonQuery();
                }
            }
        }
    }
    
    public class MyWorkaround2
    {
        private const string uid = "testuser";
        private const string pwd = "mytest1234!";
        private static string OdbcConnStr = "server=tcp:.;Driver={SQL Server};" + $"uid={uid};pwd={pwd}";

        public static void RunTest()
        {
            string inputStr = "";
            for (int i = 0; i < 10000; ++i)
            {
                inputStr += "0123456789";
            }

            string procName = "VarcharIssueReproProc";

            string removeExistingStoredProcSql =
                $"IF OBJECT_ID('{procName}', 'P') IS NOT NULL " +
                    $"DROP PROCEDURE {procName};";
            
            string createTestStoredProcSql =
                $"CREATE PROCEDURE {procName} (" +
                    "@v1 VARCHAR(MAX), " +
                    "@v2 VARCHAR(MAX) OUTPUT, " +
                    "@v3 INT OUTPUT, " +
                    "@v4 INT OUTPUT) " +
                "AS BEGIN " +
                    "SET @v2 = @v1 + @v1; " +
                    "SET @v3 = DATALENGTH(@v1); " +
                    "SET @v4 = DATALENGTH(@v2); " +
                "END;";

            try
            {
                RunNonQuery(OdbcConnStr, removeExistingStoredProcSql);
                RunNonQuery(OdbcConnStr, createTestStoredProcSql);

                using (OdbcConnection odbcConnection = new OdbcConnection(OdbcConnStr))
                {
                    odbcConnection.Open();
                    using (OdbcCommand command = new OdbcCommand())
                    {
                        command.Connection = odbcConnection;
                        command.CommandType = CommandType.Text;
                        command.CommandText =
                            "SET NOCOUNT ON; " +
                            "DECLARE @out_v2 VARCHAR(MAX); " +
                            "DECLARE @out_v3 INT; " +
                            "DECLARE @out_v4 INT; " +
                            $"EXEC {procName} @v1=?, @v2=@out_v2 OUTPUT, @v3=@out_v3 OUTPUT, @v4=@out_v4 OUTPUT; " +
                            "SELECT @out_v2, @out_v3, @out_v4;";

                        command.Parameters.Add("?", OdbcType.VarChar).Value = inputStr;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                int numOfFields = reader.FieldCount;
                                reader.Read();
                                for (int i = 0; i < numOfFields; ++i)
                                {
                                    Console.WriteLine($"output v{i+2}: "+ reader[i]);
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                RunNonQuery(OdbcConnStr, removeExistingStoredProcSql);
            }
        }

        public static void RunNonQuery(string connectionString, string sql)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                using (OdbcCommand command = new OdbcCommand(sql, connection))
                {
                    connection.Open();
                    command.ExecuteNonQuery();
                }
            }
        }
    }

    public static class OdbcParameterTests
    {
        [Fact]
        public static void Run()
        {
            Program.RunTest();
        }


        //[CheckConnStrSetupFact]
        public static void RunTest()
        {
            string str1000 = null;
            string str2000 = null;
            string str4000 = null;
            string str5000 = null;
            string str8000 = "";
            for (int i = 0; i < 800; i++)
            {
                str8000 += "0123456789";
                if (i == 99)
                {
                    str1000 = str8000;
                }
                else if (i == 199)
                {
                    str2000 = str8000;
                }
                else if (i == 399)
                {
                    str4000 = str8000;
                }
                else if (i == 499)
                {
                    str5000 = str8000;
                }
            }

            byte[] byte1000 = Encoding.ASCII.GetBytes(str1000);
            byte[] byte2000 = Encoding.ASCII.GetBytes(str2000);
            byte[] byte4000 = Encoding.ASCII.GetBytes(str4000);
            byte[] byte5000 = Encoding.ASCII.GetBytes(str5000);
            byte[] byte8000 = Encoding.ASCII.GetBytes(str8000);

            object output = null;
            int inputLength = 0;
            int outputLength = 0;

            RunTestProcedure("VARBINARY", 8000, byte8000, out output, out inputLength, out outputLength);
            string outputStr = Encoding.ASCII.GetString(output as byte[]);
            Assert.Equal(str8000, outputStr);
            Assert.Equal(byte8000.Length, inputLength);
            Assert.Equal(byte8000.Length, outputLength);

            RunTestProcedure("VARBINARY", 8000, byte5000, out output, out inputLength, out outputLength);
            outputStr = Encoding.ASCII.GetString(output as byte[]);
            Assert.Equal(str8000, outputStr);
            Assert.Equal(byte5000.Length, inputLength);
            Assert.Equal(byte8000.Length, outputLength);

            RunTestProcedure("VARBINARY", 8000, byte4000, out output, out inputLength, out outputLength);
            outputStr = Encoding.ASCII.GetString(output as byte[]);
            Assert.Equal(str8000, outputStr);
            Assert.Equal(byte4000.Length, inputLength);
            Assert.Equal(str8000.Length, outputLength);

            RunTestProcedure("VARBINARY", 8000, byte2000, out output, out inputLength, out outputLength);
            outputStr = Encoding.ASCII.GetString(output as byte[]);
            Assert.Equal(str4000, outputStr);
            Assert.Equal(byte2000.Length, inputLength);
            Assert.Equal(str4000.Length, outputLength);

            RunTestProcedure("VARBINARY", 8000, byte1000, out output, out inputLength, out outputLength);
            outputStr = Encoding.ASCII.GetString(output as byte[]);
            Assert.Equal(str2000, outputStr);
            Assert.Equal(byte1000.Length, inputLength);
            Assert.Equal(byte2000.Length, outputLength);

            RunTestProcedure("VARCHAR", 8000, str8000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str8000, outputStr);
            Assert.Equal(str8000.Length, inputLength);
            Assert.Equal(str8000.Length, outputLength);

            RunTestProcedure("VARCHAR", 8000, str5000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str8000, outputStr);
            Assert.Equal(str5000.Length, inputLength);
            Assert.Equal(str8000.Length, outputLength);

            RunTestProcedure("VARCHAR", 8000, str4000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str8000, outputStr);
            Assert.Equal(str4000.Length, inputLength);
            Assert.Equal(str8000.Length, outputLength);

            RunTestProcedure("VARCHAR", 8000, str2000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str4000, outputStr);
            Assert.Equal(str2000.Length, inputLength);
            Assert.Equal(str4000.Length, outputLength);

            RunTestProcedure("VARCHAR", 8000, str1000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str2000, outputStr);
            Assert.Equal(str1000.Length, inputLength);
            Assert.Equal(str2000.Length, outputLength);

            RunTestProcedure("NVARCHAR", 4000, str8000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str4000, outputStr);
            Assert.Equal(str4000.Length * 2, inputLength); // since NVARCHAR takes 2 bytes per character
            Assert.Equal(str4000.Length * 2, outputLength);

            RunTestProcedure("NVARCHAR", 4000, str5000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str4000, outputStr);
            Assert.Equal(str4000.Length * 2, inputLength);
            Assert.Equal(str4000.Length * 2, outputLength);

            RunTestProcedure("NVARCHAR", 4000, str4000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str4000, outputStr);
            Assert.Equal(str4000.Length * 2, inputLength);
            Assert.Equal(str4000.Length * 2, outputLength);

            RunTestProcedure("NVARCHAR", 4000, str2000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str4000, outputStr);
            Assert.Equal(str2000.Length * 2, inputLength);
            Assert.Equal(str4000.Length * 2, outputLength);

            RunTestProcedure("NVARCHAR", 4000, str1000, out output, out inputLength, out outputLength);
            outputStr = output as string;
            Assert.Equal(str2000, outputStr);
            Assert.Equal(str1000.Length * 2, inputLength);
            Assert.Equal(str2000.Length * 2, outputLength);
        }

        private static void RunTestProcedure(string procDataType, int procDataSize, object v1, out object v2, out int v3, out int v4)
        {
            string procName = DataTestUtility.GetUniqueName("ODBCTEST", "", "");

            string removeExistingStoredProcSql =
                $"IF OBJECT_ID('{procName}', 'P') IS NOT NULL " +
                    $"DROP PROCEDURE {procName};";

            string createTestStoredProcSql =
                $"CREATE PROCEDURE {procName} (" +
                    $"@v1 {procDataType}({procDataSize}), " +
                    $"@v2 {procDataType}({procDataSize}) OUT, " +
                    "@v3 INTEGER OUT, " +
                    "@v4 INTEGER OUT) " +
                "AS BEGIN " +
                    "SET @v2 = @v1 + @v1; " +
                    "SET @v3 = datalength(@v1); " +
                    "SET @v4 = datalength(@v2); " +
                "END;";

            try
            {
                DataTestUtility.RunNonQuery(DataTestUtility.OdbcConnStr, removeExistingStoredProcSql);
                DataTestUtility.RunNonQuery(DataTestUtility.OdbcConnStr, createTestStoredProcSql);

                DbAccessor dbAccessUtil = new DbAccessor();
                dbAccessUtil.connectSqlServer(DataTestUtility.OdbcConnStr);
                dbAccessUtil.callProc("{ call "+ procName+"(?,?,?,?) }", procDataType, procDataSize, v1, out v2, out v3, out v4);
                dbAccessUtil.commit();
                dbAccessUtil.disconnect();
            }
            finally
            {
                DataTestUtility.RunNonQuery(DataTestUtility.OdbcConnStr, removeExistingStoredProcSql);
            }
        }

        private class DbAccessor
        {
            private OdbcConnection con = null;
            private OdbcTransaction trn = null;

            public bool connectSqlServer(string connStr)
            {
                if (con == null)
                {
                    con = new OdbcConnection(connStr);
                }

                con.Open();
                trn = con.BeginTransaction();

                return true;
            }

            public void disconnect()
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

            public void callProc(string sql, string procDataType, int procDataSize, object v1, out object v2, out int v3, out int v4)
            {
                using (OdbcCommand command = new OdbcCommand(sql, con, trn))
                {
                    command.Parameters.Clear();
                    command.CommandType = CommandType.StoredProcedure;

                    OdbcType dataType = OdbcType.NVarChar;
                    switch (procDataType.ToUpper())
                    {
                        case "VARBINARY":
                            dataType = OdbcType.VarBinary;
                            break;
                        case "VARCHAR":
                            dataType = OdbcType.VarChar;
                            break;
                    }
                    
                    command.Parameters.Add("@v1", dataType, procDataSize);
                    command.Parameters.Add("@v2", dataType, procDataSize);
                    command.Parameters.Add("@v3", OdbcType.Int);
                    command.Parameters.Add("@v4", OdbcType.Int);

                    command.Parameters["@v1"].Direction = ParameterDirection.Input;
                    command.Parameters["@v2"].Direction = ParameterDirection.Output;
                    command.Parameters["@v3"].Direction = ParameterDirection.Output;
                    command.Parameters["@v4"].Direction = ParameterDirection.Output;

                    command.Parameters["@v1"].Value = v1;
                    command.ExecuteNonQuery();

                    v2 = command.Parameters["@v2"].Value;
                    v3 = Int32.Parse(command.Parameters["@v3"].Value.ToString());
                    v4 = Int32.Parse(command.Parameters["@v4"].Value.ToString());
                }
            }

            public bool commit()
            {
                if (trn == null)
                {
                    return false;
                }
                trn.Commit();
                trn = null;
                return true;
            }

            public bool rollback()
            {
                if (trn == null)
                {
                    return false;
                }
                trn.Rollback();
                trn = null;
                return true;
            }
        }
    }
}
