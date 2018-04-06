
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Threading;

namespace System.Data.SqlClient.Tests
{
    public static class Log
    {
        private static bool enabled = false;
        private static HashSet<string> whiteList = new HashSet<string>();
        private static string[] whiteListIdStrings = new string[]
        {
        };

        static Log()
        {
            foreach (string id in whiteListIdStrings)
            {
                whiteList.Add(id);
            }
        }

        private static bool ShouldProceed(string id)
        {
            bool result = enabled && ((whiteList.Count > 0 && whiteList.Contains(id)) || whiteList.Count == 0);
            return result;
        }

        public static void WriteLine(string id, string str)
        {
            if (ShouldProceed(id))
            {
                Console.WriteLine("[" + id + "]-- " + str);
            }
        }
    }

    public class PerfElement
    {
        public string id;
        public long numOfCall;
        public long beginSystemTicks;
        public double totalTimeElapsed;
        public double lastTimeElapsed;
        public double maxTimeElaped;
    }

    public static class PerfMonitor
    {
        private static Dictionary<string, PerfElement> elements = new Dictionary<string, PerfElement>();

        private static bool enabled = true;

        private static HashSet<string> whiteList = new HashSet<string>();

        private static string[] whiteListIdStrings = new string[]
        {
        };

        static PerfMonitor()
        {
            foreach (string id in whiteListIdStrings)
            {
                whiteList.Add(id);
            }
        }

        private static bool ShouldProceed(string id)
        {
            bool result = enabled && ((whiteList.Count > 0 && whiteList.Contains(id)) || whiteList.Count == 0);
            return result;
        }

        public static PerfElement GetPerfElement(string id)
        {
            PerfElement result = null;
            bool loop = true;
            string internalId = Thread.CurrentThread.GetHashCode() + id;
            while (loop)
            {
                if (elements.ContainsKey(internalId))
                {
                    result = elements[internalId];
                    loop = false;
                }
                else
                {
                    result = new PerfElement();
                    result.id = internalId;
                    try
                    {
                        elements.Add(internalId, result);
                        loop = false;
                    }
                    catch { }
                }
            }

            return result;
        }

        public static void StartClock(string id)
        {
            long currentTicks = DateTime.Now.Ticks;
            if (ShouldProceed(id))
            {
                PerfElement el = GetPerfElement(id);
                Interlocked.Increment(ref el.numOfCall);
                el.beginSystemTicks = currentTicks;
            }
        }

        public static void StopClock(string id)
        {
            long currentTicks = DateTime.Now.Ticks;
            if (ShouldProceed(id))
            {
                PerfElement el = GetPerfElement(id);
                el.lastTimeElapsed = ((double)(currentTicks - el.beginSystemTicks)) / ((double)TimeSpan.TicksPerMillisecond);
                el.totalTimeElapsed += el.lastTimeElapsed;
                if (el.maxTimeElaped < el.lastTimeElapsed)
                {
                    el.maxTimeElaped = el.lastTimeElapsed;
                }

                Console.WriteLine("[" + Thread.CurrentThread.GetHashCode() + " " + id + "]--" + el.numOfCall + " Result =>  lastTimeElapsed: " + el.lastTimeElapsed + " ms, totalTimeElapsed: " + el.totalTimeElapsed + " ms, maxTimeElapsed: " + el.maxTimeElaped);
            }
        }
    }

    public class SNIPacket
    {
        public byte[] Data
        {
            set; get;
        }

        public void Reset() { }
    }

    internal class SNIPacketFactory : IDisposable
    {
        private static SNIPacketFactory instance = new SNIPacketFactory();

        public static SNIPacketFactory Instance
        {
            get
            {
                return instance;
            }
        }

        private SNIPacketCache _sniPacketCache;
        private ByteArrayCache _sniPacketBufferCache;

        private SNIPacketFactory() { }

        public void Dispose()
        {
            if (_sniPacketCache != null)
            {
                _sniPacketCache.Dispose();
            }

            if (_sniPacketBufferCache != null)
            {
                _sniPacketBufferCache.Dispose();
            }
        }

        public SNIPacket GetSNIPacket()
        {
            SNIPacket sniPacket = null;

            if (_sniPacketCache != null)
            {
                sniPacket = _sniPacketCache.Get();
            }

            if (sniPacket == null)
            {
                sniPacket = new SNIPacket();
            }

            return sniPacket;
        }

        public SNIPacket GetSNIPacket(int bufferSize)
        {
            SNIPacket sniPacket = GetSNIPacket();
            sniPacket.Data = GetSNIPacketBuffer(bufferSize);

            return sniPacket;
        }

        public void PutSNIPacket(SNIPacket sniPacket)
        {
            if (sniPacket != null)
            {
                if (sniPacket.Data != null)
                {
                    if (_sniPacketBufferCache == null)
                    {
                        _sniPacketBufferCache = new ByteArrayCache();
                    }
                    _sniPacketBufferCache.Put(sniPacket.Data);
                    sniPacket.Data = null;
                }

                sniPacket.Reset();

                if (_sniPacketCache == null)
                {
                    _sniPacketCache = new SNIPacketCache();
                }
                _sniPacketCache.Put(sniPacket);
            }
        }

        public byte[] GetSNIPacketBuffer(int bufferSize)
        {
            byte[] buffer = null;
            if (_sniPacketBufferCache != null)
            {
                buffer = _sniPacketBufferCache.Get(bufferSize);
            }

            if (buffer == null)
            {
                buffer = new byte[bufferSize];
            }

            return buffer;
        }
    }

    internal class SNIPacketCache : IDisposable
    {
        private const int maxSize = 1000;
        private ConcurrentStack<SNIPacket> cache = new ConcurrentStack<SNIPacket>();

        public void Dispose()
        {
            cache.Clear();
        }

        public void Put(SNIPacket sniPacket)
        {
            if (sniPacket == null)
            {
                return;
            }

            if (cache.Count < maxSize)
            {
                cache.Push(sniPacket);
                //Console.WriteLine("count: " + cache.Count);
            }
        }

        public SNIPacket Get()
        {
            SNIPacket sniPacket = null;
            cache.TryPop(out sniPacket);

            return sniPacket;
        }
    }

    internal class ByteArrayCache : IDisposable
    {
        private const int maxSize = 1000;
        private ConcurrentDictionary<int, ConcurrentStack<byte[]>> cacheGroup = new ConcurrentDictionary<int, ConcurrentStack<byte[]>>();

        public void Dispose()
        {
            foreach (ConcurrentStack<byte[]> cs in cacheGroup.Values)
            {
                cs.Clear();
            }
            cacheGroup.Clear();
        }

        public void Put(byte[] buffer)
        {
            if (buffer == null)
            {
                return;
            }

            int bufferSize = buffer.Length;

            if (bufferSize == 0)
            {
                return;
            }

            ConcurrentStack<byte[]> cache = cacheGroup.GetOrAdd(bufferSize, new ConcurrentStack<byte[]>());
            if (cache.Count < maxSize)
            {
                cache.Push(buffer);
            }
        }

        public byte[] Get(int bufferSize)
        {
            if (bufferSize <= 0)
            {
                return null;
            }

            ConcurrentStack<byte[]> cache;
            byte[] buffer = null;
            if (cacheGroup.TryGetValue(bufferSize, out cache))
            {
                cache.TryPop(out buffer);
            }

            return buffer;
        }
    }

    public class BufferPool2
    {
        private ConcurrentDictionary<int, ConcurrentStack<byte[]>> map = new ConcurrentDictionary<int, ConcurrentStack<byte[]>>();
        public void Push(byte[] buffer)
        {
            if (buffer == null)
            {
                return;
            }

            int bufferLength = buffer.Length;

            if (bufferLength == 0)
            {
                return;
            }

            ConcurrentStack<byte[]> cs = map.GetOrAdd(bufferLength, new ConcurrentStack<byte[]>());
            cs.Push(buffer);
        }

        public byte[] Pop(int bufferLength)
        {
            if (bufferLength <= 0)
            {
                return null;
            }

            ConcurrentStack<byte[]> cs = map.GetOrAdd(bufferLength, new ConcurrentStack<byte[]>());
            byte[] buffer;
            if (!cs.TryPop(out buffer))
            {
                buffer = null;
            }

            return buffer;
        }
    }

    
    public class BufferPool1
    {
        private ConcurrentDictionary<int, BufferStack> map = new ConcurrentDictionary<int, BufferStack>();
        public void Push(byte[] buffer)
        {
            if (buffer == null)
            {
                return;
            }

            int bufferLength = buffer.Length;

            if (bufferLength == 0)
            {
                return;
            }

            BufferStack bs = map.GetOrAdd(bufferLength, new BufferStack());
            bs.Push(buffer);
        }

        public byte[] Pop(int bufferLength)
        {
            if (bufferLength <= 0)
            {
                return null;
            }

            BufferStack bs;
            if (map.TryGetValue(bufferLength, out bs))
            {
                return bs.Pop();
            }
            else
            {
                return null;
            }
        }

        private class BufferStack
        {
            private BufferNode head;

            public void Push(byte[] buffer)
            {
                BufferNode newHead = new BufferNode(buffer);
                BufferNode oldHead = null;
                do
                {
                    oldHead = head;
                    newHead.next = oldHead;
                }
                while (Interlocked.CompareExchange(ref head, newHead, oldHead) != oldHead);
            }

            public byte[] Pop()
            {
                if (head == null)
                {
                    return null;
                }

                BufferNode oldHead;
                BufferNode newHead;
                do
                {
                    oldHead = head;
                    newHead = oldHead.next;
                }
                while (Interlocked.CompareExchange(ref head, newHead, oldHead) != oldHead);

                return oldHead.buffer;
            }
        }

        private class BufferNode
        {
            public byte[] buffer;
            public BufferNode next;

            public BufferNode(byte[] buffer)
            {
                this.buffer = buffer;
            }
        }
    }
    

    public class TestUtils
    {
        public const string DefaultConnectionString = "Server=tcp:.;User ID=testuser;Password=test1234;";

        public static void RunNonQuery(string connectionString, string sql)
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
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                Console.WriteLine($"SQL \"{sql}\" was skipped.");
            }
        }

        public static void RunNonQuery(string sql)
        {
            RunNonQuery(DefaultConnectionString, sql);
        }

        /*
         * how to use: int value = Int32.Parse(dt.Rows[0][0].ToString());
         */
        public static DataTable RunQuery(string connectionString, string sql)
        {
            DataTable result = null;
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        result = new DataTable();
                        result.Load(reader);
                    }
                }
            }
            return result;
        }

        public static DataTable RunQuery(string sql)
        {
            return RunQuery(DefaultConnectionString, sql);
        }

        public static List<object[]> RunQueryOriginal(string connectionString, string sql)
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

        public class OdbcParameterElement
        {
            public string patameterName;
            public OdbcType odbcType;
            public object value;
        }

        public static List<object[]> OdbcRunQuery(string OdbcConnStr, string sql, List<OdbcParameterElement> OdbcParameters)
        {
            List<object[]> result = null;
            try
            {
                using (OdbcConnection odbcConnection = new OdbcConnection(OdbcConnStr))
                {
                    odbcConnection.Open();
                    using (OdbcCommand command = odbcConnection.CreateCommand())
                    {
                        command.CommandType = CommandType.Text;
                        command.CommandText = sql;
                        if(OdbcParameters!=null && OdbcParameters.Count > 0)
                        {
                            foreach(OdbcParameterElement param in OdbcParameters)
                            {
                                command.Parameters.Add(param.patameterName, param.odbcType).Value = param.value;
                            }
                        }
                        
                        using (OdbcDataReader reader = command.ExecuteReader())
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

        public static string GenerateTableName()
        {
            return string.Format("TEST_{0}{1}{2}", Environment.GetEnvironmentVariable("ComputerName"), Environment.TickCount, Guid.NewGuid()).Replace('-', '_');
        }

        public static string GetRandomString(int length)
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

        public class TableScheme
        {
            public string tableName;
            public List<TableColumn> columns;

            public TableScheme(string tableName)
            {
                this.tableName = tableName;
                columns = new List<TableColumn>();
            }
        }

        public class TableColumn
        {
            public string columnName;
            public string columnType;
            public TableColumn(string columnName, string columnType)
            {
                this.columnName = columnName;
                this.columnType = columnType;
            }
        }

        public static string GenerateCreateTableSql(TableScheme tableScheme)
        {
            string columns = "";
            for (int i = 0; i < tableScheme.columns.Count; ++i)
            {
                columns += tableScheme.columns[i].columnName + " " + tableScheme.columns[i].columnType;
                if (i < tableScheme.columns.Count - 1)
                {
                    columns += ", ";
                }
            }

            string sql = $"CREATE TABLE {tableScheme.tableName} ({columns});";
            //Console.WriteLine("GenerateCreateTableSql: " + sql);
            return sql;
        }

        public static string GererateRandomInsertSql(TableScheme tableScheme)
        {
            string columnNames = "";
            string values = "";
            for (int i = 0; i < tableScheme.columns.Count; ++i)
            {
                columnNames += tableScheme.columns[i].columnName;

                if ("TEXT".Equals(tableScheme.columns[i].columnType, StringComparison.InvariantCultureIgnoreCase))
                {
                    values += "'" + TestUtils.GetRandomString(50) + "'";
                }
                else if ("INT".Equals(tableScheme.columns[i].columnType, StringComparison.InvariantCultureIgnoreCase))
                {
                    values += (new Random()).Next(1, int.MaxValue).ToString();
                }

                if (i < tableScheme.columns.Count - 1)
                {
                    columnNames += ", ";
                    values += ", ";
                }
            }

            string sql = $"INSERT INTO {tableScheme.tableName} ({columnNames}) VALUES ({values})";
            Console.WriteLine("GererateRandomInsertSql: " + sql);
            return sql;
        }
    }
}
