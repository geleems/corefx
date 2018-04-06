using System.Data.SqlTypes;
using Xunit;

namespace System.Data.SqlClient.Tests
{
    public class DecimalConversionTest
    {
        public static void Run()
        {
            Console.WriteLine("DecimalConversionTest Started...");
            BulkCopySqlDecimalToTable();
        }

        public static void AdjustScaleForFlatZero()
        {
            SqlDecimal value = SqlDecimal.Parse("0");
            Assert.Equal((byte)1, value.Precision);
            Assert.Equal((byte)0, value.Scale);

            SqlDecimal valueAdjusted = SqlDecimal.AdjustScale(value, 1, false);
            Assert.Equal((byte)1, valueAdjusted.Precision);
            Assert.Equal((byte)1, valueAdjusted.Scale);
            Assert.Equal("0.0", valueAdjusted.Value.ToString());
            
            valueAdjusted = SqlDecimal.AdjustScale(value, 2, false);
            Assert.Equal((byte)2, valueAdjusted.Precision);
            Assert.Equal((byte)2, valueAdjusted.Scale);
            Assert.Equal("0.00", valueAdjusted.Value.ToString());

            valueAdjusted = SqlDecimal.AdjustScale(value, 0, false);
            Assert.Equal((byte)1, valueAdjusted.Precision);
            Assert.Equal((byte)0, valueAdjusted.Scale);
            Assert.Equal("0", valueAdjusted.Value.ToString());

            value = SqlDecimal.Parse("0.0");
            Assert.Equal((byte)1, value.Precision);
            Assert.Equal((byte)1, value.Scale);

            valueAdjusted = SqlDecimal.AdjustScale(value, -1, false);
            Assert.Equal((byte)1, valueAdjusted.Precision);
            Assert.Equal((byte)0, valueAdjusted.Scale);
            Assert.Equal("0", valueAdjusted.Value.ToString());

            value = SqlDecimal.Parse("0.00");
            Assert.Equal((byte)2, value.Precision);
            Assert.Equal((byte)2, value.Scale);

            valueAdjusted = SqlDecimal.AdjustScale(value, -1, false);
            Assert.Equal((byte)1, valueAdjusted.Precision);
            Assert.Equal((byte)1, valueAdjusted.Scale);
            Assert.Equal("0.0", valueAdjusted.Value.ToString());

            valueAdjusted = SqlDecimal.AdjustScale(value, -2, false);
            Assert.Equal((byte)1, valueAdjusted.Precision);
            Assert.Equal((byte)0, valueAdjusted.Scale);
            Assert.Equal("0", valueAdjusted.Value.ToString());
        }

        public static void DecimalScaleAdjustTest()
        {
            SqlDecimal value = SqlDecimal.Parse("0");
            value = SqlDecimal.AdjustScale(value, 1, false);
            Console.WriteLine("value: " + value);
            Console.WriteLine("value.Precision: " + value.Precision);
            Console.WriteLine("value.Scale: " + value.Scale);
        }

        public static void BulkCopySqlDecimalToTable()
        {
            string targetTableName = "mytable";
            TestUtils.RunNonQuery(TestUtils.DefaultConnectionString, $"drop table {targetTableName}");
            TestUtils.RunNonQuery(TestUtils.DefaultConnectionString, $"create table {targetTableName} (target_column decimal(2,2))");

            SqlDecimal decimalValue = SqlDecimal.Parse("1.992");
            //decimalValue = SqlDecimal.ConvertToPrecScale(decimalValue, 10,1);

            DataTable dt = new DataTable();
            dt.Clear();
            dt.Columns.Add("source_column", typeof(SqlDecimal));
            DataRow row = dt.NewRow();
            row["source_column"] = decimalValue;
            dt.Rows.Add(row);

            using (SqlBulkCopy sbc = new SqlBulkCopy(TestUtils.DefaultConnectionString, SqlBulkCopyOptions.KeepIdentity))
            {
                sbc.DestinationTableName = targetTableName;

                // Add your column mappings here
                sbc.ColumnMappings.Add("source_column", "target_column");

                // Finally write to server
                sbc.WriteToServer(dt);
            }
        }

        public static void JustTest()
        {
            SqlDecimal value = SqlDecimal.AdjustScale(SqlDecimal.Parse("0.02"), -2, false);
            Console.WriteLine("value: " + value);
            Console.WriteLine("value.Precision: " + value.Precision);
            Console.WriteLine("value.Scale: " + value.Scale);

            value = SqlDecimal.ConvertToPrecScale(SqlDecimal.Parse("1.11"), 2, 2);
            value = SqlDecimal.ConvertToPrecScale(value, 1, 1);
            Console.WriteLine("value: " + value);
            Console.WriteLine("value.Precision: " + value.Precision);
            Console.WriteLine("value.Scale: " + value.Scale);
        }
    }
}
