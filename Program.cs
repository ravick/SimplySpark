using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql.Types;

namespace SimplySpark
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkTest();
        }
         static void SparkTest()
        {
            Console.WriteLine($"Running simple Spark Test: Create and show sample data frame.");
            var data = new List<GenericRow>
                        {
                            new GenericRow(new object[] { 1, "John Doe" }),
                            new GenericRow(new object[] { 2, "Jane Doe" }),
                            new GenericRow(new object[] { 3, "Foo Bar" })
                        };

            var schema = new Microsoft.Spark.Sql.Types.StructType(new List<StructField>()
                        {
                            new StructField("Id", new IntegerType()),
                            new StructField("Name", new StringType()),
                        });

            SparkSession spark = SparkSession
               .Builder()
               .AppName("Coconut Spark Test")
               .GetOrCreate();
            
            Console.WriteLine("Spark Configuration");
            
            foreach (var conf in spark.SparkContext.GetConf().GetAll())
            {
                Console.WriteLine($"Conf: {conf.Key} = {conf.Value}");
            }
            
            DataFrame df = spark.CreateDataFrame(data, schema);
            df.Show();
            Console.WriteLine($"If you see a tabular output with names, then the Spark Test Successful !!!");
            Console.WriteLine("Press enter to complete..");
            Console.ReadLine();
            spark.Stop();
        }
    }
}
