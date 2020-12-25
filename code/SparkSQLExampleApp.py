from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Main program
if __name__ == "__main__": # Create a SparkSession 
       spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

csv_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
df = spark.read.csv(csv_file, header=True, inferSchema=True)
#df.show(3)
# Print the schema used by Spark to process the DataFrame 
print(df.printSchema())

# Read and create a temporary view
df.createOrReplaceTempView("us_delay_flights_tbl")

# Now that we have a temporary view, we can issue SQL queries using Spark SQL.
spark.sql(""" SELECT distance, origin, destination 
	From us_delay_flights_tbl where distance > 1000
	Order by distance DESC""").show(10)

# Exercise : equivalent query in dataframe API
(df
	.select("distance", "origin", "destination")
	.where(col("distance") > 1000)
	.orderBy(col("distance"),ascending=False)
	.show(10)
	)


spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)

# Exercise : equivalent query in dataframe API
# please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
'''
(df
	.select("date","delay","origin","destination")
	.where(col("delay") > 120 & col("origin") == SFO & col("destination") == ORD)
	.orderBy(col("delay"),ascending=False)
	.show(10)
	)
'''
spark.sql("""SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                  WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                  WHEN delay > 0 and delay < 60  THEN  'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'Early'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10)

# Creating SQL Databases and Tables
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# Creating a managed table
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")




