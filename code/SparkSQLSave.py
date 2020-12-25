from pyspark.sql import SparkSession
#from pyspark.sql.types import *
#from pyspark.sql.functions import *

# Main program
if __name__ == "__main__": # Create a SparkSession 
       spark = (SparkSession
         .builder
         .appName("SparkSQLSave")
         .getOrCreate())

csv_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Creating SQL Databases and Tables
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# Creating a managed table
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# Creating unmanaged table
(flights_df
  .write
  .option("path","/Users/nikhilbansal/Documents/DataML/spark/table")
  .saveAsTable("us_delay_flights_tbl")
  )
'''
 spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
      distance INT, origin STRING, destination STRING)
      USING csv OPTIONS (PATH
      '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")
'''

# The difference between a view and a table is that views don’t actually hold the data; 
# tables persist after your Spark applica‐ tion terminates, but views disappear.
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin= 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin= 'JFK'")
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")


spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

# Dropping a view
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")


