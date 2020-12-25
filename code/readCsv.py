from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Main program
if __name__ == "__main__": # Create a SparkSession 
       spark = (SparkSession
         .builder
         .appName("readCsv")
         .getOrCreate())

sf_fire_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/chapter3/data/sf-fire-calls.csv"
parquet_path = "/Users/nikhilbansal/Documents/DataML/spark/code"
fire_df = spark.read.csv(sf_fire_file, header=True, inferSchema=True)
fire_df.show(3)
# Print the schema used by Spark to process the DataFrame 
print(fire_df.printSchema())

# In Python to save as a Parquet file
#fire_df.write.format("parquet").save(parquet_path)

# In Python to save as a Parquet table
#parquet_table = "sf_fire_file.tbl"
#fire_df.write.format("parquet").saveAsTable(parquet_table)

# Projections and filters
few_fire_df = (fire_df
	.select("IncidentNumber", "AvailableDtTm", "CallType")
	.where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# In Python, return number of distinct types of calls using countDistinct()
(fire_df
	.select("CallType")
	.where(col("CallType").isNotNull())
	.agg(countDistinct("CallType").alias("DistinctCallTypes"))
	.show()
	)

 # In Python, filter for only distinct non-null CallTypes from all the rows
(fire_df
	.select("CallType")
	.where(col("CallType").isNotNull())
	.distinct()
	.show(10,False)
	)

# Change the name of the column name & show columns having delay > 5
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
	.select("ResponseDelayedinMins")
	.where(col("ResponseDelayedinMins") > 5)
	.show(5,False)
	)

# Change string to date time stamp
fire_ts_df = (new_fire_df
	.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
	.drop("CallDate")
	.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
	.drop("WatchDate")
	.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")
	)
(fire_ts_df
	.select("IncidentDate","OnWatchDate","AvailableDtTS")
	.show(5,False))

# Now month, year and day functions can be used.
(fire_ts_df
	.select(year("IncidentDate"))
	.distinct()
	.orderBy(year("IncidentDate"))
	.show()
	)

# Let’s take our first question: what were the most common types of fire calls?
(fire_ts_df
	.select("CallType")
	.where(col("CallType").isNotNull())
	.groupBy("CallType")
	.count()
	.orderBy("count", ascending=False)
	.show(10,False)
	)
#importing the PySpark functions in a Pythonic way so as not to conflict with the built-in Python functions
import pyspark.sql.functions as F 
(fire_ts_df
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show())

# Exercise
# What were all the different types of fire calls in 2018?
# What months within the year 2018 saw the highest number of fire calls?
# Which neighborhood in San Francisco generated the most fire calls in 2018? 
# Which neighborhoods had the worst response times to fire calls in 2018? 
# Which week in the year in 2018 had the most fire calls?
# Is there a correlation between neighborhood, zip code, and number of fire calls? 
# How can we use Parquet files or SQL tables to store this data and read it back?


