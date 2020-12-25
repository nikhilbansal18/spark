from pyspark.sql import SparkSession
from pyspark.ml import image
#from pyspark.sql.types import *
#from pyspark.sql.functions import *

# Main program
if __name__ == "__main__": # Create a SparkSession 
       spark = (SparkSession
         .builder
         .appName("SparkDataSourceRead")
         .getOrCreate())

# Reading Parquet file
# We recommend that you use this format in your ETL and data ingestion processes.
file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
df = spark.read.format("parquet").load(file)
df.show(5)

# Writing or saving a DataFrame as a table or file is a common operation in Spark
(df.write.format("parquet")
	.mode("overwrite") # {append | overwrite | ignore | error or error if exists}
	.option("compression", "snappy")
	.save("/Users/nikhilbansal/Documents/DataML/spark/table/parquet")
	)

# Writing DataFrames to Spark SQL tables
''' 
(df.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl"))
'''
# Reading & Writing json file
json_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
df_json = spark.read.format("json").load(json_file)
df_json.show(5)

(df_json.write.format("json")
	.mode("overwrite")
	.save("/Users/nikhilbansal/Documents/DataML/spark/table/json")
	)

# Reading & Writing csv file
csv_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
df_csv = (spark.read.format("csv")
	.option("header","true")
	.schema(schema)
	.option("mode","FAILFAST")
	.option("nullValue","")
	.load(csv_file))
df_csv.show(5)

(df_csv.write.format("csv")
	.mode("overwrite")
	.save("/Users/nikhilbansal/Documents/DataML/spark/table/csv")
	)

# Reading & Writing Avro file
# the Avro format is used, for exam‐ ple, by Apache Kafka for message serializing and deserializing.
'''
avro_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
df_avro = spark.read.format("avro").load(avro_file)
df_avro.show(truncate=False)

(df_avro.write.format("avro")
	.mode("overwrite")
	.save("/Users/nikhilbansal/Documents/DataML/spark/table/avro")
	)
'''
# Reading & Writing ORC file
orc_file = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
df_orc = spark.read.format("orc").load(orc_file)
df_orc.show(5,truncate=False)

(df_orc.write.format("orc")
	.mode("overwrite")
	.option("compression", "snappy")
	.save("/Users/nikhilbansal/Documents/DataML/spark/table/orc")
	)

# Reading & Writing Image file
# image files, to support deep learning and machine learning frameworks such as TensorFlow and PyTorch.
# sudo pip3 install numpy
images_dir = "/Users/nikhilbansal/Documents/DataML/spark/learningspark/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
images_df = spark.read.format("image").load(images_dir)
images_df.printSchema()
images_df.select("image.height", "image.width", "image.nChannels", "image.mode","label").show(5, truncate=False)

# Reading Binary file
binary_files_df = (spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .load(images_dir))
binary_files_df.show(5)

