from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Willshire processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file willshire.csv from the HDFS
incr_df = spark.read.csv('hdfs://namenode:9000/willshire/willshire.csv', header = True)
base_df = spark.sql("SELECT * FROM willshire_base_table")
incr_df = df.union(df2)

# Drop the duplicated rows based on the base and last_update columns
forex_rates = df3.select('DATE', 'WILL5000PRFC') \
    .dropDuplicates(['DATE', 'WILL5000PRFC']) \
    .fillna(0)

forex_rates.show()



# # Export the dataframe into the Hive table willshire_rates

#df2 = spark.sql("DELETE FROM willshire_rates")
#forex_rates.write.mode("append").insertInto("willshire_rates")