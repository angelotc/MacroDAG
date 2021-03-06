from os.path import expanduser, join, abspath

from pyspark.sql import *

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
df = spark.sql("SELECT * FROM willshire_base_table")
df.show()
incr_df = incr_df.subtract(df)
#incr_df = incr_df.union(base_df)
incr_df.show()

# Rename columns after union
incr_df = incr_df.selectExpr("DATE as date", "WILL5000PRFC as will5000")

# # Drop the duplicated rows based on the base and last_update columns
willshire_rates = incr_df.select('date', 'will5000') \
    .dropDuplicates(['date', 'will5000']) \
    .fillna(0,['will5000']) \
    .orderBy("date")

willshire_rates.show()


# Export the dataframe into the Hive table willshire_rates

willshire_rates.write.mode("append").insertInto("willshire_incremental_table")

spark.stop()