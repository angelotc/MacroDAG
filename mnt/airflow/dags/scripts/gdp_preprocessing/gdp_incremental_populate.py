from os.path import expanduser, join, abspath

from pyspark.sql import *

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("GDP processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file willshire.csv from the HDFS
incr_df = spark.read.csv('hdfs://namenode:9000/gdp_quarterly/gdp_quarterly.csv', header = True)
base_df = spark.sql("SELECT * FROM gdp_base_table")
incr_df = incr_df.subtract(base_df)
#incr_df = incr_df.union(base_df)
incr_df.show()

# Rename columns after union
incr_df = incr_df.selectExpr("DATE as date", "GDPC1 as quarterlyGDP")

# # Drop the duplicated rows based on the base and last_update columns
gdp_rates = incr_df.select('date', 'quarterlyGDP') \
    .dropDuplicates(['date', 'quarterlyGDP']) \
    .fillna(0,['quarterlyGDP']) \
    .orderBy("date")

#gdp_rates.show()


# Export the dataframe into the Hive table willshire_rates

gdp_rates.write.mode("append").insertInto("gdp_incremental_table")

spark.stop()