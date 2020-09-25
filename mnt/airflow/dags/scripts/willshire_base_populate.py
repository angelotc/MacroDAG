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
incr_df = spark.sql("SELECT * FROM willshire_incremental_table")

base_df = spark.sql("SELECT * FROM willshire_base_table")
#base_df.show()

base_df = incr_df.union(base_df)
base_df.show()

# Drop the duplicated rows based on the base and last_update columns
willshire_rates = base_df.select('date', 'will5000') \
    .fillna(0,['will5000']) \
    .orderBy("date")

#willshire_rates.show()


# # Export the dataframe into the Hive table willshire_rates

willshire_rates.write.mode("append").insertInto("willshire_base_table")