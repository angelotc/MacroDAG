from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("gdp processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# query both base and incremental tables, and find union
incr_df = spark.sql("SELECT * FROM gdp_incremental_table")
base_df = spark.sql("SELECT * FROM gdp_base_table")
#base_df.show()
base_df = incr_df.union(base_df)

# Drop the duplicated rows based on the base and last_update columns
gdp_rates = base_df.select('date', 'gdp') \
    .fillna(0,['gdp']) \
    .orderBy('date')

#gdp_rates.show()


# # Export the dataframe into the Hive table willshire_rates

gdp_rates.write.mode("append").insertInto("gdp_base_table")
spark.stop()