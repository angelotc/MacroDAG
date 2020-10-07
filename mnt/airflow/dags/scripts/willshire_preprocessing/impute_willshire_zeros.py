
from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession,types,dataframe
from os.path import  abspath
# for custom linear interpolation function

import pandas as pd

warehouse_location = abspath('spark-warehouse')


# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Willshire processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


# Read the file willshire.csv from the HDFS

base_df = spark.sql("SELECT * FROM willshire_base_table ORDER BY `date`")
base_df = base_df.toPandas().resample(rule="D")
print(base_df)



# Impute 0 values with mean
#df_final.show()

