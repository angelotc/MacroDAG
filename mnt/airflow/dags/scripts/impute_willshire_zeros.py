
from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession
from os.path import  abspath
# for custom linear interpolation function
from pyspark.sql import functions as F
from pyspark.sql import Window
warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Willshire processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
def fill_linear_interpolation(df,order_col,value_col):
    """ 
    Apply linear interpolation to dataframe to fill gaps. 

    :param df: spark dataframe
    :param id_cols: string or list of column names to partition by the window function 
    :param order_col: column to use to order by the window function
    :param value_col: column to be filled

    :returns: spark dataframe updated with interpolated values
    """
    # create row number over window and a column with row number only for non missing values
    w = Window.orderBy(order_col)
    new_df = df
    new_df = new_df.withColumn('rn',F.row_number().over(w))
    new_df = new_df.withColumn('rn_not_null',F.when(F.col(value_col).isNotNull(),F.col('rn')))

    # create relative references to the start value (last value not missing)
    w_start = Window.orderBy(order_col).rowsBetween(Window.unboundedPreceding,-1)
    new_df = new_df.withColumn('start_val',F.last(value_col,True).over(w_start))
    new_df = new_df.withColumn('start_rn',F.last('rn_not_null',True).over(w_start))

    # create relative references to the end value (first value not missing)
    w_end = Window.orderBy(order_col).rowsBetween(0,Window.unboundedFollowing)
    new_df = new_df.withColumn('end_val',F.first(value_col,True).over(w_end))
    new_df = new_df.withColumn('end_rn',F.first('rn_not_null',True).over(w_end))

    # create references to gap length and current gap position  
    new_df = new_df.withColumn('diff_rn',F.col('end_rn')-F.col('start_rn'))
    new_df = new_df.withColumn('curr_rn',F.col('diff_rn')-(F.col('end_rn')-F.col('rn')))

    # calculate linear interpolation value
    lin_interp_func = (F.col('start_val')+(F.col('end_val')-F.col('start_val'))/F.col('diff_rn')*F.col('curr_rn'))
    new_df = new_df.withColumn(value_col,F.when(F.col(value_col).isNull(),lin_interp_func).otherwise(F.col(value_col)))

    keep_cols = [order_col,value_col]
    new_df = new_df.select(keep_cols)
    return new_df

# Read the file willshire.csv from the HDFS

base_df = spark.sql("SELECT * FROM willshire_base_table")


# Impute 0 values with mean
new_df = fill_linear_interpolation(
    df=base_df,
    order_col='date',
    value_col='will5000')
new_df.show()

