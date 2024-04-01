from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DateType
from pyspark.sql.functions import current_date, current_timestamp, date_add, datediff, year, month, dayofmonth, to_date, date_format

spark = (SparkSession.builder.appName("Date-Time Functions")
         .config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate())
schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("ShipDate", DateType(), True),
])
df_csv = spark.read.csv("C:/Users/AmitKundu/Downloads/train.csv",schema= schema,header=True)
df_csv.show(5,truncate=False)

# date-time functions
df_with_date_functions = df_csv.select(
    current_date().alias("current_date"),
    current_timestamp().alias("current_timestamp"),
    date_add(df_csv.OrderDate, 5).alias("order_date_plus_5_days"),
    datediff(df_csv.ShipDate, df_csv.OrderDate).alias("days_to_ship"),
    year(df_csv.OrderDate).alias("order_year"),
    month(df_csv.OrderDate).alias("order_month"),
    dayofmonth(df_csv.OrderDate).alias("order_day"),
    to_date(df_csv.OrderDate).alias("formatted_order_date"),
    date_format(df_csv.OrderDate, "MM-dd-yyyy").alias("formatted_order_date_string")
)

# DataFrame with applied functions
df_with_date_functions.show(5, truncate=False)





