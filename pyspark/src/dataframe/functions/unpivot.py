from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Unpivot Example with stack").getOrCreate()

raw_df = spark.read.csv('C:/Users/AmitKundu/PycharmProjects/practice/pyspark/resource/pivoted_data.csv', header=True)
raw_df.show(5)
# Unpivot the DataFrame using stack()
unpivoted_df = raw_df.selectExpr(
    "Name",
    "stack(3, 'Maths', Maths, 'Science', Science, 'English', English) as (Subject, Score)")
unpivoted_df.show(10)
