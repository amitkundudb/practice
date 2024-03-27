from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

spark = SparkSession.builder.appName("Dataframe Functions").getOrCreate()

data = [("Amit", "Data Engineer", 5000),
        ("Rajesh", "SWE", 7000),
        ("Suresh", "Data Scientist", 3000),
        ("Pawan", "Marketing", 4000),
        ("Kalai", "Data Engineer", 2000)]

columns = ["Employee", "Department", "Salary"]

df = spark.createDataFrame(data, columns)

# count()
count_rows = df.count()
print("Count of rows:", count_rows)

# select()
selected_df = df.select("Employee", "Salary")
selected_df.show()

# filter() & where()
filtered_df = df.filter(col("Salary") > 4000)
filtered_df.show()

where_df = df.where(col("Salary") > 4000)
where_df.show()

# like()
like_df = df.filter(col("Employee").like("S%"))
like_df.show()

# describe()
description = df.describe()
description.show()

# columns()
all_columns = df.columns
print("All columns:", all_columns)

# when() & otherwise()
transformed_df = df.withColumn("Bonus", when(col("Salary") > 5000, 1000).otherwise(500))
transformed_df.show()

# alias()
aliased_df = df.withColumnRenamed("Department", "Dept")
aliased_df.show()

# orderBy() & sort()
ordered_df = df.orderBy("Salary")
ordered_df.show()

sorted_df = df.sort(col("Salary").desc())
sorted_df.show()

# groupBy() & groupBy agg()
grouped_df = df.groupBy("Department")
grouped_df.count().show()

agg_df = df.groupBy("Department").agg(sum("Salary").alias("TotalSalary"))
agg_df.show()