from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, round,collect_list, collect_set, countDistinct, count, first, last

spark = SparkSession.builder \
    .appName("AggregateFunctionsExample") \
    .getOrCreate()

data = [("Amit", 23, 24666),
        ("Shyamal", 30, 25000),
        ("Gopal", 35, 22000),
        ("Krishna", 40, 28000),
        ("Radhika", 35, 30000),
        ("Anita",24,40000)]

df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

# aggregate functions
df_agg = df.agg(
    round(mean("Salary")).alias("Mean_Salary"),
    round(avg("Salary")).alias("Avg_Salary"),
    collect_list("Age").alias("Ages_List"),
    collect_set("Age").alias("Ages_Set"),
    countDistinct("Age").alias("Distinct_Ages_Count"),
    count("*").alias("Total_Count"),
    first("Name").alias("First_Name"),
    last("Name").alias("Last_Name")
)

df_agg.show(truncate=False)