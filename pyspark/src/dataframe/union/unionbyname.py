from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("Union Example") \
    .getOrCreate()

employee_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True),
    StructField("team_lead", StringType(), True)
])

department_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("team_lead", StringType(), True),
    StructField("dept_name", StringType(), True)
])

employee_df = spark.createDataFrame([
    (1, "Amit", 50000),
    (2, "Suresh", 60000),
    (3, "Rajesh", 55000)
], schema=employee_schema)

department_df = spark.createDataFrame([
    (101, "HR", "Ravi"),
    (102, "Engineering", "Ajay"),
    (103, "Finance", "Sunil")
], schema=department_schema)

# UnionByName
combined_df_unionByName = employee_df.unionByName(department_df, allowMissingColumns=True)
print("UnionByName:")
combined_df_unionByName.show()