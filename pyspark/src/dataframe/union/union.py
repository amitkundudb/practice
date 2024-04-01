from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Union Example").getOrCreate()

employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

department_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True),
    StructField("team_lead", StringType(), True)
])

employee_df = spark.createDataFrame([
    (1, "Amit", 50000),
    (2, "Suresh", 60000),
    (3, "Rajesh", 55000)
], schema=employee_schema)

department_df = spark.createDataFrame([
    (101, "HR", "Ravi"),
    (102, "Engineering", "Ajay"),
    (103, "Finance", "Sunil"),
    (1, "Amit", 50000)
], schema=department_schema)

# Union
combined_df_union = employee_df.union(department_df)
print("Union:")
combined_df_union.show()

# UnionAll
combined_df_unionAll = employee_df.unionAll(department_df)
print("UnionAll:")
combined_df_unionAll.show()