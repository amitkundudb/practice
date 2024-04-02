from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

spark = SparkSession.builder.appName("Custom JSON Schema").getOrCreate()

custom_schema = StructType([
    StructField("employees", ArrayType(
        StructType([
            StructField("empId", IntegerType()),
            StructField("empName", StringType())
        ])
    )),
    StructField("id", IntegerType()),
    StructField("properties", StructType([
        StructField("name", StringType()),
        StructField("storeSize", StringType())
    ]))
])

json_data = "C:/Users/AmitKundu/PycharmProjects/practice/pyspark/resource/sample_json_3.json"

df = spark.read.json(json_data, schema=custom_schema, multiLine=True)
df.printSchema()
df.show(truncate=False)


# Explode the 'employees' array
df_exploded = df.selectExpr("id", "properties", "explode(employees) as employee")
print("DataFrame after exploding arrays:")
df_exploded.show(truncate=False)

# Unnest the 'properties' struct type
df_unnested = df_exploded.selectExpr(
    "id",
    "properties.name as companyName",
    "properties.storeSize",
    "employee.empId",
    "employee.empName as employeeName"
)
print("DataFrame after unnesting struct types:")
df_unnested.show(truncate=False)



