from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("Shuffle using repartition").getOrCreate()

# Sample data
data = [
    (1, "Amit", 50000, "Engineer"),
    (2, "Ayan", 60000, "Manager"),
    (3, "Budhan", 55000, "Analyst"),
    (4, "Jai", 70000, "Director"),
    (5, "Milan", 45000, "Developer"),
    (6, "Eswar", 75000, "Architect"),
    (7, "Sachin", 48000, "Tester"),
    (8, "Virat", 67000, "Designer"),
    (9, "Anuska", 52000, "Consultant"),
    (10, "Priyanka", 63000, "Product Manager"),
    (11, "Salman", 59000, "Engineer"),
    (12, "SRK", 72000, "Data Scientist"),
    (13, "Amir", 56000, "Analyst"),
    (14, "Suhana", 68000, "Engineer"),
    (15, "Ruchi", 71000, "Project Manager"),
    (16, "Haritha", 54000, "Developer"),
    (17, "Harini", 66000, "Analyst"),
    (18, "Mani", 64000, "Engineer"),
    (19, "Krishna", 57000, "Designer"),
    (20, "Baseer", 69000, "Architect")
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("role", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
# Repartition to shuffle data
shuffled_df = df.repartition(3)
num_partitions = shuffled_df.rdd.getNumPartitions()
print("Number of partitions:", num_partitions)

shuffled_df.show()