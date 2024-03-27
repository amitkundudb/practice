from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dataframe and way of rename columns") \
    .getOrCreate()

#Inserting Data
data = [('Amit','','Kundu','2001-03-14','M',24666),
  ('Rahul','Sankar','','2000-05-19','M',30000),
  ('Sachin','','Srivastava','1998-09-05','M',40000),
  ('Priya','Devi','Sharma','1997-12-01','F',50000),
  ('Neha','Rani','Patel','1989-02-17','F',80000)
]

#creating column
columns = ["firstname","middlename","lastname","dob","gender","salary"]

#creating dataframe
df = spark.createDataFrame(data=data, schema=columns)

#Print 20 elemnets from dataframe
df.show()

#change column name
df.withColumnRenamed("lastname","l_name").printSchema()
df.show()
#change multiple column name in a single line
df2 = df.withColumnRenamed("firstname","f_name").withColumnRenamed("middlename","m_name")
df2.printSchema()
df2.show()