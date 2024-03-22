from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Replace with Indian Names") \
    .getOrCreate()

data = [('Amit','','Kundu','2001-03-14','M',24666),
  ('Rahul','Sankar','','2000-05-19','M',30000),
  ('Sachin','','Srivastava','1998-09-05','M',40000),
  ('Priya','Devi','Sharma','1997-12-01','F',5000),
  ('Neha','Rani','Patel','1989-02-17','F',-1)
]
#creating columns
columns = ["firstname","middlename","lastname","dob","gender","salary"]

#creating dataframe
df = spark.createDataFrame(data=data, schema=columns)

#show 20 elements from dataframe
df.show()

