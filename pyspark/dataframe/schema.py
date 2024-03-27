from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType, StructField

spark = SparkSession.builder.appName('Schema Define').getOrCreate()

data = [(1,'Amit'),(2,'Alisha')]
#schema = ['id','name']
schema = StructType([\
    StructField(name='id',dataType = IntegerType()),\
StructField(name='name',dataType = StringType())])
df = spark.createDataFrame(data,schema)

df.show()
df.printSchema()