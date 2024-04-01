import pyspark
from pyspark import SparkContext
sc = SparkContext("local")

rdd = sc.parallelize(range(10))

rdd.cache()

result1 = rdd.reduce(lambda x, y: x + y)
result2 = rdd.count()

print("Sum of elements:", result1)
print("Count of elements:", result2)

# Persisting in different storages
rdd.persist(storageLevel=pyspark.StorageLevel.MEMORY_ONLY)
# rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

print("RDD is completed persist", rdd.is_cached)
