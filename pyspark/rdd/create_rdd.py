from pyspark import SparkContext

sc = SparkContext("local")
# "local" as we are working only on local machine
list = [15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
#create RDD from list
rdd = sc.parallelize(list,3)
print(f"This is list {rdd.collect()}")