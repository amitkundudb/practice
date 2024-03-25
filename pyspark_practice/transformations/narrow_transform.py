from pyspark import SparkContext

sc = SparkContext("local")
# "local" as we are working only on local machine
list = [15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
#create RDD from list
rdd = sc.parallelize(list,3)
print(f"This is list {rdd.collect()}")

#filter()
rdd_even = rdd.filter(lambda x: x%2 == 0)
print(f"Find out the even values {rdd_even.collect()}")

#flatMap()
data1 = ['Amit&90', 'Ahana&91']
rdd1 = sc.parallelize(data1)
#split each element with a "&" using flatmap()
rdd1 = rdd1.flatMap(lambda x: x.split("&"))
print(f"Splitting the strings where & is present {rdd1.collect()}")

#map()
sq_rdd = rdd.map(lambda x: x ** 2)
print(f"Square of elements in list {sq_rdd.collect()}")

#mapPartitions()
def add_one(iterator):
    return map(lambda x: x + 1, iterator)
result_rdd = rdd.mapPartitions(add_one)
print(f"After adding 1 with each element {result_rdd.collect()}")

#mapPartitionsWithIndex()
def add_partition_index(index, iterator):
    return map(lambda x: (index, x), iterator)
indexed_rdd = rdd.mapPartitionsWithIndex(add_partition_index)
print(f"Elements with index of partition {indexed_rdd.collect()}")