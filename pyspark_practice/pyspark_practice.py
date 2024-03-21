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

#glom() collects all elements within each partition into a list
new_rdd = rdd.glom().collect()
print(f"Glom() to make every partition as list {new_rdd}")

#union() of 2 RDDs
data2 = sc.parallelize([1,2,3,4,5,16,17,18,19,20])
union_rdd = rdd.union(data2)
print(f"Using Union {union_rdd.collect()}")

# Intersection of two RDDs
intersection_rdd = rdd.intersection(data2)
print(f"Using Intersection {intersection_rdd.collect()}")

#distinct() to Remove duplicate elements
distinct_rdd = union_rdd.distinct()
print(f"Distinct values {distinct_rdd.collect()}")

# Group values by key
pair_rdd = sc.parallelize([(135, 'Amit'), (136, 'DB'), (135, 'DE'), (136, 'Trainee')])
grouped_rdd = pair_rdd.groupByKey()
result = grouped_rdd.collect()
for key, values in result:
    print(f"Key: {key}, Values: {[v for v in values]}")

# collect() action
collected_data = rdd.collect()
print("Collected Data:", collected_data)

# take() action
taken_data = rdd.take(3)
print("Taken Data:", taken_data)

# top() action
top_data = rdd.top(3)
print("Top Data:", top_data)

# count() action
count = rdd.count()
print("Count:", count)

# min() action
min_value = rdd.min()
print("Minimum Value:", min_value)

# max() action
max_value = rdd.max()
print("Maximum Value:", max_value)

# mean() action
mean_value = rdd.mean()
print("Mean Value:", mean_value)

# reduce() action to find the sum of elements
sum_of_elements = rdd.reduce(lambda x, y: x + y)
print("Sum of Elements:", sum_of_elements)

# Creating a key-value RDD # countByKey() action
kv_rdd = sc.parallelize([(1, 'Amit'), (2, 'Ahana'), (1, 'Susmita'), (3, 'Soumita'), (2, 'Arushi')])
counts = kv_rdd.countByKey()
print("Counts by Key:", counts)

#countByValue()
value_counts = rdd.countByValue()
print("Count by value: ",value_counts)

# fold() action to find the sum of elements
sum_of_elements = rdd.fold(0, lambda x, y: x + y)
print("Sum of Elements:", sum_of_elements)

# range() action
range_rdd = sc.range(1, 20, 3)
print("Range RDD:", range_rdd.collect())
