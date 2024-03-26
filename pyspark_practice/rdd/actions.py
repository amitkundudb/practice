from pyspark import SparkContext

sc = SparkContext("local")
# "local" as we are working only on local machine
list = [15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
#create RDD from list
rdd = sc.parallelize(list,3)
print(f"This is list {rdd.collect()}")

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