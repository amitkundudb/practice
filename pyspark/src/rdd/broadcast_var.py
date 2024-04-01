from pyspark import SparkContext
sc = SparkContext("local")

broadcast_var = sc.broadcast([1, 2, 3, 4, 5])

# Accessing broadcast variable value
print("Broadcast variable value:", broadcast_var.value)
# Using broadcast variable in operations
rdd = sc.parallelize(range(10))
result = rdd.map(lambda x: x + broadcast_var.value[4]).collect()
print("Result:", result)