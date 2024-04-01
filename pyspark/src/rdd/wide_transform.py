from pyspark import SparkContext

sc = SparkContext("local")
# "local" as we are working only on local machine
list = [15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
#create RDD from list
rdd = sc.parallelize(list,3)
print(f"This is list {rdd.collect()}")

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