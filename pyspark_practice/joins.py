from pyspark import SparkContext
sc = SparkContext("local")

rdd1 = sc.parallelize([(1, 'Amit'), (2, 'Ahana'), (3, 'Arushi')])
rdd2 = sc.parallelize([(1, 'Xavier'), (2, 'Avik'), (4, 'Sagar')])

# Inner join
inner_join = rdd1.join(rdd2)
print("Inner Join:", inner_join.collect())

# Left outer join
left_outer_join = rdd1.leftOuterJoin(rdd2)
print("Left Outer Join:", left_outer_join.collect())

# Right outer join
right_outer_join = rdd1.rightOuterJoin(rdd2)
print("Right Outer Join:", right_outer_join.collect())

# Full outer join
full_outer_join = rdd1.fullOuterJoin(rdd2)
print("Full Outer Join Result:", full_outer_join.collect())

# Cross join
cross_join = rdd1.cartesian(rdd2)
print("Cross Join Result:", cross_join.collect())
