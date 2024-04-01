from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Joins").getOrCreate()
employee_data = [
    (1, "Amit", 50000, 101),
    (2, "Suresh", 60000, 102),
    (3, "Rajesh", 55000, 101),
    (4, "Vikram", 70000, 103),
    (5,"Anita", 30000, 105)
]
department_data = [
    (101, "HR", "Ravi", "HR Project"),
    (102, "Engineering", "Ajay", "Engineering Project"),
    (103, "Finance", "Sunil", "Finance Project"),
    (104, "Marketing", "Anita", "Marketing Project")
]

employee_df = spark.createDataFrame(employee_data, ["id", "name", "salary", "dept_id"])
department_df = spark.createDataFrame(department_data, ["dept_id", "name", "team_lead", "project"])

employee_df.show()
department_df.show()

# Inner Join
inner_join = employee_df.join(department_df, employee_df.dept_id == department_df.dept_id, "inner")
print("Inner Join:")
inner_join.show()

# Cross Join
cross_join = employee_df.crossJoin(department_df)
print("Cross Join:")
cross_join.show()

# Outer Join
outer_join = employee_df.join(department_df, employee_df.dept_id == department_df.dept_id, "outer")
print("Outer Join:")
outer_join.show()

# Left Join
left_join = employee_df.join(department_df, employee_df.dept_id == department_df.dept_id, "left")
print("Left Join:")
left_join.show()

# Right Join
right_join = employee_df.join(department_df, employee_df.dept_id == department_df.dept_id, "right")
print("Right Join:")
right_join.show()

# Left Semi-Join
left_semi_join = employee_df.join(department_df, employee_df.dept_id == department_df.dept_id, "leftsemi")
print("Left Semi-Join:")
left_semi_join.show()

# Left Anti-Join
left_anti_join = employee_df.join(department_df, employee_df.dept_id == department_df.dept_id, "leftanti")
print("Left Anti-Join:")
left_anti_join.show()

# Self Join
self_join = employee_df.alias("emp1").join(employee_df.alias("emp2"), col("emp1.dept_id") == col("emp2.dept_id"), "inner")
print("Self Join:")
self_join.show()

# Broadcast Join
broadcast_join = employee_df.join(broadcast(department_df), employee_df.dept_id == department_df.dept_id, "inner")
print("Broadcast Join:")
broadcast_join.show()