## **Exploring PySpark: Practical Examples and Functions Demystified**

1. `filter()`: Filters elements of the RDD based on a predicate function.

2. `flatMap()`: Transforms each element into zero or more elements, then flattens the result.

3. `map()`: Applies a function to each element of the RDD.

4. `mapPartitions()`: Applies a function to each partition of the RDD.

5. `mapPartitionsWithIndex()`: Similar to `mapPartitions()`, but also provides an index of the partition.

6. `glom()`: Collects all elements within each partition into a list.

7. `union()`: Combines two RDDs into one RDD.

8. `intersection()`: Finds the common elements between two RDDs.

9. `distinct()`: Removes duplicate elements from the RDD.

10. `groupByKey()`: Groups values by key in a key-value RDD.

11. `collect()`: Returns all elements of the RDD as an array.

12. `take()`: Returns the first n elements from the RDD.

13. `top()`: Returns the top n elements from the RDD.

14. `count()`: Returns the number of elements in the RDD.

15. `min()`: Returns the minimum value in the RDD.

16. `max()`: Returns the maximum value in the RDD.

17. `mean()`: Returns the mean value of elements in the RDD.

18. `reduce()`: Reduces the elements of the RDD using a specified binary function.

19. `countByKey()`: Counts the occurrences of each key in a key-value RDD.

20. `countByValue()`: Counts the occurrences of each unique value in the RDD.

21. `fold()`: Aggregates the elements of the RDD using a specified binary function and an initial value.

22. `range()`: Creates an RDD of evenly distributed values within a specified range.

23. Joins:
   - `join()`: Inner join between two key-value RDDs.
   - `leftOuterJoin()`: Left outer join between two key-value RDDs.
   - `rightOuterJoin()`: Right outer join between two key-value RDDs.
   - `fullOuterJoin()`: Full outer join between two key-value RDDs.
   - `cartesian()`: Cartesian product (cross join) between two RDDs.

24. `cache()`: Persist RDD in memory for faster access.

25. `persist()`: Persists RDD with specified storage level.

26. `broadcast()`: Broadcasts a read-only variable to all workers.