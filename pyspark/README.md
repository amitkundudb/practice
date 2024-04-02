
#### RDD (Resilient Distributed Dataset)
- **How to create RDD**: RDDs are fundamental data structures in Spark, representing immutable distributed collections of objects. You can create RDDs from external datasets or by parallelizing an existing collection in your driver program.
- **RDD Transformations & Actions**: Transformations are operations that create a new RDD from an existing one, while actions compute a result based on an RDD and either return it to the driver program or write it to an external storage system.

#### Dataframe
- **SparkSession**: SparkSession is the entry point to Spark SQL, providing a single point of entry to interact with Spark functionality and manage Spark configurations.
- **Schema**: Schema defines the structure of the DataFrame, specifying column names and data types. It's essential for organizing and processing structured data efficiently.
- **DataFrame Functions**: DataFrame functions offer a wide range of operations for manipulating and transforming data, including selecting specific columns, filtering rows, and aggregating data.

#### RDD, Dataframe, Dataset [Difference and Converting from one to Another]
- **Understanding the differences**: RDDs, DataFrames, and Datasets are abstractions provided by Spark for processing structured and unstructured data. RDDs are low-level APIs offering more control but less optimization, while DataFrames and Datasets provide higher-level APIs with optimizations for structured data processing.
- **Converting between RDD, Dataframe, and Dataset**: Spark provides methods to convert between these abstractions, enabling seamless integration and interoperability within a Spark application.

#### Narrow and Wide Transformation
- **Transformations in Spark**: Transformations in Spark are operations applied to RDDs to create a new RDD. They are categorized into narrow and wide transformations based on their dependency on data partitioning and shuffling.
- **Understanding Shuffling in Spark**: Shuffling is the process of redistributing data across multiple partitions, usually during wide transformations like groupByKey() or join(). It involves data exchange between nodes and can impact performance.

#### File Formats with Schema
- **Reading Different File Formats with Schema**: Spark supports reading various file formats like CSV, JSON, and Parquet. Defining a schema provides a structure to the data, enhancing performance and enabling type safety.
- **Importance of Options**: Options while reading files, such as inferSchema and custom schema, impact the structure and interpretation of the data. Understanding these options ensures accurate data processing and analysis.

#### Handling Data
- **How to Handle NULL Values**: NULL values are common in datasets and can affect analysis results. Techniques like filtering, replacing, or dropping NULL values are essential for data cleaning and preprocessing.
- **How to Handle Duplicates**: Duplicates in data can skew analysis results and increase processing overhead. Identifying and removing duplicates using distinct() or dropDuplicates() ensures data integrity and accuracy.

#### Numeric and String Functions
- **Numeric Functions**: Spark provides various numeric functions like sum(), avg(), and round() for performing mathematical operations on numerical data.
- **String Functions**: String functions like upper(), trim(), and substring() enable manipulation and transformation of string data, facilitating data cleaning and text processing tasks.

#### Date and Time Functions
- **Date and Time Operations**: Spark offers functions for performing operations on date and time data, including calculating differences between dates, extracting components like year or month, and formatting dates into desired formats.

#### Aggregate Functions and Joins
- **Aggregate Functions**: Aggregate functions like mean, count, and sum allow summarizing data across groups, facilitating statistical analysis and reporting.
- **Joins**: Joins combine data from multiple sources based on a common key, enabling integration of related datasets and analysis across different data sources.

#### Union and Window Functions
- **Union**: Union operation combines datasets vertically, allowing concatenation of multiple datasets with the same schema.
- **Window Functions**: Window functions operate on a subset of rows called a window, enabling calculations over ordered rows or groups of rows within the dataset.

#### Array Functions and UDFs
- **Array Functions**: Spark provides functions for working with array data structures, including operations like ARRAY_CONTAINS and ARRAY_LENGTH.
- **User-Defined Functions (UDFs)**: UDFs allow extending Spark's functionality by defining custom functions to apply complex transformations on data.

#### Explode Array and Maps
- **Explode Functions**: Explode functions like explode() and posexplode() enable flattening nested data structures like arrays and maps into separate rows, facilitating further analysis and processing.

#### Additional Functions
- **JSON Functions**: Functions like from_json() and to_json() facilitate parsing and serializing JSON data, enabling seamless integration with other Spark operations.