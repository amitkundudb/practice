from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("Taransform () use").getOrCreate()

data = [
    ('Amit', 'Maths', 90),
    ('Amit', 'Science', 85),
    ('Amit', 'English', 80),
    ('Rajesh', 'Maths', 78),
    ('Rajesh', 'Science', 88),
    ('Rajesh', 'English', 92),
    ('Suresh', 'Maths', 85),
    ('Suresh', 'Science', 79),
    ('Suresh', 'English', 87)
]

raw_df = spark.createDataFrame(data,['name','subject','score'])

# Define a transformation function to calculate average marks per subject
def cal_avg_marks(raw_df):
    avg_marks_df = raw_df.groupBy('name').agg(avg('score').alias('Average Marks'))
    return raw_df.join(avg_marks_df,'name')

#Applying Transformation
avg_marks_df = raw_df.transform(cal_avg_marks)

avg_marks_df.show()

