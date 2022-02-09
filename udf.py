import bisect
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

score_breakpoints = [15, 25, 35, 45, 55, 65, 75, 85, 95]

grade = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

df_data = [("Timmy", 98),
           ("Tom", 76),
           ("Jack", 49),
           ("Carl", 52),
           ("Sam", 67)]

df_schema = StructType().add("Name", StringType(), True).add("Score", IntegerType(), True)

spark = SparkSession \
    .builder \
    .appName("PySpark POC") \
    .getOrCreate()


def sort_score(score, breakpoints=score_breakpoints, grades=grade):
    i = bisect.bisect(breakpoints, score)
    return grades[i]


sort_score_udf = udf(lambda x: sort_score(x))

print(sort_score(75))

df = spark.createDataFrame(data=df_data, schema=df_schema)

df.show()

df = df.withColumn("Grade", sort_score_udf(df['Score']))

df.show()
