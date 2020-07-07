from pyspark.sql import SparkSession
from snippets import *

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([[1,2,3],[4,5,6]])
df.show()

d = {1: 'a', 2: 'b', 3: 'c'}
df = df.withColumn('_4', map_cols(d, '_1'))
df.show()

df = df.withColumn('_5', is_in({'1', '2', '3'}, '_1'))
df.show()


