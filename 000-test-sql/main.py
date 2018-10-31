from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()

df = spark.read.json("test.json")
# Displays the content of the DataFrame to stdout
df.show()
