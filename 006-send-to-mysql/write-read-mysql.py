from __future__ import print_function

from pyspark.sql import SparkSession
# $example on:schema_merging$
from pyspark.sql import Row

def read_jdbc_dataset_example(spark):
    # $example on:jdbc_dataset$
    # Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    # Loading data from a JDBC source
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", "org.mariadb.jdbc.Driver")\
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", pasword) \
        .load()

    jdbcDF.show()
    jdbcDF.printSchema()


def write_jdbc_dataset_example(spark):
    jdbcDF = spark.read.json("test.json")
    # Saving data to a JDBC source
    jdbcDF.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://172.16.4.236:3306/spark_test") \
        .option("driver", "org.mariadb.jdbc.Driver") \
        .option("dbtable", "word_info") \
        .option("user", "root") \
        .option("password", "root") \
        .save(mode="append")

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    url = "jdbc:mysql://172.16.4.236:3306/spark_test"
    table_name = "word_info"
    username = "root"
    pasword = "root"


    read_jdbc_dataset_example(spark)

    write_jdbc_dataset_example(spark)


