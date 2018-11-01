from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def read_from_hdfs():
    conf = SparkConf().setAppName("Python-Read-Write-HDFS")
    sc = SparkContext(conf=conf)

    distFile = sc.textFile("hdfs://hadoop1:9000/user/test.txt")

    for index in distFile.collect():
        print(index)

def write_to_hdfs():
    sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
    data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
    df = sparkSession.createDataFrame(data)

    # Write into HDFS
    df.write.csv("hdfs://hadoop1:9000/user/example.csv")

def read_sql_from_hdfs():
    sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
    df_load = sparkSession.read.csv('hdfs://hadoop1:9000/user/example.csv/')
    df_load.show()

if __name__ == '__main__':

   # read_from_hdfs()

    read_sql_from_hdfs()

    #write_to_hdfs()