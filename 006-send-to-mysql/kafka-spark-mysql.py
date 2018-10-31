from pyspark import SparkContext, Row
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# note: the mysql's driver is must be correct

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

if __name__ == "__main__":

    # mysql config
    url = "jdbc:mysql://172.16.4.236:3306/spark_test"
    table_name = "word_info"
    username = "root"
    pasword = "root"

    # spark context init
    para_seconds = 10
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, para_seconds)

    # receiver in kafka
    brokers = 'kafka1:9092'
    topic = 'two-two-para'

    # get streaming datas from kafka
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    lines = kvs.map(lambda x: x[1])

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))

        if (rdd.isEmpty()):

            return

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = spark.createDataFrame(rowRdd)

            # Creates a temporary view using the DataFrame.
            wordsDataFrame.createOrReplaceTempView("words")

            # Do word count on table using SQL and print it
            wordCountsDataFrame = \
                spark.sql("select word, count(*) as word_count from words group by word")
            wordCountsDataFrame.show()

            wordCountsDataFrame.write \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "org.mariadb.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", pasword) \
            .save(mode="append")

        except Exception as e:
            print("Some error happen!")
            print(e)

    lines.foreachRDD(process)


    # start job
    ssc.start()
    ssc.awaitTermination()