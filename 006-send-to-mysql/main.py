import datetime
import json
import random
import mysql.connector

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def print_line(line):
    dictinfo = json.loads(line)
    #print(dictinfo["stamp_inserted"])
    return line

# modify _id name and add properties. This is main functions for dealing data.
def generate_doc(item):
    dictinfo = json.loads(item)
    # print(dictinfo['event_type'])

    # add doc _id
    dictinfo["doc_id"] = str(random.randint(1, 1000000)) + ":" + datetime.datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
    result = (dictinfo["doc_id"], dictinfo)
    return result

def sendToES(record):
    new_record = record.map(lambda item: generate_doc(item))

    for re in new_record.collect():
        print(re)

def createMysqlConnection():
    conn = mysql.connector.connect(host='localhost', user='root', passwd='', database='word_info', port=3306)
    cur = conn.cursor()
    return cur

# error 1 : 在 driver 上创建连接，导致其他 worker 无法创建连接
def sendRecord1(rdd):
    connection = createMysqlConnection()  # executed at the driver
    rdd.foreach(lambda record: connection.send(record))
    connection.close()

# error 2: 为每条 rdd 操作创建连接，导致资源浪费
def sendRecord2(rdd):
    connection = createMysqlConnection()
    connection.send(rdd)
    connection.close()

# normal 1: 使用 Partition ，按照分片数去执行
def sendRecord3(iter):
    connection = createMysqlConnection()
    for record in iter:
        connection.send(record)
    connection.close()

if __name__ == "__main__":

    # spark context init
    para_seconds = 20
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, para_seconds)

    # receiver in kafka
    brokers = '192.168.174.135:9092'
    topic = 'spark'

    # get streaming datas from kafka
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # deal
    lines = kvs.map(lambda x: x[1])
    counts = lines.map(lambda word: print_line(word))
    # counts.pprint()


    # error 1
    counts.foreachRDD(lambda record: sendRecord1(record))
    # error 2
    counts.foreachRDD(lambda rdd: rdd.foreach(sendRecord2(rdd)))
    # normal
    counts.foreachRDD(lambda rdd: rdd.foreachPartition(sendRecord3(rdd)))

    # start job
    ssc.start()
    ssc.awaitTermination()




