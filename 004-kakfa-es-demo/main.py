# 利用 spark 从 kafka 读入数据，并利用 spark streaming 处理 json 后 发送到 elastic search
import datetime
import json
import random

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# elasticsearch config
es_write_conf = {
    "es.nodes" : "192.168.174.135",
    "es.port" : "9200",
    "es.resource": "twitter1/new_doc",
    "es.mapping.id": "doc_id"
}

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
    new_record.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)

    for re in new_record.collect():
        print(re)


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
    counts.foreachRDD(lambda record: sendToES(record))

    # start job
    ssc.start()
    ssc.awaitTermination()