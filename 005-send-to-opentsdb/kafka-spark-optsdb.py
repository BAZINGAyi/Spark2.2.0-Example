import json
import random
import time

import requests

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

payload = {
    "metric": "sys.cpu.nice",
    "timestamp": '1489544891',
    "value": '29',
    "tags": {
        "host": "spark1",
    }
}


def print_line(line):
    # print(line)
    return line

# modify _id name and add properties. This is main functions for dealing data.
def generate_doc(item):
    dictinfo = payload

    # add doc _id
    dictinfo["timestamp"] = int(time.time())
    dictinfo['value'] = random.randint(1, 1000000)
    print(dictinfo)
    send_json(dictinfo)

    return dictinfo

def send_json(json):
    r = requests.post("http://192.168.80.2:4242/api/put?details", json=json)
    return r.text

def sendToOpenTsdb(record):
    new_record = record.map(lambda item: generate_doc(item))
    for re in new_record.collect():
        print(re)


if __name__ == "__main__":

    # spark context init
    para_seconds = 10
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, para_seconds)

    # receiver in kafka
    brokers = 'kafka1:9092'
    topic = 'two-two-para'

    # get streaming datas from kafka
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # deal
    lines = kvs.map(lambda x: x[1])
    counts = lines.map(lambda word: print_line(word))
    # counts.pprint()
    counts.foreachRDD(lambda record: sendToOpenTsdb(record))

    # start job
    ssc.start()
    ssc.awaitTermination()