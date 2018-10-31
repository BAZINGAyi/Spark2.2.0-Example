from pyspark import SparkContext
from pyspark.shell import sc
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={ "es.resource" : "192.168.174.135/twitter/doc/1" })

print(es_rdd.first())