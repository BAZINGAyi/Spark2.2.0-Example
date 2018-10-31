# es_spark_test.py
import datetime
import json
import random

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("ESTest")
    sc = SparkContext(conf=conf)

    es_write_conf = {
        # specify the node that we are sending data to (this should be the master)
        "es.nodes": '192.168.174.135',

        # specify the port in case it is not the default port
        "es.port": '9200',

        # specify a resource in the form 'index/doc-type'
        "es.resource": 'testindex/testdoc',

        # is the input JSON?
        "es.input.json": "yes",

        # is there a field in the mapping that should be used to specify the ES document ID
        "es.mapping.id": "doc_id"
    }

    data = [
        {'some_key': 'some_value', 'doc_id': 1},
        {'some_key': 'some_value', 'doc_id': 2},
        {'some_key': 'some_value', 'doc_id': 3}
    ]

    rdd = sc.parallelize(data)


    def format_data(x):
        new_data = {'doc_id': str(random.randint(1, 1000000)) + ":" + datetime.datetime.now().strftime('%Y-%m-%d:%H:%M:%S')}
        result = (new_data['doc_id'], json.dumps(x))
        print(result)
        return result

    rdd = rdd.map(lambda x: format_data(x))

    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

        # critically, we must specify our `es_write_conf`
        conf=es_write_conf)