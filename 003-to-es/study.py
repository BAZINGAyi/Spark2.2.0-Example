# es_spark_test.py
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    conf = SparkConf().setAppName("ESTest")
    sc = SparkContext(conf=conf)

    ## read from elastic search
    es_read_conf = {
        "es.nodes" : "192.168.174.135",
        "es.port" : "9200",
        "es.resource": "twitter1/new_doc"
    }

    es_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf)

    for rdd in es_rdd.collect():
        print(rdd)

    # write from elastic search

    # es_write_conf = {
    #     "es.nodes" : "192.168.174.135",
    #     "es.port" : "9200",
    #     "es.resource": "twitter_new/new_doc"
    # }
    # doc = es_rdd.first()[1]
    # for field in doc:
    #     value_counts = es_rdd.map(lambda item: item[1][field])
    #     value_counts = value_counts.map(lambda word: (word, 1))
    #     value_counts = value_counts.reduceByKey(lambda a, b: a+b)
    #     value_counts = value_counts.filter(lambda item: item[1] > 1)
    #     value_counts = value_counts.map(lambda item: (field, {
    #         'field': field,
    #         'val': item[0],
    #         'count': item[1]
    #     }))
    #
    #     for line in value_counts.collect():
    #         print(line)
    #     value_counts.saveAsNewAPIHadoopFile(
    #         path='-',
    #         outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    #         keyClass="org.apache.hadoop.io.NullWritable",
    #         valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    #         conf=es_write_conf)