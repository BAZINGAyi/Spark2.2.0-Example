from pyspark import SparkContext

host = 'zookeeper'
table = 'stu'

def read_from_hbase():
    conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    sc = SparkContext(appName="PythonReadFromHbase")

    hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat"
                                   , "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
                                   , "org.apache.hadoop.hbase.client.Result"
                                   , keyConverter=keyConv
                                   , valueConverter=valueConv
                                   , conf=conf)
    count = hbase_rdd.count()
    hbase_rdd.cache()
    output = hbase_rdd.collect()
    for (k, v) in output:
        print(k, v)


def write_to_hbase():
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    # if the "mapreduce.output.fileoutputformat.outputdir" is not exist, it will happen a error, this is spark 2.2.0 bugs.

    conf = {"hbase.zookeeper.quorum": host, "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable",
            "mapreduce.output.fileoutputformat.outputdir":"/tmp"}


    rawData = ['9,info,name,Rongcheng', '10,info,name,Guanhua']

    sc = SparkContext(appName="PythonWriteFromHbase")
    sc.parallelize(rawData).map(lambda x: (x[0], x.split(','))).saveAsNewAPIHadoopDataset(conf=conf,
                                                                                          keyConverter=keyConv,
                                                                                          valueConverter=valueConv)


if __name__ == '__main__':

   read_from_hbase()

   #write_to_hbase()