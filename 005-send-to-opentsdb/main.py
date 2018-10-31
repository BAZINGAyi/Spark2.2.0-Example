from pyspark import SparkContext
import requests

payload = {
    "metric": "sys.cpu.nice",
    "timestamp": '1489544891',
    "value": '29',
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}

payload1 = {
    "metric": "sys.cpu.nice",
    "timestamp": '1489544892',
    "value": '30',
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}

payload2 = {
    "metric": "sys.cpu.nice",
    "timestamp": '1489544893',
    "value": '29',
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}

payload3 = {
    "metric": "sys.cpu.nice",
    "timestamp": '1489544894',
    "value": '30',
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}

ls = [payload, payload1, payload2, payload3]

def increment_counter(record):
    send_json(ls)

def send_json(json):
    r = requests.post("http://192.168.80.2:4242/api/put?details", json=json)
    return r.text

if __name__ == "__main__":
    # sc = SparkContext(appName="OutputToOpenstdb")
    # rdd = sc.parallelize([1, 2, 3, 4, 5])
    # rdd.foreach(lambda record: increment_counter(record))
    send_json(ls)
