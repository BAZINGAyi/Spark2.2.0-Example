import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


# kafka config
broker_address = ["kafka1:9092", "kafka2:9092"]
topic = 'kafka'

# read data from local dist
def read_data():
    file = open('data.json', 'r')
    lines = file.readlines()
    file.close()
    return lines


# send data that type is bytes to kafka
def send_bytes_to_kafka():
    producer = KafkaProducer(bootstrap_servers=broker_address)
    print(producer.partitions_for(topic))

    for i in range(1, 100):
        print(i)
        producer.send(topic, value=b'msg %d' % i).get(30)
        time.sleep(1)

# send data the type is json to kafka
def send_json_to_kafka():
    lines = read_data()
    producer = KafkaProducer(bootstrap_servers=broker_address, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for line in lines:
        json_data = json.loads(line)
        print(line)
        try:
            producer.send(topic, partition=(0, ""), value=json_data).get(30)
        except Exception :
            print("123")

        time.sleep(1)



if __name__ == '__main__':

    send_bytes_to_kafka()

    #send_json_to_kafka()
