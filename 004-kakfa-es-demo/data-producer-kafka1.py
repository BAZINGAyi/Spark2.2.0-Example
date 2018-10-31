import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


# kafka config
broker_address = ["kafka1:9092", "kafka2:9092"]
topic = 'two-two-para'

# read data from local dist
def read_data():
    file = open('data.json', 'r')
    lines = file.readlines()
    file.close()
    return lines


# send data that type is bytes to kafka
def send_bytes_to_kafka():
    producer = KafkaProducer(bootstrap_servers=broker_address)
    for i in range(1, 100):
        print(i)
        producer.send(topic, b'msg %d' % i)
        time.sleep(1)

# send data the type is json to kafka
def send_json_to_kafka():
    lines = read_data()
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for line in lines:
        json_data = json.loads(line)
        print(line)
        producer.send(topic, json_data)
        time.sleep(1)



if __name__ == '__main__':

    # send_bytes_to_kafka()

    send_json_to_kafka()
