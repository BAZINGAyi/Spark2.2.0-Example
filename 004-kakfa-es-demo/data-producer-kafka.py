import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


# kafka config
brokers = 'kafka1:9092'
topic = 'test'

# read data from local dist
def read_data():
    file = open('data.json', 'r')
    lines = file.readlines()
    file.close()
    return lines

# send data to kafka
def send_kafka():
    lines = read_data()
    for line in lines:
        json_data = json.loads(line)
        print(json_data['as_path'])
        producer.send(topic, json_data)
        time.sleep(5)

def feedfack():
    producer = KafkaProducer(bootstrap_servers=[brokers], value_serializer=lambda m: json.dumps(m).encode('ascii'))
    future = producer.send(topic, "signgle")

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        # log.exception()
        pass

    # Successful result returns assigned partition and offset
    print(record_metadata.topic)
    print(record_metadata.partition)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=[brokers], value_serializer=lambda m: json.dumps(m).encode('ascii'))
    send_kafka()

