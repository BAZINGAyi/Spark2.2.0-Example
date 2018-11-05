from kafka import KafkaConsumer
from multiprocessing import Process

group_id = "test-consumer-group"
topic_name = "down"
broker_address = ["kafka1:9092", "kafka2:9093"]


def consumer1():
    consumer1 = KafkaConsumer(topic_name,
                              group_id=group_id,
                              bootstrap_servers=broker_address,
                              fetch_max_wait_ms=300,
                              max_partition_fetch_bytes=5242880,
                              )

    for message in consumer1:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        # print("coumser1: " + message.value.decode('utf-8'))
        print("consumer1: %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value.decode('utf-8')))


def consumer2():
    consumer2 = KafkaConsumer(topic_name,
                              group_id=group_id,
                              bootstrap_servers=broker_address,
                              fetch_max_wait_ms=300,
                              max_partition_fetch_bytes=5242880,
                              )

    for message in consumer2:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        # print("coumser2: " + message.value.decode('utf-8'))
        print("consumer2: %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))


if __name__ == '__main__':
    p1 = Process(target=consumer1)
    p1.start()

    p2 = Process(target=consumer2)
    p2.start()


    while 1:
        continue

