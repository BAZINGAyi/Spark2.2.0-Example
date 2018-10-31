# random range
import json
import random

# 随机数据数据变化
tag_range = 1000
packets_range = 100000
bytes_range = 100000000

# 原始数据类型
origin_data = {"event_type": "purge", "tag": 10001, "as_path": "64910", "iface_in": 22, "flow_direction": "0", "stamp_inserted": "2018-09-26 14:30:00", "stamp_updated": "2018-09-26 14:31:01", "packets": 166000, "bytes": 166000000, "writer_id": "default_kafka/826009"}
path = 'data.json'

# 生成假数据
def change_data(origin_data):
    new_origin_data = origin_data
    new_origin_data['tag_range'] = str(random.randint(1, tag_range))
    new_origin_data['packets_range'] = str(random.randint(1, packets_range))
    new_origin_data['bytes_range'] = str(random.randint(1, bytes_range))
    return new_origin_data

# 显示生成的数据
def read_data():
    file = open(path, 'r')
    for line in file:
        json_data = json.loads(line)
        print("json contents:" + json_data['event_type'])
    file.close()

# 写入文件
def write_data():
    file = open(path, 'w')
    for i in range(1, 100):
        json.dump(change_data(origin_data), file)
        file.write('\n')
    file.close()

if __name__ == '__main__':

    write_data()

    read_data()