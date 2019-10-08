# -*- coding: utf-8 -*-
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers='49.4.90.247:6667')

msg_dict = {
    "sleep_time": 10,
    "db_config": {
        "database": "test_1",
        "host": "xxxx",
        "user": "root",
        "password": "root"
    },
    "table": "msg",
    "msg": "Hello World"
}
msg = json.dumps(msg_dict)
future = producer.send('test_rhj', msg)
record_metadata = future.get(timeout=10)
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)
producer.close()

