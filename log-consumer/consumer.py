import json
from kafka import KafkaConsumer
from pymongo import MongoClient

class LogConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'logs',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='log-consumers',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['logdb']
        self.collection = self.db['logs']

    def consume_logs(self):
        for message in self.consumer:
            log = message.value
            self.collection.insert_one(log)
            print(f"Inserted log: {log}")

# Usage
if __name__ == "__main__":
    log_consumer = LogConsumer()
    log_consumer.consume_logs()
