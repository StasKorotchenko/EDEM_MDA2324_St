import time
from confluent_kafka import Consumer, KafkaError
from json import loads

class Objetos_Consumer2:
    def __init__(self, bootstrap_servers='localhost:9092', group_id='python-consumer-group', auto_offset_reset='earliest'):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset
        }
        self.consumer = Consumer(self.config)
        self.topic_consumer = 'Asteroids_and_Comets_stream_2011'
        self.objetos_data = {}

    def subscribe_to_topic(self):
        self.consumer.subscribe([self.topic_consumer])

   