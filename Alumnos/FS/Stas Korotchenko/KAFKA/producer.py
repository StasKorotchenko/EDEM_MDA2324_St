import time
import requests
import json
from json import dumps
from confluent_kafka import Producer
from datetime import datetime, timedelta

class Near_Earth_Asteroids_and_Comets_Producer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'python-producer'
        }
        self.topic = 'Near_Earth_Asteroids_and_Comets'
        self.producer = Producer(self.config)

    def data(self):
        json_file_path = '2vr3-k9wn.json'
        with open(json_file_path) as json_file:
            data = json.load(json_file)
        return data

    def send(self, data):
        self.producer.produce(topic=self.topic, value=dumps(data))
        print(f"Sending data: {data} to topic {self.topic}")
        time.sleep(1)
        

    def flush_producer(self):
        # After your loop where you send messages:
        self.producer.flush()

        # Optionally, you can check if there are any messages that failed to be delivered:
        if self.producer.flush() != 0:
            print("Some messages failed to be delivered")

if __name__ == "__main__":
    near_earth_asteroids_and_comets_producer = Near_Earth_Asteroids_and_Comets_Producer()
    # Пример отправки данных
    data_to_send = near_earth_asteroids_and_comets_producer.data()
    near_earth_asteroids_and_comets_producer.send(data_to_send)

    # Завершаем работу producer после отправки данных
    near_earth_asteroids_and_comets_producer.flush_producer()
