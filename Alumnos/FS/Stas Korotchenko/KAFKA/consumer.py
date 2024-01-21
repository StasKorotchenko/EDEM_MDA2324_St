import time
from confluent_kafka import Consumer, Producer, KafkaError
from json import dumps, loads

class Near_Earth_Asteroids_and_Comets_Consumer:
    def __init__(self,consumer_config, producer_config, topic_consumer, topic_producer, orbit_file_path):
        self.consumer = Consumer(consumer_config)
        self.producer = Producer(producer_config)
        self.topic_consumer = topic_consumer
        self.topic_producer = topic_producer
        self.near_earth_object = self.load_orbits(orbit_file_path)

    def load_orbits(self, file_path):
        with open(file_path, 'r') as file:
            return [line.strip().lower() for line in file]

    def is_near_earth_object(self, orbit):
        return orbit.lower() in self.near_earth_object

    def consume_messages(self):
        self.consumer.subscribe([self.topic_consumer])
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaError(msg.error())
                else:
                    orbit = msg.value().decode('utf-8')
                    if self.is_near_earth_object(orbit):
                        self.producer.produce(self.topic_producer, orbit)
                        self.producer.flush()
                        print(f"Orbit: {orbit}")
                        time.sleep(1)
        except KafkaError as e:
            print(f"Error: {e}")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    # Configuración del consumidor
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    # Configuración del productor
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer'
    }

    topic_consumer = 'Near_Earth_Asteroids_and_Comets'
    topic_producer = 'Near_Earth_Asteroids_and_Comets_Orbits_Apollo_Amor'
    orbit_file_path = 'orbit_class.txt'

    data_processor = Near_Earth_Asteroids_and_Comets_Consumer(consumer_config, producer_config, topic_consumer, topic_producer, orbit_file_path)

    try:
        data_processor.consume_messages()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        data_processor.consumer.close()