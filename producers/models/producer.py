from confluent_kafka import Producer as ProducerImpl

from config import NUM_PARTITIONS, NUM_REPLICAS, BROKER_URL
from producers.models.base_producer import BaseProducer


class Producer(BaseProducer):
    def __init__(self, topic_name, num_partitions=NUM_PARTITIONS, num_replicas=NUM_REPLICAS):
        BaseProducer.__init__(self, topic_name, num_partitions, num_replicas)

        self.producer = ProducerImpl({'bootstrap.servers': BROKER_URL})

    def produce(self, encoded_data):
        self.producer.produce(self.topic_name, encoded_data)
