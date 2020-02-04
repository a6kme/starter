import abc

from confluent_kafka.admin import AdminClient, NewTopic

from config import BROKER_URL


class BaseProducer(abc.ABC):
    # admin client instance for all Producer instances
    admin_client = None

    def __init__(self, topic_name, num_partitions, num_replicas):
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.producer = None

        # Create the admin client instance
        if BaseProducer.admin_client is None:
            BaseProducer.admin_client = AdminClient({'bootstrap.servers': BROKER_URL})

        # If the topic does not already exist, try to create it
        if not self.topic_exists():
            self.create_topic()

    def topic_exists(self):
        topic_metadata = BaseProducer.admin_client.list_topics(timeout=5)
        return self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        new_topic = NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)
        topic_creation = BaseProducer.admin_client.create_topics([new_topic])
        return topic_creation

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # Flush the producer so that all the messages are written to kafka
        self.producer.flush()
