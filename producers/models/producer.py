"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from config import BROKER_URL, SCHEMA_REGISTRY_URL

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    # admin client instance for all Producer instances
    admin_client = None

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # Create the admin client instance
        if Producer.admin_client is None:
            Producer.admin_client = AdminClient({'bootstrap.servers': BROKER_URL})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties, default_key_schema=key_schema, default_value_schema=value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        topic_creation = Producer.admin_client.create_topics(
            [NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)])
        return topic_creation

    @classmethod
    def time_millis(cls):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # Flush the producer so that all the messages are written to kafka
        self.producer.flush()
