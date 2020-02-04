"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.avro import AvroProducer as AvroProducerImpl

from config import BROKER_URL, SCHEMA_REGISTRY_URL, NUM_PARTITIONS, NUM_REPLICAS
from producers.models.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class AvroProducer(BaseProducer):
    """Defines and provides common functionality amongst Producers"""

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=NUM_PARTITIONS,
            num_replicas=NUM_REPLICAS,
    ):
        """Initializes a Producer object with basic settings"""
        BaseProducer.__init__(self, topic_name, num_partitions, num_replicas)
        self.key_schema = key_schema
        self.value_schema = value_schema

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        self.producer = AvroProducerImpl(
            self.broker_properties, default_key_schema=key_schema, default_value_schema=value_schema
        )

    @classmethod
    def time_millis(cls):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
