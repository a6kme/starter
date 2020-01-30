"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from topics import TURNSTILE_TOPIC_NAME
from .producer import Producer
from .turnstile_hardware import TurnstileHardware
from config import NUM_PARTITIONS, NUM_REPLICAS

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            topic_name=TURNSTILE_TOPIC_NAME,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=NUM_PARTITIONS,
            num_replicas=NUM_REPLICAS,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        self.producer.produce(
            topic=self.topic_name,
            key={
                "timestamp": int(timestamp.timestamp())
            },
            value={
                "station_id": self.station.station_id,
                "station_name": self.station.name,
                "line": num_entries
            }
        )
