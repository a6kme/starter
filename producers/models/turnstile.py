"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from topics import TURNSTILE_TOPIC_NAME
from .avro_producer import AvroProducer
from .turnstile_hardware import TurnstileHardware
from config import NUM_PARTITIONS, NUM_REPLICAS

logger = logging.getLogger(__name__)


class Turnstile(AvroProducer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            topic_name=TURNSTILE_TOPIC_NAME,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        # logger.info(f'Advanced turnstiles for station_id: {self.station.station_id} num_entries: {num_entries}')
        for i in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={
                    "timestamp": str(int(timestamp.timestamp() * 1000))
                },
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name
                }
            )
