"""Defines trends calculations for stations"""
import logging

import faust

from config import KAFKA_URL
from topics import TOPIC_PREFIX_FOR_STATIONS_CONNECTOR, STATIONS_TOPIC_NAME

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=KAFKA_URL, store="memory://")
topic = app.topic(f'{TOPIC_PREFIX_FOR_STATIONS_CONNECTOR}stations', value_type=Station)
out_topic = app.topic(STATIONS_TOPIC_NAME, partitions=1, value_type=TransformedStation)
table = app.Table(
    "stations-table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def process_station(stations):
    async for station in stations:
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"
        else:
            line = "NA"
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )


if __name__ == "__main__":
    app.main()
