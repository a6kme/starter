"""Methods pertaining to weather data"""
import datetime
import json
import logging
import random
from pathlib import Path

import requests

from config import REST_PROXY_URL
from enums import weather_status
from topics import WEATHER_TOPIC_NAME
from .avro_producer import AvroProducer

logger = logging.getLogger(__name__)


class Weather(AvroProducer):
    """Defines a simulated weather model"""

    status = weather_status

    rest_proxy_url = REST_PROXY_URL

    key_schema = None
    value_schema = None

    winter_months = {0, 1, 2, 3, 10, 11}  # set
    summer_months = {6, 7, 8}  # set

    def __init__(self, month):
        self.topic_name = WEATHER_TOPIC_NAME
        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

        super().__init__(
            self.topic_name,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema
        )

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(
                {
                    "key_schema": json.dumps(Weather.key_schema),
                    "value_schema": json.dumps(Weather.value_schema),
                    "records": [
                        {
                            "key": {
                                "timestamp": str(int(datetime.datetime.utcnow().timestamp() * 1000))
                            },
                            "value": {
                                "temperature": self.temp,
                                "status": self.status.value
                            }
                        }
                    ]
                }
            ),
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
