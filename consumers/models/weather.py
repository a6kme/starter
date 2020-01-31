"""Contains functionality related to Weather"""
import json
import logging

from enums import weather_status

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        weather_value = message.value()
        self.temperature = round(weather_value['temperature'], 2)
        self.status = weather_status(weather_value['status']).name
