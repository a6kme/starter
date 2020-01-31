"""Contains functionality related to Lines"""
import json
import logging

import topics
from consumers.models import Line

logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        if message.topic() in [topics.STATIONS_TOPIC_NAME, topics.ARRIVALS_TOPIC_NAME]:
            value = message.value()

            # Stations data is coming from faust, which has not been deserialized by Consumer.
            if message.topic() == topics.STATIONS_TOPIC_NAME:
                value = json.loads(value)

            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug(f'Ignoring line {value["line"]}')

        elif message.topic() == topics.TURNSTILE_SUMMARY_TABLE_NAME:
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())
